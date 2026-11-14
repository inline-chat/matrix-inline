package connector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
)

// A partial cache record cannot erase source structure. A complete newer record
// can explicitly detach a chat from its workspace or parent.
func mergeDialogStructure(previous, next sidecar.DialogRecord) sidecar.DialogRecord {
	stale := previous.StructureSeq != nil && (next.StructureSeq == nil || *next.StructureSeq < *previous.StructureSeq)
	if !next.StructureKnown || stale {
		if previous.Kind != "" && previous.Kind != "unknown" {
			next.Kind = previous.Kind
		}
		if next.PeerUserID == nil || stale {
			next.PeerUserID = previous.PeerUserID
		}
		next.SpaceID, next.ParentChatID, next.ParentMessageID = previous.SpaceID, previous.ParentChatID, previous.ParentMessageID
		next.Description, next.StructureKnown, next.StructureSeq = previous.Description, previous.StructureKnown, previous.StructureSeq
		next.IsPublic = previous.IsPublic
	}
	return next
}

func sameDialogStructure(left, right sidecar.DialogRecord) bool {
	return left.Kind == right.Kind && left.StructureKnown == right.StructureKnown &&
		int64PtrEqual(left.SpaceID, right.SpaceID) && int64PtrEqual(left.ParentChatID, right.ParentChatID) &&
		int64PtrEqual(left.ParentMessageID, right.ParentMessageID) && stringPtrEqual(left.Description, right.Description) &&
		boolPtrEqual(left.IsPublic, right.IsPublic)
}

// Existing incorrect group-DM classifications are repaired explicitly by the
// account command. New ordinary rooms use the correct classification at creation.
func preserveExistingRoomClassification(portal *bridgev2.Portal, info *bridgev2.ChatInfo) *bridgev2.ChatInfo {
	if portal != nil && portal.MXID != "" && portal.RoomType == database.RoomTypeGroupDM && info != nil && info.Type != nil && *info.Type == database.RoomTypeDefault {
		copy := *info
		copy.Type = nil
		return &copy
	}
	return info
}

func guardedChatInfo(info *bridgev2.ChatInfo) func(context.Context, *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	return func(_ context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
		return preserveExistingRoomClassification(portal, info), nil
	}
}

var _ bridgev2.RoomTopicHandlingNetworkAPI = (*InlineClient)(nil)

// Topics are Matrix-local. Recording the user's choice also preserves a cleared
// topic when child context or source descriptions change later.
func (ic *InlineClient) HandleMatrixRoomTopic(_ context.Context, msg *bridgev2.MatrixRoomTopic) (bool, error) {
	if msg == nil || msg.Portal == nil || msg.Content == nil {
		return false, fmt.Errorf("missing Matrix room topic")
	}
	meta := portalIdentityMetadata(msg.Portal)
	msg.Portal.Topic, msg.Portal.TopicSet = msg.Content.Topic, true
	meta.UserTopic, meta.ChildTopicOwned = true, false
	meta.OwnedChildTopic = ""
	return true, nil
}

func portalIdentityMetadata(portal *bridgev2.Portal) *PortalMetadata {
	if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil {
		return meta
	}
	meta := &PortalMetadata{}
	portal.Metadata = meta
	return meta
}

func (ic *InlineClient) updateChildContext(ctx context.Context, portal *bridgev2.Portal, dialog sidecar.DialogRecord) bool {
	if portal == nil || !dialog.StructureKnown {
		return false
	}
	meta := portalIdentityMetadata(portal)
	changed := !int64PtrEqual(meta.InlineParentChatID, dialog.ParentChatID) || !int64PtrEqual(meta.InlineParentMessageID, dialog.ParentMessageID)
	meta.InlineParentChatID, meta.InlineParentMessageID = dialog.ParentChatID, dialog.ParentMessageID
	if meta.UserTopic || (portal.Topic != "" && (!meta.ChildTopicOwned || portal.Topic != meta.OwnedChildTopic)) {
		return changed
	}
	if portal.MXID != "" {
		// Offline edits (including an intentionally empty topic) may precede
		// this bridge version or its local state. Inspect Matrix ownership before
		// replacing a topic; a failed read leaves the pending context intact.
		if portal.Bridge == nil || portal.Bridge.Bot == nil {
			return changed
		}
		reader, ok := portal.Bridge.Matrix.(bridgev2.MatrixConnectorWithArbitraryRoomState)
		if !ok {
			return changed
		}
		current, err := reader.GetStateEvent(ctx, portal.MXID, event.StateTopic, "")
		if err != nil {
			return changed
		}
		if current != nil {
			_ = current.Content.ParseRaw(event.StateTopic)
			topic := current.Content.AsTopic().Topic
			if current.Sender != "" && current.Sender != portal.Bridge.Bot.GetMXID() {
				meta.UserTopic, meta.ChildTopicOwned, meta.OwnedChildTopic = true, false, ""
				portal.Topic, portal.TopicSet = topic, true
				return true
			}
			if topic != portal.Topic {
				portal.Topic, portal.TopicSet = topic, true
				changed = true
				if topic != "" && (!meta.ChildTopicOwned || topic != meta.OwnedChildTopic) {
					return changed
				}
			}
		}
	}
	topic := ""
	if dialog.Description != nil {
		topic = strings.TrimSpace(*dialog.Description)
	}
	if dialog.ParentChatID != nil && *dialog.ParentChatID > 0 {
		parent, err := ic.accessibleMappedParent(ctx, *dialog.ParentChatID)
		// A temporary source lookup failure must not delete known context. Missing
		// mappings also never cause the bridge to create or advertise a parent room.
		if err != nil || parent == nil {
			return changed
		}
		link := "https://matrix.to/#/" + string(parent.MXID)
		if dialog.ParentMessageID != nil && *dialog.ParentMessageID > 0 {
			message, err := ic.UserLogin.Bridge.DB.Message.GetFirstPartByID(ctx, parent.Receiver, makeMessageID(*dialog.ParentChatID, *dialog.ParentMessageID))
			if err == nil && message != nil && message.Room == parent.PortalKey && message.MXID != "" {
				link += "/" + string(message.MXID)
			}
		}
		name := strings.TrimSpace(parent.Name)
		if name == "" {
			name = fmt.Sprintf("Inline chat %d", *dialog.ParentChatID)
		}
		if topic != "" {
			topic += "\n\n"
		}
		topic += "Parent conversation: " + name + " — " + link
	}
	if topic == portal.Topic && meta.ChildTopicOwned && meta.OwnedChildTopic == topic {
		return changed
	}
	if portal.MXID != "" {
		if portal.Bridge == nil || portal.Bridge.Bot == nil {
			return changed
		}
		_, err := portal.Bridge.Bot.SendState(ctx, portal.MXID, event.StateTopic, "", &event.Content{Parsed: &event.TopicEventContent{Topic: topic}, Raw: map[string]any{"com.beeper.exclude_from_timeline": true}}, time.Now())
		if err != nil {
			return changed
		}
	}
	portal.Topic, portal.TopicSet = topic, portal.MXID != ""
	meta.ChildTopicOwned, meta.OwnedChildTopic = true, topic
	return true
}

func (ic *InlineClient) accessibleMappedParent(ctx context.Context, chatID int64) (*bridgev2.Portal, error) {
	if ic.UserLogin == nil || ic.UserLogin.Bridge == nil || ic.Sidecar == nil {
		return nil, nil
	}
	dialog, known := ic.cachedDialog(chatID)
	if !known || !ic.shouldProjectDialog(dialog) {
		return nil, nil
	}
	parent, err := ic.UserLogin.Bridge.GetExistingPortalByKey(ctx, ic.portalKeyForChat(chatID))
	if err != nil || parent == nil || parent.MXID == "" {
		return nil, err
	}
	membership, err := ic.UserLogin.Bridge.DB.UserPortal.Get(ctx, ic.UserLogin.UserLogin, parent.PortalKey)
	if err != nil || membership == nil {
		return nil, err
	}
	// The source endpoint, not shared ghost/cache presence, proves this login can
	// still inspect the parent. No parent or child data is written to the parent.
	if _, err := ic.Sidecar.ChatParticipants(ctx, sidecar.ChatParticipantsRequest{ChatID: chatID}); err != nil {
		return nil, err
	}
	return parent, nil
}

// Saved once before applying the narrow in-place classification repair.
// Notification preferences, topic, room identity and all message rows are untouched.
type RoomClassificationRepair struct {
	PreviousType        database.RoomType `json:"previous_type"`
	PreviousOtherUserID string            `json:"previous_other_user_id,omitempty"`
	AppliedAt           time.Time         `json:"applied_at"`
}
