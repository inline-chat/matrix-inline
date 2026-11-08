package connector

import (
	"context"
	"github.com/rs/zerolog"
	"reflect"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

type roomKind string

const (
	roomUnknown    roomKind = "unknown"
	roomDirect     roomKind = "dm"
	roomChannel    roomKind = "channel"
	roomReplyChild roomKind = "reply_thread"
)

// classifyDialog consumes the complete descriptor, never absence in an old row.
// Hidden/archive/notification state is independent of structural identity.
func classifyDialog(d sidecar.DialogRecord) roomKind {
	s := d.Structure
	if s == nil || d.ChatID <= 0 {
		return roomUnknown
	}
	positive := func(v *int64) bool { return v != nil && *v > 0 }
	invalid := func(v *int64) bool { return v != nil && *v <= 0 }
	if invalid(s.PeerUserID) || invalid(s.SpaceID) || invalid(s.ParentChatID) || invalid(s.ParentMessageID) {
		return roomUnknown
	}
	// Flat compatibility fields cannot contradict the authoritative observation.
	for _, pair := range [][2]*int64{{d.PeerUserID, s.PeerUserID}, {d.SpaceID, s.SpaceID}, {d.ParentChatID, s.ParentChatID}, {d.ParentMessageID, s.ParentMessageID}} {
		if pair[0] != nil && !int64PtrEqual(pair[0], pair[1]) {
			return roomUnknown
		}
	}
	switch s.Kind {
	case "direct":
		if positive(s.PeerUserID) && s.SpaceID == nil && s.ParentChatID == nil && s.ParentMessageID == nil {
			return roomDirect
		}
	case "channel":
		if s.PeerUserID == nil && s.ParentChatID == nil && s.ParentMessageID == nil {
			return roomChannel
		}
	case "reply_child":
		if s.PeerUserID == nil && positive(s.ParentChatID) && *s.ParentChatID != d.ChatID {
			return roomReplyChild
		}
	}
	return roomUnknown
}

func isDMDialog(d sidecar.DialogRecord) bool          { return classifyDialog(d) == roomDirect }
func sameDialogRecord(a, b sidecar.DialogRecord) bool { return reflect.DeepEqual(a, b) }

func (ic *InlineClient) chatInfoForDialog(ctx context.Context, d sidecar.DialogRecord, name *string) *bridgev2.ChatInfo {
	info := &bridgev2.ChatInfo{Name: name, CanBackfill: true, Members: ic.chatMemberListForKnownSelf(ctx)}
	switch classifyDialog(d) {
	case roomDirect:
		rt := database.RoomTypeDM
		info.Type = &rt
		info.Name = nil
		info.Members = ic.chatMemberListForDM(ctx, *d.Structure.PeerUserID)
	case roomChannel, roomReplyChild:
		rt := database.RoomTypeDefault
		info.Type = &rt
	}
	// Parent correction uses the connector journal in the serialized portal lane.
	// The framework's ParentID path retains cached parents on detach in v0.28.
	info.ExtraUpdates = func(ctx context.Context, p *bridgev2.Portal) bool {
		if err := ic.reconcileSourceTopic(ctx, p, d); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Msg("Source topic projection remains pending")
		}
		ic.projectDialogInLane(ctx, p, d)
		return false
	}
	return info
}
