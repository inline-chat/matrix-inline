package connector

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/simplevent"
)

var commandInlineRepairRoom = &commands.FullHandler{
	Func: fnInlineRepairRoom, Name: "inline-repair-room", RequiresLogin: true,
	Help: commands.HelpMeta{Section: commands.HelpSectionMisc, Description: "Preview or apply an in-place correction for a channel previously classified as a group DM", Args: "<chat-id> [apply]"},
}

func fnInlineRepairRoom(ce *commands.Event) {
	_, client := inlineCommandLogin(ce)
	if client == nil {
		ce.Reply("Your default login is not an Inline login.")
		return
	}
	if len(ce.Args) < 1 || len(ce.Args) > 2 || (len(ce.Args) == 2 && ce.Args[1] != "apply") {
		ce.Reply("Usage: `$cmdprefix inline-repair-room <chat-id> [apply]`")
		return
	}
	chatID, err := parseInlineUserID(ce.Args[0])
	if err != nil {
		ce.Reply("Invalid Inline chat ID: %v", err)
		return
	}
	summary, err := client.repairRoomClassification(ce.Ctx, chatID, len(ce.Args) == 2)
	if err != nil {
		ce.Reply("Inline room repair failed: %v", err)
		return
	}
	ce.Reply("%s", summary)
}

func (ic *InlineClient) repairRoomClassification(ctx context.Context, chatID int64, apply bool) (string, error) {
	if ic.UserLogin == nil || ic.UserLogin.Bridge == nil {
		return "", fmt.Errorf("Inline bridge is unavailable")
	}
	state, err := ic.Sidecar.ChatState(ctx, sidecar.ChatStateRequest{ChatID: chatID})
	if err != nil {
		return "", err
	}
	if state.Deleted || state.Dialog == nil || state.Dialog.ChatID != chatID || !state.Dialog.StructureKnown || state.Dialog.Kind != sidecar.ConversationChat {
		return "", fmt.Errorf("source has not confirmed an ordinary Inline conversation for this chat")
	}
	if !ic.shouldProjectDialog(*state.Dialog) {
		return "", fmt.Errorf("this Inline chat is excluded by the hidden chat setting")
	}
	if _, err := ic.Sidecar.ChatParticipants(ctx, sidecar.ChatParticipantsRequest{ChatID: chatID}); err != nil {
		return "", fmt.Errorf("source access check failed: %w", err)
	}
	portal, err := ic.UserLogin.Bridge.GetExistingPortalByKey(ctx, ic.portalKeyForChat(chatID))
	if err != nil {
		return "", err
	}
	if portal == nil || portal.MXID == "" {
		return "", fmt.Errorf("there is no existing Matrix room for Inline chat %d", chatID)
	}
	membership, err := ic.UserLogin.Bridge.DB.UserPortal.Get(ctx, ic.UserLogin.UserLogin, portal.PortalKey)
	if err != nil {
		return "", err
	}
	if membership == nil {
		return "", fmt.Errorf("the Matrix room is not mapped for this Inline login")
	}
	if portal.RoomType != database.RoomTypeGroupDM {
		return fmt.Sprintf("Inline chat %d already uses room classification %q; no repair needed.", chatID, portal.RoomType), nil
	}
	if !apply {
		return fmt.Sprintf("Preview: change Inline chat %d (%s) from group DM to ordinary room in place. Room and message IDs, topics and notification preferences stay the same. Apply with `$cmdprefix inline-repair-room %d apply`.", chatID, portal.MXID, chatID), nil
	}
	result := ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatResync{
		EventMeta:       simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: portal.PortalKey, Timestamp: time.Now()},
		GetChatInfoFunc: classificationRepairInfo,
	})
	if err := eventHandlingResultError("room classification repair", result); err != nil {
		return "", err
	}
	if portal.RoomType != database.RoomTypeDefault {
		return "", fmt.Errorf("room classification repair did not complete")
	}
	return "Repaired Inline chat " + strconv.FormatInt(chatID, 10) + " in place (" + string(portal.MXID) + "). The previous classification is saved in bridge metadata.", nil
}

func classificationRepairInfo(ctx context.Context, actual *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	if actual.RoomType != database.RoomTypeGroupDM {
		return nil, nil
	}
	meta := portalIdentityMetadata(actual)
	if meta.ClassificationRepair == nil {
		meta.ClassificationRepair = &RoomClassificationRepair{PreviousType: actual.RoomType, PreviousOtherUserID: string(actual.OtherUserID), AppliedAt: time.Now().UTC()}
		if err := actual.Save(ctx); err != nil {
			return nil, err
		}
	}
	roomType := database.RoomTypeDefault
	return &bridgev2.ChatInfo{Type: &roomType, ExcludeChangesFromTimeline: true}, nil
}
