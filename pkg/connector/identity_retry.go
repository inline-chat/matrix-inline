package connector

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
)

const identityProjectionRetryLimit = 8

// Metadata work is independent of chat traffic: a quiet child's topic and a
// pending workspace link both recover after transient Matrix/source failures.
func (ic *InlineClient) retryIdentityProjection(ctx context.Context) {
	if ic.UserLogin == nil || ic.UserLogin.Bridge == nil {
		return
	}
	ic.retryWorkspaceLinks(ctx)
	policy := ic.effectiveHiddenDialogsPolicy()
	ic.mu.Lock()
	dialogs := make([]sidecar.DialogRecord, 0, len(ic.dialogs))
	for _, dialog := range ic.dialogs {
		if dialog.StructureKnown && (policy == hiddenDialogsInclude || dialog.ChatListHidden == nil || !*dialog.ChatListHidden) {
			dialogs = append(dialogs, dialog)
		}
	}
	cursor := ic.identityRetryCursor
	ic.mu.Unlock()
	sort.Slice(dialogs, func(i, j int) bool { return dialogs[i].ChatID < dialogs[j].ChatID })
	if len(dialogs) == 0 {
		return
	}
	start := sort.Search(len(dialogs), func(i int) bool { return dialogs[i].ChatID > cursor })
	if start == len(dialogs) {
		start = 0
	}
	count := min(len(dialogs), identityProjectionRetryLimit)
	for offset := 0; offset < count; offset++ {
		if ctx.Err() != nil {
			return
		}
		dialog := dialogs[(start+offset)%len(dialogs)]
		ic.mu.Lock()
		ic.identityRetryCursor = dialog.ChatID
		ic.mu.Unlock()
		// This event cannot create a room or change its audience/classification.
		info := ic.chatInfoForDialog(ctx, dialog, nil)
		ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: ic.portalKeyForChat(dialog.ChatID), Timestamp: time.Now()},
			ChatInfo:  &bridgev2.ChatInfo{ExtraUpdates: info.ExtraUpdates, ExcludeChangesFromTimeline: true},
		})
	}
}

func (ic *InlineClient) retryWorkspaceLinks(ctx context.Context) {
	ic.navigationMu.Lock()
	defer ic.navigationMu.Unlock()
	spaces := ic.workspaceNavigationSnapshot()
	remaining := identityProjectionRetryLimit
	for _, space := range spaces {
		if remaining <= 0 {
			break
		}
		if space.Active && space.RoomID == "" {
			remaining--
			if err := ic.ensureWorkspaceRoom(ctx, space); err != nil {
				continue
			}
		}
		for chatID, link := range space.Links {
			if remaining <= 0 {
				break
			}
			remove := !space.Active
			if parsed, err := strconv.ParseInt(chatID, 10, 64); err == nil {
				if dialog, known := ic.cachedDialog(parsed); known {
					remove = remove || !ic.shouldProjectDialog(dialog) || (dialog.StructureKnown && (dialog.SpaceID == nil || *dialog.SpaceID != space.SpaceID))
				}
			}
			if !remove && link.Attached {
				continue
			}
			remaining--
			if !remove {
				parsed, err := strconv.ParseInt(chatID, 10, 64)
				if err != nil || parsed <= 0 || ic.Sidecar == nil {
					continue
				}
				if _, err := ic.Sidecar.ChatParticipants(ctx, sidecar.ChatParticipantsRequest{ChatID: parsed}); err != nil {
					continue
				}
			}
			if err := ic.sendWorkspaceLink(ctx, space.RoomID, link.RoomID, remove); err != nil {
				continue
			}
			if remove {
				delete(space.Links, chatID)
			} else {
				link.Attached = true
				space.Links[chatID] = link
			}
		}
	}
	if err := ic.saveWorkspaceNavigation(ctx, spaces); err != nil {
		ic.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to save pending Inline navigation")
	}
}
