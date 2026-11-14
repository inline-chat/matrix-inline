package connector

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Navigation belongs to a source login, including on bridges that share chat
// portals across logins. Every generated workspace Space has an account-qualified
// ID, and only this login's current source membership can activate it.
type WorkspaceNavigation struct {
	SpaceID int64                        `json:"space_id"`
	Name    string                       `json:"name"`
	Active  bool                         `json:"active"`
	RoomID  id.RoomID                    `json:"room_id,omitempty"`
	Links   map[string]WorkspaceRoomLink `json:"links,omitempty"`
}

type WorkspaceRoomLink struct {
	RoomID   id.RoomID `json:"room_id"`
	Attached bool      `json:"attached"`
}

func (ic *InlineClient) workspacePortalID(spaceID int64) networkid.PortalID {
	account := ic.AccountID
	if account == "" && ic.UserLogin != nil {
		account = string(ic.UserLogin.ID)
	}
	return networkid.PortalID("workspace:" + account + ":" + strconv.FormatInt(spaceID, 10))
}

func (ic *InlineClient) workspaceIDFromKey(key networkid.PortalKey) (int64, error) {
	if ic.UserLogin == nil {
		return 0, fmt.Errorf("Inline login is unavailable")
	}
	if key.Receiver != "" && key.Receiver != ic.UserLogin.ID {
		return 0, fmt.Errorf("Inline workspace belongs to a different login")
	}
	prefix := string(ic.workspacePortalID(0))
	prefix = strings.TrimSuffix(prefix, "0")
	raw, ok := strings.CutPrefix(string(key.ID), prefix)
	if !ok {
		return 0, fmt.Errorf("parent must be an Inline workspace for this login; creating native child conversations from Matrix is not supported")
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid Inline workspace ID")
	}
	return value, nil
}

func cloneWorkspaceNavigation(source map[string]*WorkspaceNavigation) map[string]*WorkspaceNavigation {
	copy := make(map[string]*WorkspaceNavigation, len(source))
	for key, value := range source {
		if value == nil {
			continue
		}
		entry := *value
		entry.Links = make(map[string]WorkspaceRoomLink, len(value.Links))
		for chatID, link := range value.Links {
			entry.Links[chatID] = link
		}
		copy[key] = &entry
	}
	return copy
}

func (ic *InlineClient) workspaceNavigationSnapshot() map[string]*WorkspaceNavigation {
	ic.metadataSaveMu.Lock()
	defer ic.metadataSaveMu.Unlock()
	if ic.UserLogin == nil {
		return make(map[string]*WorkspaceNavigation)
	}
	meta, ok := ic.UserLogin.Metadata.(*UserLoginMetadata)
	if !ok || meta == nil {
		return make(map[string]*WorkspaceNavigation)
	}
	return cloneWorkspaceNavigation(meta.WorkspaceNavigation)
}

func (ic *InlineClient) saveWorkspaceNavigation(ctx context.Context, spaces map[string]*WorkspaceNavigation) error {
	ic.metadataSaveMu.Lock()
	defer ic.metadataSaveMu.Unlock()
	if ic.UserLogin == nil {
		return fmt.Errorf("Inline login unavailable")
	}
	meta, ok := ic.UserLogin.Metadata.(*UserLoginMetadata)
	if !ok || meta == nil {
		return fmt.Errorf("Inline login metadata unavailable")
	}
	meta.WorkspaceNavigation = cloneWorkspaceNavigation(spaces)
	if ic.UserLogin.Bridge == nil {
		return nil
	}
	return ic.UserLogin.Save(ctx)
}

func (ic *InlineClient) workspacePortalInfo(portal *bridgev2.Portal) (*bridgev2.ChatInfo, bool, error) {
	if portal == nil || !strings.HasPrefix(string(portal.ID), "workspace:") {
		return nil, false, nil
	}
	spaceID, err := ic.workspaceIDFromKey(portal.PortalKey)
	if err != nil {
		return nil, true, err
	}
	space := ic.workspaceNavigationSnapshot()[strconv.FormatInt(spaceID, 10)]
	if space == nil || !space.Active {
		return nil, true, fmt.Errorf("Inline workspace membership is not confirmed for this login")
	}
	return ic.chatInfoForWorkspace(space), true, nil
}

func (ic *InlineClient) chatInfoForWorkspace(space *WorkspaceNavigation) *bridgev2.ChatInfo {
	roomType := database.RoomTypeSpace
	members := ic.chatMemberListForKnownSelf(context.Background())
	if members != nil {
		members.CheckAllLogins = false
	}
	return &bridgev2.ChatInfo{Name: &space.Name, Type: &roomType, Members: members, ExcludeChangesFromTimeline: true}
}

func applyWorkspaceAccess(spaces map[string]*WorkspaceNavigation, snapshot *sidecar.AccountStateSnapshot, selfID int64) {
	if snapshot == nil {
		return
	}
	removed := make(map[int64]bool, len(snapshot.RemovedSpaceIDs))
	for _, spaceID := range snapshot.RemovedSpaceIDs {
		removed[spaceID] = true
	}
	current := make(map[int64]bool)
	for _, member := range snapshot.SpaceMemberships {
		if member.UserID == selfID {
			current[member.SpaceID] = true
		}
	}
	present := make(map[int64]bool)
	for _, record := range snapshot.Spaces {
		if record.SpaceID <= 0 {
			continue
		}
		present[record.SpaceID] = true
		key := strconv.FormatInt(record.SpaceID, 10)
		entry := spaces[key]
		// Incomplete refreshes retain structure but never invent new memberships.
		if entry == nil && (!current[record.SpaceID] || removed[record.SpaceID]) {
			continue
		}
		if entry == nil {
			entry = &WorkspaceNavigation{SpaceID: record.SpaceID, Links: make(map[string]WorkspaceRoomLink)}
			spaces[key] = entry
		}
		if record.Name != "" {
			entry.Name = record.Name
		}
		if current[record.SpaceID] && !removed[record.SpaceID] {
			entry.Active = true
		} else if snapshot.SpacesComplete {
			entry.Active = false
		}
	}
	for _, entry := range spaces {
		if removed[entry.SpaceID] || (snapshot.SpacesComplete && (!present[entry.SpaceID] || !current[entry.SpaceID])) {
			entry.Active = false
		}
	}
}

func (ic *InlineClient) refreshWorkspaceNavigation(ctx context.Context) error {
	_, err := ic.fetchWorkspaceNavigation(ctx, true)
	return err
}

// Serialize the source read as well as reconciliation. Otherwise an older HTTP
// response can arrive after a revocation has been persisted and reactivate it.
func (ic *InlineClient) fetchWorkspaceNavigation(ctx context.Context, refresh bool) (*sidecar.AccountStateSnapshot, error) {
	ic.workspaceStateMu.Lock()
	defer ic.workspaceStateMu.Unlock()
	var snapshot *sidecar.AccountStateSnapshot
	var err error
	if refresh {
		snapshot, err = ic.Sidecar.RefreshAccountState(ctx)
	} else {
		snapshot, err = ic.Sidecar.AccountState(ctx)
	}
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, fmt.Errorf("Inline returned no account navigation state")
	}
	if err := ic.reconcileWorkspaceNavigation(ctx, snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (ic *InlineClient) reconcileWorkspaceNavigation(ctx context.Context, snapshot *sidecar.AccountStateSnapshot) error {
	if ic.UserLogin == nil {
		return nil
	}
	ic.navigationMu.Lock()
	defer ic.navigationMu.Unlock()
	spaces := ic.workspaceNavigationSnapshot()
	selfID, err := parseInlineUserID(string(ic.GetUserID()))
	if err != nil {
		return err
	}
	applyWorkspaceAccess(spaces, snapshot, selfID)
	// Persist revocation before touching Matrix so a restart cannot recreate links
	// from the previous access view. Pending links are retried idempotently.
	if err := ic.saveWorkspaceNavigation(ctx, spaces); err != nil {
		return err
	}
	if ic.UserLogin.Bridge == nil {
		return nil
	}
	var firstErr error
	for _, space := range spaces {
		if space.Active {
			if err := ic.ensureWorkspaceRoom(ctx, space); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}
		for chatID, link := range space.Links {
			remove := !space.Active
			if parsed, err := strconv.ParseInt(chatID, 10, 64); err == nil {
				if dialog, ok := ic.cachedDialog(parsed); ok {
					remove = remove || !ic.shouldProjectDialog(dialog) || (dialog.StructureKnown && (dialog.SpaceID == nil || *dialog.SpaceID != space.SpaceID))
				}
			}
			if remove || !link.Attached {
				if !remove {
					parsed, err := strconv.ParseInt(chatID, 10, 64)
					if err != nil || parsed <= 0 || ic.Sidecar == nil {
						if firstErr == nil {
							firstErr = fmt.Errorf("pending Inline navigation has no verifiable source chat")
						}
						continue
					}
					if _, err := ic.Sidecar.ChatParticipants(ctx, sidecar.ChatParticipantsRequest{ChatID: parsed}); err != nil {
						if firstErr == nil {
							firstErr = err
						}
						continue
					}
				}
				if err := ic.sendWorkspaceLink(ctx, space.RoomID, link.RoomID, remove); err != nil {
					if firstErr == nil {
						firstErr = err
					}
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
	}
	if err := ic.saveWorkspaceNavigation(ctx, spaces); err != nil {
		return err
	}
	// Existing independently accessible rooms can be attached after a successful
	// refresh; no new chat portal is created merely because a workspace lists it.
	ic.mu.RLock()
	dialogs := make([]sidecar.DialogRecord, 0, len(ic.dialogs))
	for _, dialog := range ic.dialogs {
		dialogs = append(dialogs, dialog)
	}
	ic.mu.RUnlock()
	for _, dialog := range dialogs {
		portal, err := ic.UserLogin.Bridge.GetExistingPortalByKey(ctx, ic.portalKeyForChat(dialog.ChatID))
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := ic.syncDialogWorkspaceLinkLocked(ctx, portal, dialog, spaces); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (ic *InlineClient) ensureWorkspaceRoom(ctx context.Context, space *WorkspaceNavigation) error {
	portal, err := ic.UserLogin.Bridge.GetPortalByKey(ctx, networkid.PortalKey{ID: ic.workspacePortalID(space.SpaceID), Receiver: ic.UserLogin.ID})
	if err != nil {
		return err
	}
	if portal.MXID != "" && portal.RoomType != database.RoomTypeSpace {
		return fmt.Errorf("Inline workspace portal is not a Matrix Space")
	}
	info := ic.chatInfoForWorkspace(space)
	if portal.MXID == "" {
		err = portal.CreateMatrixRoom(ctx, ic.UserLogin, info)
	} else {
		portal.UpdateInfo(ctx, info, ic.UserLogin, nil, time.Time{})
	}
	if err != nil {
		return err
	}
	space.RoomID = portal.MXID
	return nil
}

func (ic *InlineClient) syncDialogWorkspaceLink(ctx context.Context, portal *bridgev2.Portal, dialog sidecar.DialogRecord) error {
	if ic.UserLogin == nil || ic.UserLogin.Bridge == nil {
		return nil
	}
	ic.navigationMu.Lock()
	defer ic.navigationMu.Unlock()
	return ic.syncDialogWorkspaceLinkLocked(ctx, portal, dialog, ic.workspaceNavigationSnapshot())
}

// A source chat deletion revokes its navigation independently of workspace
// membership. Persisted source deletion events keep this cleanup retryable.
func (ic *InlineClient) removeChatWorkspaceLinks(ctx context.Context, chatID int64) error {
	if ic.UserLogin == nil || ic.UserLogin.Bridge == nil {
		return nil
	}
	ic.navigationMu.Lock()
	defer ic.navigationMu.Unlock()
	spaces := ic.workspaceNavigationSnapshot()
	chatKey := strconv.FormatInt(chatID, 10)
	changed := false
	for _, space := range spaces {
		link, exists := space.Links[chatKey]
		if !exists {
			continue
		}
		if err := ic.sendWorkspaceLink(ctx, space.RoomID, link.RoomID, true); err != nil {
			return err
		}
		delete(space.Links, chatKey)
		changed = true
	}
	if changed {
		return ic.saveWorkspaceNavigation(ctx, spaces)
	}
	return nil
}

func (ic *InlineClient) syncDialogWorkspaceLinkLocked(ctx context.Context, portal *bridgev2.Portal, dialog sidecar.DialogRecord, spaces map[string]*WorkspaceNavigation) error {
	if portal == nil || portal.MXID == "" || !dialog.StructureKnown {
		return nil
	}
	chatKey := strconv.FormatInt(dialog.ChatID, 10)
	var desired *WorkspaceNavigation
	if dialog.SpaceID != nil && ic.shouldProjectDialog(dialog) && !isDMDialog(dialog) {
		desired = spaces[strconv.FormatInt(*dialog.SpaceID, 10)]
		if desired != nil && !desired.Active {
			desired = nil
		}
	}
	for _, space := range spaces {
		link, exists := space.Links[chatKey]
		if exists && (space != desired || link.RoomID != portal.MXID) {
			if err := ic.sendWorkspaceLink(ctx, space.RoomID, link.RoomID, true); err != nil {
				return err
			}
			delete(space.Links, chatKey)
			if err := ic.saveWorkspaceNavigation(ctx, spaces); err != nil {
				return err
			}
		}
	}
	if desired == nil {
		return nil
	}
	if desired.RoomID == "" {
		if err := ic.ensureWorkspaceRoom(ctx, desired); err != nil {
			return err
		}
	}
	if desired.Links == nil {
		desired.Links = make(map[string]WorkspaceRoomLink)
	}
	if link, exists := desired.Links[chatKey]; exists && link.RoomID == portal.MXID && link.Attached {
		return nil
	}
	// Workspace membership alone does not grant access to a private chat.
	// Only create a new navigation entry after this login can inspect its source audience.
	if ic.Sidecar == nil {
		return fmt.Errorf("Inline source access check is unavailable")
	}
	if _, err := ic.Sidecar.ChatParticipants(ctx, sidecar.ChatParticipantsRequest{ChatID: dialog.ChatID}); err != nil {
		return fmt.Errorf("verify Inline chat access before adding workspace navigation: %w", err)
	}
	desired.Links[chatKey] = WorkspaceRoomLink{RoomID: portal.MXID}
	if err := ic.saveWorkspaceNavigation(ctx, spaces); err != nil {
		return err
	}
	if err := ic.sendWorkspaceLink(ctx, desired.RoomID, portal.MXID, false); err != nil {
		return err
	}
	desired.Links[chatKey] = WorkspaceRoomLink{RoomID: portal.MXID, Attached: true}
	return ic.saveWorkspaceNavigation(ctx, spaces)
}

func (ic *InlineClient) sendWorkspaceLink(ctx context.Context, spaceID, roomID id.RoomID, remove bool) error {
	if spaceID == "" || roomID == "" {
		if remove {
			return nil
		}
		return fmt.Errorf("workspace room is not created yet")
	}
	via := []string{ic.UserLogin.Bridge.Matrix.ServerName()}
	if remove {
		via = nil
	}
	_, err := ic.UserLogin.Bridge.Bot.SendState(ctx, spaceID, event.StateSpaceChild, string(roomID), &event.Content{Parsed: &event.SpaceChildEventContent{Via: via}}, time.Now())
	return err
}
