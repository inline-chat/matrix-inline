package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func (ic *InlineClient) refreshWorkspaceSnapshot(ctx context.Context) error {
	snapshot, err := ic.Sidecar.AccountState(ctx)
	if err != nil {
		return err
	}
	ic.cacheWorkspaceSnapshot(snapshot)
	return nil
}
func (ic *InlineClient) cacheWorkspaceSnapshot(snapshot *sidecar.AccountStateSnapshot) bool {
	ic.cacheUsers(snapshot.Users)
	ic.mu.Lock()
	defer ic.mu.Unlock()
	oldSpaces, oldAccess, oldComplete := ic.spaces, ic.spaceAccess, ic.spaceAccessComplete
	ic.spaces = make(map[int64]sidecar.SpaceRecord, len(snapshot.Spaces))
	ic.spaceAccess = make(map[int64]string, len(snapshot.SpaceAccess))
	for _, s := range snapshot.Spaces {
		if s.SpaceID > 0 {
			ic.spaces[s.SpaceID] = s
		}
	}
	for _, a := range snapshot.SpaceAccess {
		if a.SpaceID > 0 {
			ic.spaceAccess[a.SpaceID] = a.Status
		}
	}
	ic.spaceAccessComplete = snapshot.SpaceAccessComplete
	changed := !reflect.DeepEqual(oldSpaces, ic.spaces) || !reflect.DeepEqual(oldAccess, ic.spaceAccess) || oldComplete != ic.spaceAccessComplete
	if changed {
		ic.workspaceRevision++
	}
	return changed
}
func (ic *InlineClient) desiredWorkspace(d sidecar.DialogRecord) (string, bool) {
	kind := classifyDialog(d)
	if kind == roomUnknown {
		return "", false
	}
	if kind == roomDirect || d.Structure.SpaceID == nil {
		return "", true
	}
	space := *d.Structure.SpaceID
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	access, observed := ic.spaceAccess[space]
	if access == "removed" || (!observed && ic.spaceAccessComplete) {
		return "", true
	}
	if access != "accessible" {
		return "", false
	} // Unknown access neither creates navigation nor proves an owned link should detach.
	if _, ok := ic.spaces[space]; !ok {
		return "", false
	}
	return fmt.Sprintf("space:%d", space), true
}
func (ic *InlineClient) workspaceChatInfo(space sidecar.SpaceRecord) *bridgev2.ChatInfo {
	rt := database.RoomTypeSpace
	name := space.Name
	return &bridgev2.ChatInfo{Name: &name, Type: &rt, Members: ic.chatMemberListForKnownSelf(context.Background()), CanBackfill: false}
}
func (ic *InlineClient) ensureWorkspace(ctx context.Context, d sidecar.DialogRecord) error {
	desired, known := ic.desiredWorkspace(d)
	if !known || desired == "" {
		return nil
	}
	ic.mu.RLock()
	space := ic.spaces[*d.Structure.SpaceID]
	ic.mu.RUnlock()
	result := ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: networkid.PortalKey{ID: networkid.PortalID(desired), Receiver: ic.UserLogin.ID}, CreatePortal: true},
		ChatInfo:  ic.workspaceChatInfo(space),
	})
	return eventHandlingResultError("workspace resync", result)
}
func (ic *InlineClient) projectDialogInLane(ctx context.Context, portal *bridgev2.Portal, d sidecar.DialogRecord) {
	if portal == nil || portal.Bridge == nil {
		return
	}
	s, k, err := ic.projectionJournal(ctx, string(portal.ID))
	if err != nil {
		return
	}
	p, err := s.load(ctx, k)
	if err != nil {
		return
	}
	desired, known := ic.desiredWorkspace(d)
	p.NotificationPolicy = notificationPolicyDegraded
	if portal.ParentKey.ID != "" && !strings.HasPrefix(string(portal.ParentKey.ID), "space:") && p.Migration == nil {
		p.ParentError = "unowned_parent_preserved"
		_ = s.save(ctx, k, &p)
		ic.reconcileProjection(ctx, portal)
		return
	}
	if p.Migration != nil && p.Migration.Phase == "rolled_back" {
		portal.RoomType = p.Migration.Before.Kind
		ic.reconcileProjection(ctx, portal)
		return
	}
	if p.Migration != nil && p.Migration.Phase == "conflict" {
		// UpdateInfo may have assigned the source type before our hook. Preserve
		// the stored value while the conflict waits for explicit reselection.
		if stored, err := portal.Bridge.DB.Portal.GetByKey(ctx, portal.PortalKey); err == nil && stored != nil {
			portal.RoomType = stored.RoomType
		}
		ic.reconcileProjection(ctx, portal)
		return
	}
	// During an explicit migration/rollback, its journal owns presentation.
	if p.Migration != nil && p.Migration.Phase != "applied" && p.Migration.Phase != "rolled_back" && p.Migration.Phase != "conflict" {
		ic.runMigrationInLane(ctx, portal, s, k, &p)
		return
	}
	if known {
		p.DesiredParent = desired
		p.ParentKnown = true
		p.ParentPending = true
	}
	if portal.Parent != nil && strings.HasPrefix(string(portal.Parent.ID), "space:") && portal.Parent.Receiver == portal.Receiver && portal.Parent.MXID != "" && !slices.Contains(p.OwnedParents, portal.Parent.MXID) {
		p.OwnedParents = append(p.OwnedParents, portal.Parent.MXID)
	}
	if err = s.save(ctx, k, &p); err != nil {
		return
	} // durable old and desired parent BEFORE external effects
	if known && p.ParentKnown && p.ParentPending && portal.MXID != "" {
		if err = ic.repairHierarchy(ctx, portal, s, k, &p); err != nil {
			p.ParentError = "hierarchy_write_or_readback_failed"
			_ = s.save(ctx, k, &p)
		}
	}
	ic.reconcileProjection(ctx, portal)
}

// hierarchyWire writes and reads the two independent Matrix state events.
// It deliberately avoids v0.28's updateParent, which retains Parent on detach.
type hierarchyWire interface {
	put(context.Context, id.RoomID, event.Type, string, any) error
	get(context.Context, id.RoomID, event.Type, string, any) error
}
type matrixHierarchyWire struct{ raw *matrix.ASIntent }

func (w matrixHierarchyWire) put(ctx context.Context, room id.RoomID, typ event.Type, key string, value any) error {
	_, err := w.raw.SendState(ctx, room, typ, key, &event.Content{Parsed: value}, time.Time{})
	return err
}
func (w matrixHierarchyWire) get(ctx context.Context, room id.RoomID, typ event.Type, key string, value any) error {
	return w.raw.Matrix.Client.StateEvent(ctx, room, typ, key, value)
}

func ensureHierarchyLink[T any](ctx context.Context, w hierarchyWire, room id.RoomID, typ event.Type, key string, desired T, correct func(T) bool) error {
	var current T
	err := w.get(ctx, room, typ, key, &current)
	if err != nil && !errors.Is(err, mautrix.MNotFound) {
		return err
	}
	if correct(current) {
		return nil
	}
	if err = w.put(ctx, room, typ, key, desired); err != nil {
		return err
	}
	var confirmed T
	err = w.get(ctx, room, typ, key, &confirmed)
	if err != nil && !errors.Is(err, mautrix.MNotFound) {
		return err
	}
	if !correct(confirmed) {
		return errors.New("hierarchy readback differs")
	}
	return nil
}
func reconcileHierarchyLinks(ctx context.Context, w hierarchyWire, child id.RoomID, owned []id.RoomID, target id.RoomID, via string) error {
	for _, old := range owned {
		if old == target {
			continue
		}
		if err := ensureHierarchyLink(ctx, w, old, event.StateSpaceChild, string(child), event.SpaceChildEventContent{}, func(c event.SpaceChildEventContent) bool { return len(c.Via) == 0 }); err != nil {
			return err
		}
		if err := ensureHierarchyLink(ctx, w, child, event.StateSpaceParent, string(old), event.SpaceParentEventContent{}, func(p event.SpaceParentEventContent) bool { return len(p.Via) == 0 && !p.Canonical }); err != nil {
			return err
		}
	}
	if target != "" {
		if err := ensureHierarchyLink(ctx, w, target, event.StateSpaceChild, string(child), event.SpaceChildEventContent{Via: []string{via}}, func(c event.SpaceChildEventContent) bool { return slices.Contains(c.Via, via) }); err != nil {
			return err
		}
		if err := ensureHierarchyLink(ctx, w, child, event.StateSpaceParent, string(target), event.SpaceParentEventContent{Via: []string{via}, Canonical: true}, func(p event.SpaceParentEventContent) bool { return slices.Contains(p.Via, via) && p.Canonical }); err != nil {
			return err
		}
	}
	return nil
}

func (ic *InlineClient) repairHierarchy(ctx context.Context, portal *bridgev2.Portal, s *projectionStore, k projectionKey, p *projectionState) error {
	return ic.repairHierarchyState(ctx, portal, s, k, p, true)
}
func (ic *InlineClient) repairHierarchyState(ctx context.Context, portal *bridgev2.Portal, s *projectionStore, k projectionKey, p *projectionState, writeLinks bool) error {
	raw, ok := portal.Bridge.Bot.(*matrix.ASIntent)
	if !ok || raw.Matrix == nil {
		return errors.New("hierarchy readback unsupported")
	}
	var parent *bridgev2.Portal
	var target id.RoomID
	if p.DesiredParent != "" {
		var err error
		parent, err = portal.Bridge.GetExistingPortalByKey(ctx, networkid.PortalKey{ID: networkid.PortalID(p.DesiredParent), Receiver: portal.Receiver})
		if err != nil {
			return err
		}
		if parent == nil || parent.MXID == "" || parent.RoomType != database.RoomTypeSpace {
			return errors.New("workspace not yet created")
		}
		target = parent.MXID
		if !slices.Contains(p.OwnedParents, target) {
			p.OwnedParents = append(p.OwnedParents, target)
			if err = s.save(ctx, k, p); err != nil {
				return err
			}
		}
	}
	via := portal.Bridge.Matrix.ServerName()
	var err error
	if writeLinks {
		if err = reconcileHierarchyLinks(ctx, matrixHierarchyWire{raw}, portal.MXID, p.OwnedParents, target, via); err != nil {
			return err
		}
	}
	portal.Parent = parent
	portal.ParentKey = networkid.PortalKey{}
	portal.InSpace = parent != nil
	if parent != nil {
		portal.ParentKey = parent.PortalKey
	}
	if verifyBridgeInfo(ctx, raw, portal) != nil {
		portal.UpdateBridgeInfo(ctx)
	}
	if err = portal.Save(ctx); err != nil {
		return err
	}
	stored, err := portal.Bridge.DB.Portal.GetByKey(ctx, portal.PortalKey)
	if err != nil {
		return err
	}
	if stored == nil || stored.ParentKey != portal.ParentKey || stored.InSpace != portal.InSpace {
		return errors.New("portal parent readback failed")
	}
	if err = verifyBridgeInfo(ctx, raw, portal); err != nil {
		return err
	}
	p.OwnedParents = nil
	if target != "" {
		p.OwnedParents = []id.RoomID{target}
	}
	p.AppliedParent = p.DesiredParent
	p.ParentPending = false
	p.ParentError = ""
	return s.save(ctx, k, p)
}
func verifyBridgeInfo(ctx context.Context, raw *matrix.ASIntent, p *bridgev2.Portal) error {
	state, err := raw.Matrix.Client.State(ctx, p.MXID)
	if err != nil {
		return err
	}
	for _, typ := range []event.Type{event.StateBridge, event.StateHalfShotBridge} {
		found := false
		for _, evt := range state[typ] {
			var info event.BridgeEventContent
			encoded, err := evt.Content.MarshalJSON()
			if err != nil {
				return err
			}
			if err = json.Unmarshal(encoded, &info); err != nil {
				return err
			}
			if info.Channel.ID == string(p.ID) && info.Channel.Receiver == string(p.Receiver) {
				if info.BeeperRoomTypeV2 != string(p.RoomType) {
					return errors.New("bridge room classification readback differs")
				}
				if p.RoomType == database.RoomTypeDefault && info.BeeperRoomType != "" {
					return errors.New("legacy room classification remains")
				}
				if p.Parent == nil && info.Network != nil {
					return errors.New("stale bridge network after detach")
				}
				if p.Parent != nil && (info.Network == nil || info.Network.ID != string(p.Parent.ID)) {
					return errors.New("bridge parent readback differs")
				}
				found = true
			}
		}
		if !found {
			return errors.New("bridge info missing")
		}
	}
	return nil
}
