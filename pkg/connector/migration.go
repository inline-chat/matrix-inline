package connector

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"slices"
	"strconv"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

type presentationValue struct {
	RoomID id.RoomID         `json:"room_id"`
	Kind   database.RoomType `json:"kind"`
	Parent string            `json:"parent"`
}
type presentationMigration struct {
	Before       presentationValue `json:"before"`
	Applied      presentationValue `json:"applied"`
	Phase        string            `json:"phase"`
	Parents      []id.RoomID       `json:"parents"`
	BeforeWire   map[string]string `json:"before_wire"`
	AppliedWire  map[string]string `json:"applied_wire"`
	RollbackWire map[string]string `json:"rollback_wire"`
}

func presentationOf(p *bridgev2.Portal) presentationValue {
	return presentationValue{RoomID: p.MXID, Kind: p.RoomType, Parent: string(p.ParentKey.ID)}
}
func migrationRollbackAllowed(current presentationValue, m *presentationMigration) bool {
	return current == m.Applied
}
func (ic *InlineClient) runMigrationInLane(ctx context.Context, portal *bridgev2.Portal, s *projectionStore, k projectionKey, p *projectionState) {
	m := p.Migration
	if m == nil || portal.MXID == "" || m.Phase == "applied" || m.Phase == "rolled_back" || m.Phase == "conflict" {
		return
	}
	raw, ok := portal.Bridge.Bot.(*matrix.ASIntent)
	if !ok || raw.Matrix == nil {
		p.ParentError = "migration_readback_unsupported"
		_ = s.save(ctx, k, p)
		return
	}
	rollback := m.Phase == "rollback_pending" || m.Phase == "rolling_back"
	current, err := migrationWireSnapshot(ctx, raw.Matrix.Client, portal, m.Parents)
	if err != nil {
		p.ParentError = "migration_readback_failed"
		_ = s.save(ctx, k, p)
		return
	}
	conflict := !migrationValuesCompatible(presentationOf(portal), m.Before, m.Applied)
	if m.Phase == "rollback_pending" {
		conflict = conflict || !migrationRollbackAllowed(presentationOf(portal), m) || !maps.Equal(current, m.AppliedWire)
	} else if rollback {
		conflict = conflict || !migrationWireCompatible(current, m.RollbackWire, m.AppliedWire)
	} else {
		conflict = conflict || !migrationWireCompatible(current, m.BeforeWire, m.AppliedWire)
	}
	if conflict {
		m.Phase = "conflict"
		p.ParentError = "migration_user_change_conflict"
		_ = s.save(ctx, k, p)
		return
	}
	desired := m.Applied
	m.Phase = "applying"
	if rollback {
		desired = m.Before
		m.Phase = "rolling_back"
	}
	if desired.RoomID != portal.MXID || portal.RoomType == database.RoomTypeSpace || desired.Kind == database.RoomTypeSpace {
		m.Phase = "conflict"
		_ = s.save(ctx, k, p)
		return
	}
	p.DesiredParent = desired.Parent
	p.ParentKnown = true
	p.ParentPending = true
	// Keep all affected links durable through every partial apply/rollback.
	p.OwnedParents = slices.Clone(m.Parents)
	if err = s.save(ctx, k, p); err != nil {
		return
	}
	expected := m.AppliedWire
	if rollback {
		expected = m.RollbackWire
	}
	if err = restoreMigrationLinks(ctx, matrixHierarchyWire{raw}, portal.MXID, m.Parents, expected, m.BeforeWire, m.AppliedWire); err != nil {
		p.ParentError = "migration_links_pending"
		if errors.Is(err, errProjectionConflict) {
			m.Phase = "conflict"
			p.ParentError = "migration_user_change_conflict"
		}
		_ = s.save(ctx, k, p)
		return
	}
	// Recheck immediately before metadata writes. Matrix offers no conditional
	// state transaction; operators must keep this migration window controlled.
	current, err = migrationWireSnapshot(ctx, raw.Matrix.Client, portal, m.Parents)
	if err != nil || !migrationWireCompatible(current, m.BeforeWire, m.AppliedWire) {
		p.ParentError = "migration_readback_failed"
		if err == nil {
			m.Phase = "conflict"
			p.ParentError = "migration_user_change_conflict"
		}
		_ = s.save(ctx, k, p)
		return
	}
	portal.RoomType = desired.Kind
	// Exact journaled link writes above are the only link effects in migration.
	if err = ic.repairHierarchyState(ctx, portal, s, k, p, false); err != nil {
		p.ParentError = "migration_pending"
		_ = s.save(ctx, k, p)
		return
	}
	current, err = migrationWireSnapshot(ctx, raw.Matrix.Client, portal, m.Parents)
	if err != nil || !maps.Equal(current, expected) {
		p.ParentPending = true
		p.ParentError = "migration_readback_failed"
		_ = s.save(ctx, k, p)
		return
	}
	if rollback {
		m.Phase = "rolled_back"
	} else {
		m.Phase = "applied"
	}
	p.ParentError = ""
	_ = s.save(ctx, k, p)
}

func (ic *InlineClient) prepareMigration(ctx context.Context, portal *bridgev2.Portal, mode string) error {
	s, k, err := ic.projectionJournal(ctx, string(portal.ID))
	if err != nil {
		return err
	}
	p, err := s.load(ctx, k)
	if err != nil {
		return err
	}
	if mode == "rollback" {
		if p.Migration == nil {
			return errors.New("no applied migration for selected room")
		}
		switch p.Migration.Phase {
		case "rollback_pending", "rolling_back", "rolled_back":
			return nil
		case "applied":
			p.Migration.Phase = "rollback_pending"
		default:
			return errors.New("no applied migration for selected room")
		}
	} else {
		chat, err := chatIDFromPortal(portal)
		if err != nil {
			return err
		}
		d, ok := ic.cachedDialog(chat)
		if !ok || !ic.shouldProjectDialog(d) || classifyDialog(d) == roomUnknown {
			return errors.New("selected room has unknown or excluded source structure")
		}
		parent, known := ic.desiredWorkspace(d)
		if !known {
			return errors.New("selected room parent is unknown")
		}
		kind := database.RoomTypeDefault
		if isDMDialog(d) {
			kind = database.RoomTypeDM
		}
		if portal.RoomType == database.RoomTypeSpace {
			return errors.New("cannot change Matrix space type")
		}
		if p.Migration != nil && p.Migration.Phase != "rolled_back" && p.Migration.Phase != "conflict" {
			return nil // Repeated selection resumes the same durable operation.
		}
		m := &presentationMigration{Before: presentationOf(portal), Applied: presentationValue{RoomID: portal.MXID, Kind: kind, Parent: parent}, Phase: "pending"}
		m.Parents = slices.Clone(p.OwnedParents)
		if portal.Parent != nil && portal.Parent.MXID != "" && !slices.Contains(m.Parents, portal.Parent.MXID) {
			m.Parents = append(m.Parents, portal.Parent.MXID)
		}
		var target id.RoomID
		if parent != "" {
			resolved, err := portal.Bridge.GetExistingPortalByKey(ctx, networkid.PortalKey{ID: networkid.PortalID(parent), Receiver: portal.Receiver})
			if err != nil {
				return err
			}
			if resolved == nil || resolved.MXID == "" {
				return errors.New("selected workspace not ready")
			}
			target = resolved.MXID
			if !slices.Contains(m.Parents, target) {
				m.Parents = append(m.Parents, target)
			}
		}
		raw, ok := portal.Bridge.Bot.(*matrix.ASIntent)
		if !ok || raw.Matrix == nil {
			return errors.New("migration readback unsupported")
		}
		m.BeforeWire, err = migrationWireSnapshot(ctx, raw.Matrix.Client, portal, m.Parents)
		if err != nil {
			return err
		}
		m.AppliedWire = expectedMigrationWire(m.BeforeWire, m.Applied, m.Parents, target, portal.Bridge.Matrix.ServerName())
		m.RollbackWire = migrationBridgeValues(m.BeforeWire, m.Before)
		p.Migration = m
	}
	// Selection is durable before the event lane performs external writes.
	return s.save(ctx, k, &p)
}

func parseMigrationCommand(args []string) (string, []int64, error) {
	mode := "preview"
	if len(args) > 0 {
		switch args[0] {
		case "preview", "apply", "rollback":
			mode = args[0]
			args = args[1:]
		}
	}
	if len(args) == 0 || len(args) > 50 {
		return "", nil, errors.New("select 1–50 numeric chat IDs explicitly")
	}
	ids := make([]int64, 0, len(args))
	seen := map[int64]bool{}
	for _, arg := range args {
		n, err := strconv.ParseInt(arg, 10, 64)
		if err != nil || n <= 0 || strconv.FormatInt(n, 10) != arg || seen[n] {
			return "", nil, errors.New("select unique positive numeric chat IDs")
		}
		seen[n] = true
		ids = append(ids, n)
	}
	return mode, ids, nil
}
func (ic *InlineClient) migrationPostHandler(ctx context.Context, p *bridgev2.Portal) {
	s, k, err := ic.projectionJournal(ctx, string(p.ID))
	if err != nil {
		return
	}
	state, err := s.load(ctx, k)
	if err == nil {
		ic.runMigrationInLane(ctx, p, s, k, &state)
	}
}
func (ic *InlineClient) migrateSelected(ctx context.Context, mode string, ids []int64) string {
	var out strings.Builder
	fmt.Fprintf(&out, "Presentation migration: %s (version %d)\n", mode, projectionVersion)
	for _, chat := range ids {
		portal, err := ic.UserLogin.Bridge.GetExistingPortalByKey(ctx, ic.portalKeyForChat(chat))
		if err != nil || portal == nil || portal.MXID == "" {
			fmt.Fprintf(&out, "- %d: no existing room for this login\n", chat)
			continue
		}
		if mode == "preview" {
			d, ok := ic.cachedDialog(chat)
			parent, known := ic.desiredWorkspace(d)
			eligibility := "eligible"
			if !ok || classifyDialog(d) == roomUnknown || !known {
				eligibility = "ambiguous; refresh required"
			} else if !ic.shouldProjectDialog(d) {
				eligibility = "hidden; excluded"
			}
			fmt.Fprintf(&out, "- %d: room `%s`, kind `%s` → `%s`, parent `%s` → `%s`; %s. Notification/tag and delivery state preserved.\n", chat, portal.MXID, portal.RoomType, classifyDialog(d), portal.ParentKey.ID, parent, eligibility)
			continue
		}
		if mode == "apply" {
			if d, ok := ic.cachedDialog(chat); ok {
				if err := ic.ensureWorkspace(ctx, d); err != nil {
					fmt.Fprintf(&out, "- %d: pending workspace creation\n", chat)
					continue
				}
			}
		}
		var prepareErr error
		result := ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: portal.PortalKey, PostHandleFunc: func(ctx context.Context, p *bridgev2.Portal) {
				if prepareErr == nil {
					ic.migrationPostHandler(ctx, p)
				}
			}},
			GetChatInfoFunc: func(ctx context.Context, p *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
				prepareErr = ic.prepareMigration(ctx, p, mode)
				if prepareErr != nil {
					return nil, prepareErr
				}
				return ic.migrationGatedChatInfo(ctx, p, &bridgev2.ChatInfo{})
			},
		})
		if prepareErr != nil {
			fmt.Fprintf(&out, "- %d: %s\n", chat, prepareErr)
			continue
		}
		if err := eventHandlingResultError("selected migration", result); err != nil {
			fmt.Fprintf(&out, "- %d: pending portal handling\n", chat)
			continue
		}
		s, k, err := ic.projectionJournal(ctx, string(portal.ID))
		if err != nil {
			fmt.Fprintf(&out, "- %d: journal unavailable\n", chat)
			continue
		}
		p, err := s.load(ctx, k)
		if err != nil || p.Migration == nil {
			fmt.Fprintf(&out, "- %d: journal unavailable\n", chat)
			continue
		}
		fmt.Fprintf(&out, "- %d: %s\n", chat, p.Migration.Phase)
	}
	return out.String()
}

var errMigrationPresentationHeld = errors.New("presentation updates held by migration journal")

// This gate must run in the portal lane before the framework sees any ChatInfo.
// ExtraUpdates is too late: UpdateInfo may already have changed type/name and
// will write bridge metadata after that hook even when the hook reports conflict.
func (ic *InlineClient) guardMigrationPresentation(ctx context.Context, portal *bridgev2.Portal) error {
	if portal == nil || portal.MXID == "" || strings.HasPrefix(string(portal.ID), "space:") {
		return nil
	}
	store, key, err := ic.projectionJournal(ctx, string(portal.ID))
	if err != nil {
		return err
	}
	state, err := store.load(ctx, key)
	if err != nil {
		return err
	}
	if state.Migration == nil || state.Migration.Phase == "applied" {
		return nil
	}
	if state.Migration.Phase != "conflict" && state.Migration.Phase != "rolled_back" {
		ic.runMigrationInLane(ctx, portal, store, key, &state)
		state, err = store.load(ctx, key)
		if err != nil {
			return err
		}
		if state.Migration != nil && state.Migration.Phase == "applied" {
			return nil
		}
	}
	return errMigrationPresentationHeld
}

func (ic *InlineClient) migrationGatedChatInfo(ctx context.Context, portal *bridgev2.Portal, info *bridgev2.ChatInfo) (*bridgev2.ChatInfo, error) {
	if err := ic.guardMigrationPresentation(ctx, portal); err != nil {
		return nil, err
	}
	return info, nil
}
func (ic *InlineClient) migrationGatedInfoProvider(info *bridgev2.ChatInfo) func(context.Context, *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	return func(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
		return ic.migrationGatedChatInfo(ctx, portal, info)
	}
}
