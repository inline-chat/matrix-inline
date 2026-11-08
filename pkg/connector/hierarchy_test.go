package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"hegel.dev/go/hegel"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type fixtureHierarchy struct {
	state         map[string][]byte
	failAt, calls int
	writes        int
}

func (f *fixtureHierarchy) key(room id.RoomID, typ event.Type, key string) string {
	return fmt.Sprintf("%s/%s/%s", room, typ.Type, key)
}
func (f *fixtureHierarchy) put(_ context.Context, room id.RoomID, typ event.Type, key string, value any) error {
	f.calls++
	f.writes++
	if f.failAt == f.calls {
		return errors.New("injected state write failure")
	}
	raw, err := json.Marshal(value)
	if err == nil {
		f.state[f.key(room, typ, key)] = raw
	}
	return err
}
func (f *fixtureHierarchy) get(_ context.Context, room id.RoomID, typ event.Type, key string, value any) error {
	f.calls++
	if f.failAt == f.calls {
		return errors.New("injected readback failure")
	}
	raw := f.state[f.key(room, typ, key)]
	if raw == nil {
		return mautrix.MNotFound
	}
	return json.Unmarshal(raw, value)
}
func TestHierarchyBothSidesMoveDetachAndFailureRecovery(t *testing.T) {
	for fail := 0; fail <= 8; fail++ {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			f := &fixtureHierarchy{state: map[string][]byte{}}
			ctx := t.Context()
			child := id.RoomID("!child:test")
			a := id.RoomID("!a:test")
			b := id.RoomID("!b:test")
			if err := reconcileHierarchyLinks(ctx, f, child, nil, a, "test"); err != nil {
				t.Fatal(err)
			}
			f.calls = 0
			f.failAt = fail
			err := reconcileHierarchyLinks(ctx, f, child, []id.RoomID{a, b}, b, "test")
			if fail > 0 && err == nil {
				t.Fatalf("boundary %d incorrectly completed", fail)
			}
			// Replay after a fresh owner loads the same remote state. Every partial side
			// effect is repaired; no external-success callback is treated as a checkpoint.
			restarted := &fixtureHierarchy{state: f.state}
			if err := reconcileHierarchyLinks(ctx, restarted, child, []id.RoomID{a, b}, b, "test"); err != nil {
				t.Fatal(err)
			}
			var old event.SpaceParentEventContent
			var current event.SpaceParentEventContent
			if err := restarted.get(ctx, child, event.StateSpaceParent, string(a), &old); err != nil {
				t.Fatal(err)
			}
			if err := restarted.get(ctx, child, event.StateSpaceParent, string(b), &current); err != nil {
				t.Fatal(err)
			}
			if old.Canonical || len(old.Via) != 0 || !current.Canonical || len(current.Via) == 0 {
				t.Fatalf("move old=%+v current=%+v", old, current)
			}
			if err := reconcileHierarchyLinks(ctx, restarted, child, []id.RoomID{a, b}, "", "test"); err != nil {
				t.Fatal(err)
			}
			current = event.SpaceParentEventContent{}
			if err := restarted.get(ctx, child, event.StateSpaceParent, string(b), &current); err != nil {
				t.Fatal(err)
			}
			if current.Canonical || len(current.Via) > 0 {
				t.Fatal("canonical parent survived detach")
			}
		})
	}
}
func TestMigrationRollbackCompareAndSet(t *testing.T) {
	before := presentationValue{RoomID: "!same:test", Kind: database.RoomTypeGroupDM, Parent: "space:1"}
	applied := presentationValue{RoomID: "!same:test", Kind: database.RoomTypeDefault, Parent: "space:2"}
	m := &presentationMigration{Before: before, Applied: applied}
	if !migrationRollbackAllowed(applied, m) {
		t.Fatal("own applied value rejected")
	}
	changed := applied
	changed.Parent = "space:3"
	if migrationRollbackAllowed(changed, m) {
		t.Fatal("rollback overwrites user parent choice")
	}
	changed = applied
	changed.Kind = database.RoomTypeDM
	if migrationRollbackAllowed(changed, m) {
		t.Fatal("rollback overwrites later room classification")
	}
	changed = applied
	changed.RoomID = "!replacement:test"
	if migrationRollbackAllowed(changed, m) {
		t.Fatal("rollback targets different Matrix room")
	}
}
func TestWorkspaceUnknownAndRemovedNeverLink(t *testing.T) {
	d := policyDialog(t, `{"chat_id":7,"space_id":1,"structure":{"kind":"channel","space_id":1,"peer_user_id":null,"parent_chat_id":null,"parent_message_id":null}}`)
	client := &InlineClient{}
	if parent, _ := client.desiredWorkspace(d); parent != "" {
		t.Fatal("unloaded access linked")
	}
	client.spaces = map[int64]sidecar.SpaceRecord{1: {SpaceID: 1, Name: "private"}}
	for _, status := range []string{"unknown", "removed", ""} {
		client.spaceAccess = map[int64]string{1: status}
		if parent, _ := client.desiredWorkspace(d); parent != "" {
			t.Fatalf("%s linked stale workspace", status)
		}
	}
	client.spaceAccess = map[int64]string{1: "accessible"}
	if parent, known := client.desiredWorkspace(d); !known || parent != "space:1" {
		t.Fatal("accessible source-backed workspace not linked")
	}
}

func TestWorkspaceAccessChangesInvalidatePresentation(t *testing.T) {
	ic := &InlineClient{}
	snap := &sidecar.AccountStateSnapshot{Spaces: []sidecar.SpaceRecord{{SpaceID: 1, Name: "one"}}, SpaceAccess: []sidecar.SpaceAccessRecord{{SpaceID: 1, Status: "accessible"}}}
	if !ic.cacheWorkspaceSnapshot(snap) {
		t.Fatal("initial workspace snapshot did not change")
	}
	if ic.cacheWorkspaceSnapshot(snap) {
		t.Fatal("unchanged snapshot reprojected")
	}
	snap.SpaceAccess[0].Status = "removed"
	if !ic.cacheWorkspaceSnapshot(snap) {
		t.Fatal("access-only loss did not invalidate hierarchy")
	}
	snap.Spaces[0].Name = "renamed"
	if !ic.cacheWorkspaceSnapshot(snap) {
		t.Fatal("name-only change did not invalidate workspace")
	}
}

func TestMigrationMixedFieldsResumeButThirdValuesConflict(t *testing.T) {
	before := presentationValue{RoomID: "!same:test", Kind: database.RoomTypeGroupDM, Parent: "space:1"}
	after := presentationValue{RoomID: "!same:test", Kind: database.RoomTypeDefault, Parent: "space:2"}
	for _, kind := range []database.RoomType{before.Kind, after.Kind} {
		for _, parent := range []string{before.Parent, after.Parent} {
			if !migrationValuesCompatible(presentationValue{RoomID: before.RoomID, Kind: kind, Parent: parent}, before, after) {
				t.Fatal("partial own effect cannot resume")
			}
		}
	}
	if migrationValuesCompatible(presentationValue{RoomID: before.RoomID, Kind: database.RoomTypeDM, Parent: after.Parent}, before, after) {
		t.Fatal("third kind overwritten")
	}
	if migrationValuesCompatible(presentationValue{RoomID: before.RoomID, Kind: after.Kind, Parent: "space:3"}, before, after) {
		t.Fatal("third parent overwritten")
	}
}
func TestMigrationRemoteGuardRejectsUserEditWithUnchangedCache(t *testing.T) {
	before := map[string]string{"kind": "group_dm", "parent": "space:1"}
	after := map[string]string{"kind": "", "parent": "space:2"}
	if !migrationWireCompatible(map[string]string{"kind": "", "parent": "space:1"}, before, after) {
		t.Fatal("crash intermediate rejected")
	}
	for _, changed := range []map[string]string{{"kind": "", "parent": "space:3"}, {"kind": "", "parent": "space:2", "parent:!manual:test": "linked"}} {
		if migrationWireCompatible(changed, before, after) {
			t.Fatal("user Matrix change overwritten despite unchanged local cache")
		}
	}
	if migrationWireCompatible(after, nil, nil) {
		t.Fatal("missing journal permits mutation")
	}
}

func TestMigrationRollbackRestartsAtEveryLinkBoundary(t *testing.T) {
	for fail := 1; fail <= 12; fail++ {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			child, a, b := id.RoomID("!child:test"), id.RoomID("!a:test"), id.RoomID("!b:test")
			before := map[string]string{"parent:" + string(a): `{"canonical":true,"via":["test","extra"]}`, "child:" + string(a): `{"suggested":true,"via":["test","extra"]}`, "parent:" + string(b): `{}`, "child:" + string(b): `{}`}
			f := &fixtureHierarchy{state: map[string][]byte{}}
			if err := reconcileHierarchyLinks(t.Context(), f, child, []id.RoomID{a, b}, b, "test"); err != nil {
				t.Fatal(err)
			}
			store := fixtureJournal(t, filepath.Join(t.TempDir(), "migration.db"))
			key := projectionKey{Bridge: "b", User: "@u:test", Login: "1", Receiver: "1", Chat: "7"}
			p, _ := store.load(t.Context(), key)
			p.ReadMaxID = 77
			p.ReadApplied = 33
			applied := expectedMigrationWire(before, presentationValue{}, []id.RoomID{a, b}, b, "test")
			p.Migration = &presentationMigration{AppliedWire: applied, Phase: "rolling_back", Before: presentationValue{RoomID: child, Kind: database.RoomTypeGroupDM, Parent: "space:1"}, Applied: presentationValue{RoomID: child, Kind: database.RoomTypeDefault, Parent: "space:2"}, BeforeWire: before, Parents: []id.RoomID{a, b}}
			if err := store.save(t.Context(), key, &p); err != nil {
				t.Fatal(err)
			}
			f.calls = 0
			f.failAt = fail
			if err := restoreMigrationLinks(t.Context(), f, child, []id.RoomID{a, b}, before, applied); err == nil {
				t.Fatal("injected rollback failure was checkpointed")
			}
			for field, original := range before {
				room, typ, key := child, event.StateSpaceParent, strings.TrimPrefix(field, "parent:")
				if strings.HasPrefix(field, "child:") {
					room, typ, key = id.RoomID(strings.TrimPrefix(field, "child:")), event.StateSpaceChild, string(child)
				}
				var content map[string]any
				if err := (&fixtureHierarchy{state: f.state}).get(t.Context(), room, typ, key, &content); err != nil && !errors.Is(err, mautrix.MNotFound) {
					t.Fatal(err)
				}
				value := "{}"
				if content != nil {
					value = migrationJSON(content)
				}
				if value != original && value != applied[field] {
					t.Fatal("bridge created unjournaled intermediate", field, value)
				}
			}
			// Crash after any put/read: journal still says rolling_back. A fresh owner
			// restores exactly the saved link values, including previous metadata.
			loaded, err := store.load(t.Context(), key)
			if err != nil || loaded.Migration.Phase != "rolling_back" || loaded.ReadMaxID != 77 || loaded.ReadApplied != 33 {
				t.Fatal("rollback changed read state or lost phase", err)
			}
			restarted := &fixtureHierarchy{state: f.state}
			if err = restoreMigrationLinks(t.Context(), restarted, child, loaded.Migration.Parents, loaded.Migration.BeforeWire, loaded.Migration.AppliedWire); err != nil {
				t.Fatal(err)
			}
			for _, link := range []struct {
				room      id.RoomID
				typ       event.Type
				key, want string
			}{{child, event.StateSpaceParent, string(a), before["parent:"+string(a)]}, {a, event.StateSpaceChild, string(child), before["child:"+string(a)]}, {child, event.StateSpaceParent, string(b), "{}"}, {b, event.StateSpaceChild, string(child), "{}"}} {
				var got map[string]any
				if err = restarted.get(t.Context(), link.room, link.typ, link.key, &got); err != nil || migrationJSON(got) != link.want {
					t.Fatal("rollback did not restore selected fields", err)
				}
			}
		})
	}
}

func TestMigrationCrashStatesHaveOnlyBeforeOrAppliedFields(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		oldKind := hegel.Draw(ht, hegel.SampledFrom([]string{"", "dm", "group_dm"}))
		oldParent := fmt.Sprintf("space:%d", hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64)))
		newParent := fmt.Sprintf("space:%d", hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64)))
		before := map[string]string{"kind": oldKind, "parent": oldParent}
		after := map[string]string{"kind": "", "parent": newParent}
		current := map[string]string{"kind": oldKind, "parent": oldParent}
		if hegel.Draw(ht, hegel.Booleans()) {
			current["kind"] = after["kind"]
		}
		if hegel.Draw(ht, hegel.Booleans()) {
			current["parent"] = after["parent"]
		}
		if !migrationWireCompatible(current, before, after) {
			ht.Fatal("own partial write cannot resume")
		}
		current["parent"] = "space:third"
		if migrationWireCompatible(current, before, after) {
			ht.Fatal("user parent choice allowed to overwrite")
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestHierarchyStableResyncDoesNotEmitMoreState(t *testing.T) {
	f := &fixtureHierarchy{state: map[string][]byte{}}
	child, parent := id.RoomID("!child:test"), id.RoomID("!parent:test")
	if err := reconcileHierarchyLinks(t.Context(), f, child, []id.RoomID{parent}, parent, "test"); err != nil {
		t.Fatal(err)
	}
	writes := f.writes
	if err := reconcileHierarchyLinks(t.Context(), f, child, []id.RoomID{parent}, parent, "test"); err != nil {
		t.Fatal(err)
	}
	if f.writes != writes {
		t.Fatal("unchanged hierarchy resync emitted Matrix state", f.writes-writes)
	}
	if err := reconcileHierarchyLinks(t.Context(), f, child, []id.RoomID{parent}, "", "test"); err != nil {
		t.Fatal(err)
	}
	writes = f.writes
	if err := reconcileHierarchyLinks(t.Context(), f, child, []id.RoomID{parent}, "", "test"); err != nil {
		t.Fatal(err)
	}
	if f.writes != writes {
		t.Fatal("unchanged detach emitted Matrix state")
	}
}
