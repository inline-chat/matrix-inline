package connector

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"hegel.dev/go/hegel"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type workspaceWireFixture struct {
	path   string
	wire   map[string]json.RawMessage
	calls  int
	ic     *InlineClient
	portal *bridgev2.Portal
	server *httptest.Server
}

func (f *workspaceWireFixture) set(room, typ, key string, value any) {
	raw, _ := json.Marshal(value)
	f.wire[room+"|"+typ+"|"+key] = raw
}
func newWorkspaceWireFixture(t *testing.T) *workspaceWireFixture {
	t.Helper()
	f := &workspaceWireFixture{path: filepath.Join(t.TempDir(), "workspace.db"), wire: map[string]json.RawMessage{}}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls++
		rest := strings.TrimPrefix(r.URL.Path, "/_matrix/client/v3/rooms/")
		if r.Method == http.MethodGet && strings.HasSuffix(rest, "/state") {
			room := strings.TrimSuffix(rest, "/state")
			events := []map[string]any{}
			for k, raw := range f.wire {
				p := strings.SplitN(k, "|", 3)
				if p[0] == room {
					events = append(events, map[string]any{"type": p[1], "state_key": p[2], "content": raw})
				}
			}
			json.NewEncoder(w).Encode(events)
			return
		}
		p := strings.SplitN(rest, "/state/", 2)
		if len(p) != 2 {
			t.Errorf("unexpected Matrix route%s", r.URL.Path)
			http.Error(w, "unexpected", 400)
			return
		}
		state := strings.SplitN(p[1], "/", 2)
		if len(state) != 2 {
			t.Errorf("missing state key%s", r.URL.Path)
			http.Error(w, "key", 400)
			return
		}
		key := p[0] + "|" + state[0] + "|" + state[1]
		if r.Method == http.MethodPut {
			var raw json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				t.Error(err)
			}
			f.wire[key] = raw
			json.NewEncoder(w).Encode(map[string]string{"event_id": "$state"})
			return
		}
		if raw, ok := f.wire[key]; ok {
			w.Write(raw)
		} else {
			w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(f.server.Close)
	f.restart(t)
	f.set("!child:test", "m.space.parent", "!parent1:test", map[string]any{"via": []string{"test"}, "canonical": true})
	f.set("!parent1:test", "m.space.child", "!child:test", map[string]any{"via": []string{"test"}})
	for _, typ := range []string{"m.bridge", "uk.half-shot.bridge"} {
		f.set("!child:test", typ, "fixture", map[string]any{"channel": map[string]any{"id": "7", "fi.mau.receiver": "login"}, "com.beeper.room_type.v2": "", "network": map[string]any{"id": "space:1"}})
	}
	f.calls = 0
	return f
}
func (f *workspaceWireFixture) restart(t *testing.T) {
	t.Helper()
	f.ic = fixtureHistoryClient(t, f.path)
	f.ic.AccountID = "1"
	br := f.ic.UserLogin.Bridge
	f.ic.UserLogin.User = &bridgev2.User{User: &database.User{MXID: "@user:test"}, Bridge: br}
	as, err := appservice.CreateFull(appservice.CreateOpts{HomeserverDomain: "test", HomeserverURL: f.server.URL, Registration: &appservice.Registration{SenderLocalpart: "bot", AppToken: "synthetic"}})
	if err != nil {
		t.Fatal(err)
	}
	as.BotIntent().Client.DefaultHTTPRetries = 0
	br.Bot = &matrix.ASIntent{Matrix: as.BotIntent()}
	for n := 1; n <= 2; n++ {
		key := networkid.PortalKey{ID: networkid.PortalID(fmt.Sprintf("space:%d", n)), Receiver: "login"}
		row, err := br.DB.Portal.GetByKey(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		if row == nil {
			if err = br.DB.Portal.Insert(t.Context(), &database.Portal{PortalKey: key, RoomType: database.RoomTypeSpace, MXID: networkidRoom(n), Metadata: &PortalMetadata{}}); err != nil {
				t.Fatal(err)
			}
		}
		if err = as.StateStore.SetMembership(t.Context(), networkidRoom(n), br.Bot.GetMXID(), event.MembershipJoin); err != nil {
			t.Fatal(err)
		}
	}
	key := f.ic.portalKeyForChat(7)
	row, err := br.DB.Portal.GetByKey(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		if err = br.DB.Portal.Insert(t.Context(), &database.Portal{PortalKey: key, ParentKey: networkid.PortalKey{ID: "space:1", Receiver: "login"}, InSpace: true, MXID: "!child:test", Metadata: &PortalMetadata{}}); err != nil {
			t.Fatal(err)
		}
	}
	f.portal, err = br.GetPortalByKey(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err = as.StateStore.SetMembership(t.Context(), f.portal.MXID, br.Bot.GetMXID(), event.MembershipJoin); err != nil {
		t.Fatal(err)
	}
}
func networkidRoom(n int) id.RoomID { return id.RoomID(fmt.Sprintf("!parent%d:test", n)) }
func workspaceDialog(space int64) sidecar.DialogRecord {
	return sidecar.DialogRecord{ChatID: 7, Structure: &sidecar.DialogStructure{Kind: "channel", SpaceID: &space}}
}
func (f *workspaceWireFixture) pending(t *testing.T, target string) {
	t.Helper()
	s, k, err := f.ic.projectionJournal(t.Context(), "7")
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.load(t.Context(), k)
	if err != nil {
		t.Fatal(err)
	}
	p.ParentKnown = true
	p.ParentPending = true
	p.AppliedParent = "space:1"
	p.DesiredParent = target
	p.OwnedParents = []id.RoomID{"!parent1:test"}
	if err = s.save(t.Context(), k, &p); err != nil {
		t.Fatal(err)
	}
}
func TestWorkspaceRefreshFailureHoldsOwnedHierarchy(t *testing.T) {
	f := newWorkspaceWireFixture(t)
	f.pending(t, "space:1")
	snapshot := &sidecar.AccountStateSnapshot{Spaces: []sidecar.SpaceRecord{{SpaceID: 1, Name: "one"}}, SpaceAccess: []sidecar.SpaceAccessRecord{{SpaceID: 1, Status: "accessible"}}, SpaceAccessComplete: true}
	f.ic.cacheWorkspaceSnapshot(snapshot)
	revision := f.ic.workspaceRevision
	f.ic.workspaceProjectedRevision = revision
	before, _ := json.Marshal(f.wire)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/compatibility":
			json.NewEncoder(w).Encode(sidecar.ProtocolInfo{ProtocolVersion: sidecar.ProtocolVersion})
		case "/rpc/state/account":
			http.Error(w, "temporary failure", 503)
		case "/rpc/state/dialogs":
			writeConnectorSidecarResult(t, w, "dialogs", sidecar.DialogsPage{})
		default:
			t.Errorf("unexpected source%s", r.URL.Path)
			http.Error(w, "unexpected", 400)
		}
	}))
	defer server.Close()
	f.ic.Sidecar = sidecar.NewClient(server.URL).WithSessionNamespace("fixture")
	if err := f.ic.syncDialogs(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	f.ic.projectDialogInLane(t.Context(), f.portal, workspaceDialog(1))
	after, _ := json.Marshal(f.wire)
	if f.ic.workspaceRevision != revision || f.ic.spaceAccess[1] != "accessible" || !f.ic.spaceAccessComplete || !reflect.DeepEqual(before, after) {
		t.Fatalf("temporary RPC failure changed snapshot/navigation: revision%d=>%d access%v", revision, f.ic.workspaceRevision, f.ic.spaceAccess)
	}
	row, err := f.ic.UserLogin.Bridge.DB.Portal.GetByKey(t.Context(), f.portal.PortalKey)
	if err != nil || row.ParentKey.ID != "space:1" {
		t.Fatalf("owned parent lost: %+v %v", row, err)
	}
}
func TestWorkspaceUnknownHoldsPendingAttachAndDetachAcrossRestart(t *testing.T) {
	for _, target := range []string{"", "space:2"} {
		t.Run(target, func(t *testing.T) {
			f := newWorkspaceWireFixture(t)
			f.pending(t, target)
			before, _ := json.Marshal(f.wire)
			f.restart(t)
			f.calls = 0
			f.ic.projectDialogInLane(t.Context(), f.portal, workspaceDialog(2))
			after, _ := json.Marshal(f.wire)
			s, k, err := f.ic.projectionJournal(t.Context(), "7")
			if err != nil {
				t.Fatal(err)
			}
			p, err := s.load(t.Context(), k)
			if err != nil || f.calls != 0 || !reflect.DeepEqual(before, after) || !p.ParentPending || p.DesiredParent != target {
				t.Fatalf("unknown access replayed pendingtarget%q: calls%d state%+v err%v", target, f.calls, p, err)
			}
			// Fresh positive access or explicit removal releases the persisted operation.
			status := "accessible"
			space := int64(2)
			want := "space:2"
			if target == "" {
				status = "removed"
				want = ""
			}
			f.ic.cacheWorkspaceSnapshot(&sidecar.AccountStateSnapshot{Spaces: []sidecar.SpaceRecord{{SpaceID: space, Name: "two"}}, SpaceAccess: []sidecar.SpaceAccessRecord{{SpaceID: space, Status: status}}})
			f.ic.projectDialogInLane(t.Context(), f.portal, workspaceDialog(space))
			p, err = s.load(t.Context(), k)
			if err != nil || p.ParentPending || string(f.portal.ParentKey.ID) != want {
				t.Fatalf("positive recovery failed status%s: %+v parent%+v %v", status, p, f.portal.ParentKey, err)
			}
		})
	}
}
func TestWorkspaceAccessThreeStateContractProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		space := hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64))
		status := hegel.Draw(ht, hegel.SampledFrom([]string{"accessible", "unknown", "removed", ""}))
		complete := hegel.Draw(ht, hegel.Booleans())
		hasRecord := hegel.Draw(ht, hegel.Booleans())
		ic := &InlineClient{spaceAccess: map[int64]string{space: status}, spaceAccessComplete: complete, spaces: map[int64]sidecar.SpaceRecord{}}
		if hasRecord {
			ic.spaces[space] = sidecar.SpaceRecord{SpaceID: space}
		}
		if status == "" {
			delete(ic.spaceAccess, space)
		}
		parent, known := ic.desiredWorkspace(workspaceDialog(space))
		expectedKnown := status == "removed" || (status == "" && complete) || (status == "accessible" && hasRecord)
		expectedParent := ""
		if status == "accessible" && hasRecord {
			expectedParent = fmt.Sprintf("space:%d", space)
		}
		if known != expectedKnown || parent != expectedParent {
			ht.Fatalf("access%s complete%t metadata%t => %q/%t expected%q/%t", status, complete, hasRecord, parent, known, expectedParent, expectedKnown)
		}
	}, hegel.WithTestCases(3000), hegel.WithDatabase(t.TempDir()))
}

func TestWorkspaceExplicitInvalidAccessIsUnknownDespiteCompleteList(t *testing.T) {
	ic := &InlineClient{spaceAccess: map[int64]string{1: ""}, spaceAccessComplete: true}
	if parent, known := ic.desiredWorkspace(workspaceDialog(1)); known || parent != "" {
		t.Fatal("invalid explicit access became proof of removal")
	}
	d := workspaceDialog(1)
	d.Structure.SpaceID = nil
	if parent, known := ic.desiredWorkspace(d); !known || parent != "" {
		t.Fatal("known structural detach was blocked by unknown access")
	}
}
