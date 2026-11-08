package connector

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"hegel.dev/go/hegel"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
)

func dirPtr[T any](v T) *T { return &v }
func dirUser(id int64, name string) sidecar.UserRecord {
	return sidecar.UserRecord{UserID: id, Username: dirPtr(name), DisplayName: dirPtr(name)}
}
func dirSnapshot(users ...sidecar.UserRecord) *directorySnapshot {
	d := &directorySnapshot{users: map[int64]directoryUser{}, spaces: map[int64]sidecar.SpaceRecord{}, access: map[int64]string{}, members: map[int64]map[int64]bool{}, receiver: "login-a", namespace: "a"}
	for _, u := range users {
		d.users[u.UserID] = directoryUser{Record: u}
	}
	return d
}

func TestDirectoryExactPositiveIDProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		id := hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64))
		d := dirSnapshot(dirUser(id, "alice"))
		got, err := d.resolve(strconv.FormatInt(id, 10))
		if err != nil || got.Record.UserID != id {
			ht.Fatalf("exact accessible id %d: %+v %v", id, got, err)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}
func TestDirectoryArbitraryNumericNeverSynthesizesProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		id := hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, math.MaxInt64))
		got, err := dirSnapshot().resolve(strconv.FormatInt(id, 10))
		if !errors.Is(err, errDirectoryNotFound) || got.Record.UserID != 0 {
			ht.Fatalf("invented user %d: %+v %v", id, got, err)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestDirectoryExactIDMissDistinguishesIncompleteProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		id := hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64))
		present := hegel.Draw(ht, hegel.Booleans())
		incomplete := hegel.Draw(ht, hegel.Booleans())
		d := dirSnapshot()
		d.incomplete = incomplete
		if present {
			d.users[id] = directoryUser{Record: dirUser(id, "known")}
		}
		got, err := d.resolve(strconv.FormatInt(id, 10))
		if present {
			if err != nil || got.Record.UserID != id {
				ht.Fatalf("known exact ID lost under incomplete=%v: %+v %v", incomplete, got, err)
			}
		} else {
			want := errDirectoryNotFound
			if incomplete {
				want = errDirectoryIncomplete
			}
			if !errors.Is(err, want) || got.Record.UserID != 0 {
				ht.Fatalf("ID %d miss incomplete=%v: %+v %v, want %v", id, incomplete, got, err, want)
			}
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}
func TestDirectoryUsernameAndSearch(t *testing.T) {
	d := dirSnapshot(dirUser(9, "Alice"), dirUser(2, "alice"), dirUser(20, "bob"))
	for _, q := range []string{"alice", "@ALICE"} {
		if _, err := d.resolve(q); !errors.Is(err, errDirectoryAmbiguous) {
			t.Fatalf("ambiguity %q: %v", q, err)
		}
	}
	if u, err := d.resolve("@BoB"); err != nil || u.Record.UserID != 20 {
		t.Fatalf("username: %+v %v", u, err)
	}
	for _, q := range []string{"0", "-1", "2x", "+2", "alice@example.org", "+15555555"} {
		if _, err := d.resolve(q); err == nil {
			t.Fatalf("accepted %q", q)
		}
	}
	got, err := d.search("", 0, 2)
	if err != nil || len(got) != 2 || got[0].Record.UserID != 2 || got[1].Record.UserID != 9 {
		t.Fatalf("ordered bounded search: %+v %v", got, err)
	}
	got, err = d.search("2", 0, 100)
	if err != nil || len(got) != 1 || got[0].Record.UserID != 2 {
		t.Fatalf("numeric search must be exact: %+v %v", got, err)
	}
	d.incomplete = true
	if _, err = d.resolve("bob"); !errors.Is(err, errDirectoryIncomplete) {
		t.Fatalf("truncated username claimed unique: %v", err)
	}
}

type directoryFixture struct {
	state        sidecar.AccountStateSnapshot
	dialogs      sidecar.DialogsPage
	participants map[int64]sidecar.ChatParticipantsPage
	deny         string
	calls        []string
	chatReads    []int64
	mu           sync.Mutex
}

func directoryServer(t *testing.T, accounts map[string]*directoryFixture) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/compatibility" {
			json.NewEncoder(w).Encode(sidecar.ProtocolInfo{ProtocolVersion: sidecar.ProtocolVersion})
			return
		}
		ns := r.Header.Get("X-Inline-Session-Namespace")
		f := accounts[ns]
		if f == nil {
			t.Errorf("unscoped namespace %q", ns)
			http.Error(w, "bad namespace", 400)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls = append(f.calls, r.URL.Path)
		if r.Header.Get("X-Inline-Protocol-Version") != "5" {
			t.Error("missing protocol header")
		}
		if r.URL.Path == f.deny {
			json.NewEncoder(w).Encode(map[string]any{"protocol_version": sidecar.ProtocolVersion, "outcome": map[string]any{"status": "error", "data": sidecar.Error{Category: "permission_denied", Message: "permission denied"}}})
			return
		}
		var typ string
		var data any
		switch r.URL.Path {
		case "/rpc/dialogs":
			typ = "dialogs"
			data = f.dialogs
		case "/rpc/state/account":
			typ = "account_state"
			data = f.state
		case "/rpc/chat/participants":
			var req sidecar.ChatParticipantsRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			f.chatReads = append(f.chatReads, req.ChatID)
			typ = "chat_participants"
			data = f.participants[req.ChatID]
		default:
			t.Errorf("unexpected directory RPC %s", r.URL.Path)
			http.Error(w, "unexpected", 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"protocol_version": sidecar.ProtocolVersion, "outcome": map[string]any{"status": "ok", "data": map[string]any{"type": typ, "data": data}}})
	}))
	t.Cleanup(s.Close)
	return s
}
func directoryClient(url, ns string) *InlineClient {
	return &InlineClient{Sidecar: sidecar.NewClient(url).WithSessionNamespace(ns), UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID("login-" + ns)}}, AccountID: "1", users: map[int64]sidecar.UserRecord{999: dirUser(999, "unrelated")}}
}
func TestDirectoryAccountAccessAndFreshness(t *testing.T) {
	f := &directoryFixture{state: sidecar.AccountStateSnapshot{
		Users:        []sidecar.UserRecord{dirUser(2, "peer"), dirUser(3, "workspace"), dirUser(4, "removed"), dirUser(5, "unknown"), dirUser(6, "hidden"), dirUser(7, "deleted"), dirUser(8, "live"), dirUser(999, "old")},
		SpaceAccess:  []sidecar.SpaceAccessRecord{{SpaceID: 10, Status: "accessible"}, {SpaceID: 11, Status: "removed"}, {SpaceID: 12, Status: "unknown"}},
		SpaceMembers: []sidecar.SpaceMemberRecord{{SpaceID: 10, UserID: 3}, {SpaceID: 11, UserID: 4}, {SpaceID: 12, UserID: 5}}, DeletedChatIDs: []int64{70}},
		dialogs: sidecar.DialogsPage{Dialogs: []sidecar.DialogRecord{
			{ChatID: 20, Structure: &sidecar.DialogStructure{Kind: "direct", PeerUserID: dirPtr(int64(2))}},
			{ChatID: 60, ChatListHidden: dirPtr(true), Structure: &sidecar.DialogStructure{Kind: "direct", PeerUserID: dirPtr(int64(6))}},
			{ChatID: 70, Structure: &sidecar.DialogStructure{Kind: "direct", PeerUserID: dirPtr(int64(7))}},
			{ChatID: 80, Structure: &sidecar.DialogStructure{Kind: "channel", SpaceID: dirPtr(int64(11))}},
		}}, participants: map[int64]sidecar.ChatParticipantsPage{80: {Participants: []sidecar.ChatParticipantRecord{{UserID: 8}}, Users: []sidecar.UserRecord{dirUser(8, "live"), dirUser(999, "unrelated")}}}}
	s := directoryServer(t, map[string]*directoryFixture{"a": f, "b": {state: sidecar.AccountStateSnapshot{Users: []sidecar.UserRecord{dirUser(2, "other-account")}}, dialogs: sidecar.DialogsPage{Dialogs: []sidecar.DialogRecord{{ChatID: 20, Structure: &sidecar.DialogStructure{Kind: "direct", PeerUserID: dirPtr(int64(2))}}}}}})
	a := directoryClient(s.URL, "a")
	d, err := a.loadDirectory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.search("", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	ids := []int64{}
	for _, u := range got {
		ids = append(ids, u.Record.UserID)
	}
	if !reflect.DeepEqual(ids, []int64{2, 3, 8}) {
		t.Fatalf("access leak/missing users: %v", ids)
	}
	if d.users[2].Fresh || d.users[3].Fresh || !d.users[8].Fresh {
		t.Fatalf("wrong source freshness: %+v", d.users)
	}
	b, err := directoryClient(s.URL, "b").loadDirectory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if u, err := b.resolve("2"); err != nil || *u.Record.Username != "other-account" {
		t.Fatalf("namespace collision: %+v %v", u, err)
	}
	if _, err = b.resolve("peer"); !errors.Is(err, errDirectoryNotFound) {
		t.Fatalf("cross-account username leaked: %v", err)
	}
	if !reflect.DeepEqual(f.chatReads, []int64{80}) {
		t.Fatalf("hidden/deleted/unknown dialog read: %v", f.chatReads)
	}
}

func TestDirectoryExcludedConversationDiagnosticsAndIndependentEvidence(t *testing.T) {
	for _, excluded := range []string{"hidden", "deleted"} {
		for _, evidence := range []string{"none", "workspace", "live"} {
			t.Run(excluded+"/"+evidence, func(t *testing.T) {
				f := &directoryFixture{
					state:   sidecar.AccountStateSnapshot{Users: []sidecar.UserRecord{dirUser(42, "peer")}},
					dialogs: sidecar.DialogsPage{Dialogs: []sidecar.DialogRecord{{ChatID: 7, Structure: &sidecar.DialogStructure{Kind: "direct", PeerUserID: dirPtr(int64(42))}}}},
				}
				if excluded == "hidden" {
					f.dialogs.Dialogs[0].ChatListHidden = dirPtr(true)
				} else {
					f.state.DeletedChatIDs = []int64{7}
				}
				switch evidence {
				case "workspace":
					f.state.SpaceAccess = []sidecar.SpaceAccessRecord{{SpaceID: 10, Status: "accessible"}}
					f.state.SpaceMembers = []sidecar.SpaceMemberRecord{{SpaceID: 10, UserID: 42}}
				case "live":
					f.dialogs.Dialogs = append(f.dialogs.Dialogs, sidecar.DialogRecord{ChatID: 8, Structure: &sidecar.DialogStructure{Kind: "channel"}})
					f.participants = map[int64]sidecar.ChatParticipantsPage{8: {Participants: []sidecar.ChatParticipantRecord{{UserID: 42}}, Users: []sidecar.UserRecord{dirUser(42, "peer")}}}
				}
				s := directoryServer(t, map[string]*directoryFixture{"a": f})
				ic := directoryClient(s.URL, "a")
				for _, query := range []string{"42", "@PEER"} {
					got, err := ic.ResolveIdentifier(t.Context(), query, false)
					if evidence == "none" {
						if !errors.Is(err, errDirectoryNotFound) || got != nil || !strings.Contains(err.Error(), "eligible directory") || strings.Contains(err.Error(), "not found") {
							t.Fatalf("excluded conversation misreported for %s: %+v %v", query, got, err)
						}
					} else if err != nil || got == nil || got.UserID != "42" {
						t.Fatalf("independent %s evidence lost: %+v %v", evidence, got, err)
					}
				}
				if evidence == "none" {
					if _, err := ic.CreateChatWithGhost(t.Context(), &bridgev2.Ghost{Ghost: &database.Ghost{ID: "42"}}); !errors.Is(err, errDirectoryNotFound) {
						t.Fatalf("ineligible peer caused creation: %v", err)
					}
				}
			})
		}
	}
}
func TestDirectoryRefreshDenial(t *testing.T) {
	for _, path := range []string{"/rpc/dialogs", "/rpc/state/account", "/rpc/chat/participants"} {
		t.Run(path, func(t *testing.T) {
			f := &directoryFixture{deny: path, dialogs: sidecar.DialogsPage{Dialogs: []sidecar.DialogRecord{{ChatID: 8, Structure: &sidecar.DialogStructure{Kind: "channel"}}}}}
			s := directoryServer(t, map[string]*directoryFixture{"a": f})
			d, err := directoryClient(s.URL, "a").loadDirectory(t.Context())
			if err == nil || d != nil || !strings.Contains(err.Error(), "permission denied") {
				t.Fatalf("denial lost/fallback: %+v %v", d, err)
			}
		})
	}
}
func TestDirectoryCreationPolicy(t *testing.T) {
	d := dirSnapshot(dirUser(2, "peer"))
	d.spaces[10] = sidecar.SpaceRecord{SpaceID: 10, IsPublic: dirPtr(true)}
	d.access[10] = "accessible"
	d.members[10] = map[int64]bool{2: true}
	parent := &networkid.PortalKey{ID: "space:10", Receiver: "login-a"}
	for _, typ := range []string{"", "thread", "channel", "private_thread"} {
		params := &bridgev2.GroupCreateParams{Type: typ, Participants: []networkid.UserID{"2", "2"}, Name: &event.RoomNameEventContent{Name: " hello "}, Topic: &event.TopicEventContent{Topic: "topic"}}
		req, ids, err := validateThreadCreation(params, "login-a", d)
		if err != nil || req.IsPublic || req.SpaceID != nil || len(req.Participants) != 1 || req.Participants[0].UserID != 2 || !reflect.DeepEqual(ids, []networkid.UserID{"2"}) || req.Title == nil || *req.Title != "hello" || req.Description == nil || *req.Description != "topic" {
			t.Fatalf("home request %+v %v %v", req, ids, err)
		}
		params.Parent = parent
		req, _, err = validateThreadCreation(params, "login-a", d)
		if err != nil || req.SpaceID == nil || *req.SpaceID != 10 {
			t.Fatalf("private workspace %+v %v", req, err)
		}
	}
	req, ids, err := validateThreadCreation(&bridgev2.GroupCreateParams{Type: "public_thread", Parent: parent}, "login-a", d)
	if err != nil || !req.IsPublic || *req.SpaceID != 10 || len(ids) != 0 {
		t.Fatalf("public workspace %+v %v %v", req, ids, err)
	}
	bad := []*bridgev2.GroupCreateParams{nil, {}, {Type: "group_dm", Participants: []networkid.UserID{"2"}}, {Type: "direct"}, {Type: "mystery"}, {Type: "public_thread"}, {Type: "public_thread", Parent: parent, Participants: []networkid.UserID{"2"}}, {Participants: []networkid.UserID{"999"}}, {Participants: []networkid.UserID{"2"}, Username: "x"}, {Participants: []networkid.UserID{"2"}, Avatar: &event.RoomAvatarEventContent{}}, {Participants: []networkid.UserID{"2"}, Disappear: &event.BeeperDisappearingTimer{}}, {Parent: &networkid.PortalKey{ID: "space:10", Receiver: "login-b"}, Participants: []networkid.UserID{"2"}}, {Parent: &networkid.PortalKey{ID: "10", Receiver: "login-a"}, Participants: []networkid.UserID{"2"}}}
	for i, p := range bad {
		if _, _, err := validateThreadCreation(p, "login-a", d); err == nil {
			t.Errorf("accepted invalid params %d: %+v", i, p)
		}
	}
	d.access[10] = "removed"
	if _, _, err := validateThreadCreation(&bridgev2.GroupCreateParams{Type: "public_thread", Parent: parent}, "login-a", d); err == nil {
		t.Fatal("removed workspace accepted")
	}
}
func TestDirectoryCreationPositiveIDsProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		id := hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64))
		space := hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64))
		d := dirSnapshot(dirUser(id, "user"))
		d.spaces[space] = sidecar.SpaceRecord{SpaceID: space}
		d.access[space] = "accessible"
		d.members[space] = map[int64]bool{id: true}
		p := &bridgev2.GroupCreateParams{Type: "private_thread", Parent: &networkid.PortalKey{ID: networkid.PortalID("space:" + strconv.FormatInt(space, 10)), Receiver: "login-a"}, Participants: []networkid.UserID{networkid.UserID(strconv.FormatInt(id, 10))}}
		req, ids, err := validateThreadCreation(p, "login-a", d)
		if err != nil || req.SpaceID == nil || *req.SpaceID != space || len(req.Participants) != 1 || req.Participants[0].UserID != id || len(ids) != 1 || string(ids[0]) != strconv.FormatInt(id, 10) {
			ht.Fatalf("lost exact IDs: %+v %v %v", req, ids, err)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestDirectoryInvalidNumericNamesNeverBypassIDValidation(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		id := hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, math.MaxInt64))
		if id > 0 {
			return
		}
		q := strconv.FormatInt(id, 10)
		d := dirSnapshot(dirUser(1, q))
		if _, err := d.resolve(q); !errors.Is(err, errDirectoryNotFound) {
			ht.Fatalf("invalid numeric ID %q resolved through username: %v", q, err)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestDirectoryOverflowIDCannotBecomeUsername(t *testing.T) {
	q := "9223372036854775808"
	d := dirSnapshot(dirUser(1, q))
	if _, err := d.resolve(q); !errors.Is(err, errDirectoryNotFound) {
		t.Fatalf("overflow ID resolved through username: %v", err)
	}
}

func TestDirectorySearchPaginationProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		ids := hegel.Draw(ht, hegel.Lists(hegel.Integers[int64](1, math.MaxInt64)))
		if len(ids) > 200 {
			ids = ids[:200]
		} // Bound materialization, never the ID domain.
		d := dirSnapshot()
		expected := map[int64]bool{}
		for _, id := range ids {
			d.users[id] = directoryUser{Record: dirUser(id, "person")}
			expected[id] = true
		}
		limit := hegel.Draw(ht, hegel.Integers(1, 100))
		var got []int64
		for offset := 0; ; offset += limit {
			page, err := d.search("person", offset, limit)
			if err != nil {
				ht.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			for _, u := range page {
				got = append(got, u.Record.UserID)
			}
		}
		if len(got) != len(expected) {
			ht.Fatalf("pagination lost/duplicated people: %v %v", got, expected)
		}
		for i, id := range got {
			if !expected[id] || i > 0 && got[i-1] >= id {
				ht.Fatalf("unstable/foreign search results: %v", got)
			}
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestDirectorySourceMembershipIsolationProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		id := hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64))
		status := hegel.Draw(ht, hegel.SampledFrom([]string{"accessible", "removed", "unknown", ""}))
		f := &directoryFixture{state: sidecar.AccountStateSnapshot{Users: []sidecar.UserRecord{dirUser(id, "member")}, SpaceAccess: []sidecar.SpaceAccessRecord{{SpaceID: 1, Status: status}}, SpaceMembers: []sidecar.SpaceMemberRecord{{SpaceID: 1, UserID: id}}}}
		// A second account has overlapping IDs but no discovery/access evidence.
		b := &directoryFixture{state: sidecar.AccountStateSnapshot{Users: []sidecar.UserRecord{dirUser(id, "other")}}}
		s := directoryServer(t, map[string]*directoryFixture{"a": f, "b": b})
		a, err := directoryClient(s.URL, "a").loadDirectory(t.Context())
		if err != nil {
			ht.Fatal(err)
		}
		other, err := directoryClient(s.URL, "b").loadDirectory(t.Context())
		if err != nil {
			ht.Fatal(err)
		}
		u, err := a.resolve(strconv.FormatInt(id, 10))
		if status == "accessible" {
			if err != nil || u.Record.UserID != id || u.Fresh {
				ht.Fatalf("accessible cached member lost: %+v %v", u, err)
			}
		} else if !errors.Is(err, errDirectoryNotFound) {
			ht.Fatalf("inaccessible workspace member discovered: %+v %v", u, err)
		}
		if _, err = other.resolve(strconv.FormatInt(id, 10)); !errors.Is(err, errDirectoryNotFound) {
			ht.Fatalf("account scope leak: %v", err)
		}
		s.Close()
	}, hegel.WithTestCases(100), hegel.WithDatabase(t.TempDir()))
}

func TestDirectoryBoundedRefreshAndFrameworkInterfaces(t *testing.T) {
	dialogs := []sidecar.DialogRecord{}
	for i := int64(1); i <= 20; i++ {
		dialogs = append(dialogs, sidecar.DialogRecord{ChatID: i, Structure: &sidecar.DialogStructure{Kind: "channel"}})
	}
	f := &directoryFixture{dialogs: sidecar.DialogsPage{Dialogs: dialogs, NextCursor: "repeat"}}
	s := directoryServer(t, map[string]*directoryFixture{"a": f})
	d, err := directoryClient(s.URL, "a").loadDirectory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !d.incomplete {
		t.Fatal("bounded refresh claimed complete")
	}
	reads, pages := 0, 0
	for _, path := range f.calls {
		if path == "/rpc/dialogs" {
			pages++
		}
		if path == "/rpc/chat/participants" {
			reads++
		}
	}
	if pages > directoryMaxPages || reads != directoryParticipantBudget {
		t.Fatalf("unbounded refresh %d pages %d reads", pages, reads)
	}
	f.dialogs = sidecar.DialogsPage{Dialogs: []sidecar.DialogRecord{{ChatID: 30, Structure: &sidecar.DialogStructure{Kind: "direct", PeerUserID: dirPtr(int64(2))}}}, Users: []sidecar.UserRecord{dirUser(2, "peer")}}
	ic := directoryClient(s.URL, "a")
	got, err := ic.SearchUsers(t.Context(), "peer")
	if err != nil || len(got) != 1 || got[0].UserID != "2" || got[0].UserInfo == nil {
		t.Fatalf("search interface: %+v %v", got, err)
	}
	got, err = ic.GetContactList(t.Context())
	if err != nil || len(got) != 1 || got[0].UserID != "2" {
		t.Fatalf("contact interface: %+v %v", got, err)
	}
}

func TestDirectoryCreationPreflightAndSourceDenial(t *testing.T) {
	f := &directoryFixture{deny: "/rpc/chat/create-thread"}
	s := directoryServer(t, map[string]*directoryFixture{"a": f})
	ic := directoryClient(s.URL, "a")
	d := dirSnapshot(dirUser(2, "peer"))
	attempt := func(p *bridgev2.GroupCreateParams) error {
		req, _, err := validateThreadCreation(p, ic.UserLogin.ID, d)
		if err != nil {
			return err
		}
		_, err = ic.Sidecar.CreateThread(t.Context(), req)
		return err
	}
	for _, p := range []*bridgev2.GroupCreateParams{{Type: "group_dm"}, {Participants: []networkid.UserID{"999"}}, {Participants: []networkid.UserID{"2"}, Avatar: &event.RoomAvatarEventContent{}}, {Type: "public_thread"}, {Participants: []networkid.UserID{"2"}, Parent: &networkid.PortalKey{ID: "space:1", Receiver: "login-b"}}} {
		if err := attempt(p); err == nil {
			t.Fatal("invalid creation succeeded")
		}
	}
	if len(f.calls) != 0 {
		t.Fatalf("bad preflight issued RPC: %v", f.calls)
	}
	err := attempt(&bridgev2.GroupCreateParams{Type: "channel", Participants: []networkid.UserID{"2"}})
	var denied *sidecar.Error
	if !errors.As(err, &denied) || denied.Category != "permission_denied" || denied.Message != "permission denied" {
		t.Fatalf("source authorization denial lost: %v", err)
	}
	if !reflect.DeepEqual(f.calls, []string{"/rpc/chat/create-thread"}) {
		t.Fatalf("unexpected create flow %v", f.calls)
	}
}

func TestDirectoryZeroIDRegression(t *testing.T) {
	if _, err := dirSnapshot(dirUser(1, "0")).resolve("0"); !errors.Is(err, errDirectoryNotFound) {
		t.Fatalf("zero numeric ID became username: %v", err)
	}
}

func TestDirectoryEmptyUnknownAndMissingRecords(t *testing.T) {
	for _, f := range []*directoryFixture{
		{},
		{state: sidecar.AccountStateSnapshot{Users: []sidecar.UserRecord{dirUser(2, "unrelated")}}},
		{dialogs: sidecar.DialogsPage{Dialogs: []sidecar.DialogRecord{{ChatID: 2, Structure: &sidecar.DialogStructure{Kind: "direct", PeerUserID: dirPtr(int64(2))}}}}},
		{state: sidecar.AccountStateSnapshot{Users: []sidecar.UserRecord{dirUser(2, "unknown")}}, dialogs: sidecar.DialogsPage{Dialogs: []sidecar.DialogRecord{{ChatID: 2, PeerUserID: dirPtr(int64(2))}}}},
	} {
		s := directoryServer(t, map[string]*directoryFixture{"a": f})
		d, err := directoryClient(s.URL, "a").loadDirectory(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = d.resolve("2"); !errors.Is(err, errDirectoryNotFound) {
			t.Fatalf("incomplete snapshot synthesized user: %v", err)
		}
		s.Close()
	}
	ic := directoryClient("http://127.0.0.1:1", "")
	if _, err := ic.loadDirectory(t.Context()); err == nil || !strings.Contains(err.Error(), "scoped login") {
		t.Fatalf("unscoped read allowed: %v", err)
	}
}

func TestDirectoryCreationRejectDirectionProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		id := hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, math.MaxInt64))
		status := hegel.Draw(ht, hegel.SampledFrom([]string{"removed", "unknown", ""}))
		d := dirSnapshot(dirUser(id, "cached-admin"))
		d.spaces[10] = sidecar.SpaceRecord{SpaceID: 10, Creator: true}
		d.access[10] = status
		d.members[10] = map[int64]bool{id: true}
		p := &bridgev2.GroupCreateParams{Type: "private_thread", Parent: &networkid.PortalKey{ID: "space:10", Receiver: "login-a"}, Participants: []networkid.UserID{networkid.UserID(strconv.FormatInt(id, 10))}}
		req, ids, err := validateThreadCreation(p, "login-a", d)
		if err == nil || req.SpaceID != nil || len(req.Participants) != 0 || len(ids) != 0 {
			ht.Fatalf("cached creator/inaccessible workspace granted create: %+v %v %v", req, ids, err)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestDirectoryPaginationRejectsInvalidBoundsProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		offset := hegel.Draw(ht, hegel.Integers(math.MinInt, math.MaxInt))
		limit := hegel.Draw(ht, hegel.Integers(math.MinInt, math.MaxInt))
		d := dirSnapshot(dirUser(2, "peer"))
		got, err := d.search("", offset, limit)
		valid := offset >= 0 && limit >= 1 && limit <= 100
		if valid {
			if err != nil {
				ht.Fatal(err)
			}
			if offset == 0 && (len(got) != 1 || got[0].Record.UserID != 2) {
				ht.Fatalf("valid page lost peer: %v", got)
			}
			if offset > 0 && len(got) != 0 {
				ht.Fatalf("offset beyond end returned people: %v", got)
			}
		} else if err == nil {
			ht.Fatalf("invalid bounds accepted: %d %d", offset, limit)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestDirectoryLargeSnapshot(t *testing.T) {
	d := dirSnapshot()
	for id := int64(1); id <= 100_000; id++ {
		d.users[id] = directoryUser{Record: sidecar.UserRecord{UserID: id}}
	}
	got, err := d.search("", 99_900, 100)
	if err != nil || len(got) != 100 || got[0].Record.UserID != 99_901 || got[99].Record.UserID != 100_000 {
		t.Fatalf("large snapshot page: %v %v", len(got), err)
	}
	u, err := d.resolve("100000")
	if err != nil || u.Record.UserID != 100_000 {
		t.Fatalf("large snapshot resolve: %+v %v", u, err)
	}
	req, ids, err := validateThreadCreation(&bridgev2.GroupCreateParams{Participants: []networkid.UserID{"100000"}}, "login-a", d)
	if err != nil || len(req.Participants) != 1 || req.Participants[0].UserID != 100_000 || !reflect.DeepEqual(ids, []networkid.UserID{"100000"}) {
		t.Fatalf("large snapshot creation: %+v %v %v", req, ids, err)
	}
}

var _ bridgev2.UserSearchingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.ContactListingNetworkAPI = (*InlineClient)(nil)

func TestDirectoryNumericSearchNeverFallsBackToNames(t *testing.T) {
	for _, q := range []string{"0", "-1", "9223372036854775808"} {
		d := dirSnapshot(dirUser(1, q))
		got, err := d.search(q, 0, 100)
		if err != nil || len(got) != 0 {
			t.Fatalf("invalid numeric query %q matched names: %+v %v", q, got, err)
		}
	}
}
