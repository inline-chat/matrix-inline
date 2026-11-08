package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maps"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestMigrationRemoteSnapshotDetectsLaterParentChoice(t *testing.T) {
	parent := "!applied:test"
	kind := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/!child:test/state") {
			events := []map[string]any{}
			for _, typ := range []string{"m.bridge", "uk.half-shot.bridge"} {
				events = append(events, map[string]any{"type": typ, "state_key": "fixture", "content": map[string]any{"channel": map[string]any{"id": "7", "fi.mau.receiver": "1"}, "com.beeper.room_type.v2": kind, "network": map[string]any{"id": "space:2"}}})
			}
			events = append(events, map[string]any{"type": "m.space.parent", "state_key": parent, "content": map[string]any{"via": []string{"test"}, "canonical": true}})
			json.NewEncoder(w).Encode(events)
		} else if strings.Contains(r.URL.Path, "/state/m.space.child/") {
			json.NewEncoder(w).Encode(map[string]any{"via": []string{"test"}})
		} else {
			t.Errorf("unexpected read %s", r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer server.Close()
	cli, err := mautrix.NewClient(server.URL, "@bot:test", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "7", Receiver: "1"}, MXID: "!child:test", RoomType: database.RoomTypeDefault}}
	applied, err := migrationWireSnapshot(t.Context(), cli, portal, []id.RoomID{"!applied:test"})
	if err != nil {
		t.Fatal(err)
	}
	if applied["bridge:m.bridge:parent"] != "space:2" || applied["parent:!applied:test"] != `{"canonical":true,"via":["test"]}` {
		t.Fatalf("wire snapshot dropped presentation: %+v", applied)
	}
	parent = "!user-choice:test"
	changed, err := migrationWireSnapshot(t.Context(), cli, portal, []id.RoomID{"!applied:test"})
	if err != nil {
		t.Fatal(err)
	}
	if maps.Equal(applied, changed) || migrationWireCompatible(changed, applied, applied) {
		t.Fatal("user remote parent change hidden by unchanged portal cache")
	}
}

func TestMigrationLinkRestoreRefusesUnjournaledUserChoice(t *testing.T) {
	f := &fixtureHierarchy{state: map[string][]byte{}}
	child, parent := id.RoomID("!child:test"), id.RoomID("!parent:test")
	before := map[string]string{"parent:" + string(parent): `{"canonical":true,"via":["before","extra"]}`, "child:" + string(parent): `{"suggested":true,"via":["before","extra"]}`}
	if err := f.put(t.Context(), child, event.StateSpaceParent, string(parent), map[string]any{"via": []string{"user"}, "canonical": true}); err != nil {
		t.Fatal(err)
	}
	writes := f.writes
	if err := restoreMigrationLinks(t.Context(), f, child, []id.RoomID{parent}, before); err == nil {
		t.Fatal("overwrote unjournaled user parent")
	}
	if f.writes != writes {
		t.Fatal("conflicting link was overwritten")
	}
}

func TestMigrationGateRunsBeforeFrameworkInfoEffects(t *testing.T) {
	for _, third := range []bool{true, false} {
		t.Run(fmt.Sprint(third), func(t *testing.T) {
			ic := fixtureHistoryClient(t, filepath.Join(t.TempDir(), "guard.db"))
			br := ic.UserLogin.Bridge
			ic.UserLogin.User = &bridgev2.User{User: &database.User{MXID: "@user:test"}, Bridge: br}
			portal, err := br.GetPortalByKey(t.Context(), ic.portalKeyForChat(7))
			if err != nil {
				t.Fatal(err)
			}
			portal.MXID = "!child:test"
			portal.RoomType = database.RoomTypeGroupDM
			if err = portal.Save(t.Context()); err != nil {
				t.Fatal(err)
			}
			wire := map[string]map[string]any{}
			for _, typ := range []string{"m.bridge", "uk.half-shot.bridge"} {
				wire[typ] = map[string]any{"channel": map[string]any{"id": "7", "fi.mau.receiver": "login"}, "com.beeper.room_type.v2": "group_dm", "com.beeper.room_type": "dm"}
			}
			writes := 0
			hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/state") {
					events := []map[string]any{}
					for typ, content := range wire {
						events = append(events, map[string]any{"type": typ, "state_key": "fixture", "content": content})
					}
					json.NewEncoder(w).Encode(events)
					return
				}
				if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/state/") {
					writes++
					for _, typ := range []string{"m.bridge", "uk.half-shot.bridge"} {
						if strings.Contains(r.URL.Path, "/state/"+typ+"/") {
							var content map[string]any
							json.NewDecoder(r.Body).Decode(&content)
							wire[typ] = content
						}
					}
					w.Write([]byte(`{"event_id":"$state"}`))
					return
				}
				t.Errorf("unexpected Matrix route %s %s", r.Method, r.URL.Path)
				w.WriteHeader(400)
			}))
			defer hs.Close()
			as, err := appservice.CreateFull(appservice.CreateOpts{HomeserverDomain: "test", HomeserverURL: hs.URL, Registration: &appservice.Registration{SenderLocalpart: "bot", AppToken: "synthetic"}})
			if err != nil {
				t.Fatal(err)
			}
			raw := &matrix.ASIntent{Matrix: as.BotIntent()}
			br.Bot = raw
			if err = as.StateStore.SetMembership(t.Context(), portal.MXID, raw.GetMXID(), event.MembershipJoin); err != nil {
				t.Fatal(err)
			}
			beforeWire, err := migrationWireSnapshot(t.Context(), raw.Matrix.Client, portal, nil)
			if err != nil {
				t.Fatal(err)
			}
			journal, key, err := ic.projectionJournal(t.Context(), "7")
			if err != nil {
				t.Fatal(err)
			}
			state, _ := journal.load(t.Context(), key)
			state.Migration = &presentationMigration{Before: presentationOf(portal), Applied: presentationValue{RoomID: portal.MXID, Kind: database.RoomTypeDefault}, Phase: "pending", BeforeWire: beforeWire}
			state.Migration.AppliedWire = migrationBridgeValues(beforeWire, state.Migration.Applied)
			if err = journal.save(t.Context(), key, &state); err != nil {
				t.Fatal(err)
			}
			if third {
				for _, content := range wire {
					content["com.beeper.room_type.v2"] = "external"
				}
			}
			source := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeConnectorSidecarResult(t, w, "chat_participants", sidecar.ChatParticipantsPage{Participants: []sidecar.ChatParticipantRecord{{UserID: 1}}})
			}))
			defer source.Close()
			ic.Sidecar = sidecar.NewClient(source.URL)
			ic.rememberDialog(sidecar.DialogRecord{ChatID: 7, Structure: &sidecar.DialogStructure{Kind: "channel"}})
			postCalled, backfillChecked := false, false
			br.Config.Backfill.Enabled = true
			result := portal.Internal().HandleRemoteEvent(t.Context(), nil, bridgev2.RemoteEventChatResync, &simplevent.ChatResync{
				EventMeta:              simplevent.EventMeta{PostHandleFunc: func(context.Context, *bridgev2.Portal) { postCalled = true }},
				GetChatInfoFunc:        ic.GetChatInfo,
				CheckNeedsBackfillFunc: func(context.Context, *database.Message) (bool, error) { backfillChecked = true; return false, nil },
			})
			if !postCalled || !backfillChecked {
				t.Fatal("presentation hold blocked history check or post handler")
			}
			if !result.Success {
				t.Fatal(result.Error)
			}
			state, err = journal.load(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}
			if third {
				if writes != 0 || state.Migration.Phase != "conflict" || portal.RoomType != database.RoomTypeGroupDM {
					t.Fatalf("conflicting metadata escaped pre-UpdateInfo gate: writes%d phase%s kind%s", writes, state.Migration.Phase, portal.RoomType)
				}
				for _, content := range wire {
					if content["com.beeper.room_type.v2"] != "external" {
						t.Fatal("framework overwrote third-party bridge metadata")
					}
				}
			} else if state.Migration.Phase != "applied" || portal.RoomType != database.RoomTypeDefault {
				t.Fatalf("pending migration did not resume before UpdateInfo: %+v kind%s", state.Migration, portal.RoomType)
			}
		})
	}
}
