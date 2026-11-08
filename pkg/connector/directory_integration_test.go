package connector

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func TestDirectoryIntegratedLookupRejectsUnrelatedCachedUsers(t *testing.T) {
	f := &directoryFixture{state: sidecar.AccountStateSnapshot{Users: []sidecar.UserRecord{dirUser(42, "ada")}}, dialogs: sidecar.DialogsPage{Dialogs: []sidecar.DialogRecord{{ChatID: 7, Structure: &sidecar.DialogStructure{Kind: "direct", PeerUserID: dirPtr(int64(42))}}}}}
	s := directoryServer(t, map[string]*directoryFixture{"a": f})
	ic := directoryClient(s.URL, "a")
	if _, err := ic.ResolveIdentifier(t.Context(), "999", false); !errors.Is(err, errDirectoryNotFound) {
		t.Fatalf("arbitrary cached user resolved: %v", err)
	}
	if _, err := ic.CreateChatWithGhost(t.Context(), &bridgev2.Ghost{Ghost: &database.Ghost{ID: "999"}}); !errors.Is(err, errDirectoryNotFound) {
		t.Fatalf("undiscovered ghost created chat: %v", err)
	}
	got, err := ic.ResolveIdentifier(t.Context(), "@ADA", false)
	if err != nil || got == nil || got.UserID != "42" || got.UserInfo == nil || got.UserInfo.Name == nil || *got.UserInfo.Name != "ada" {
		t.Fatalf("accessible canonical username lost: %+v %v", got, err)
	}
}

func TestDirectoryIntegratedCreatePreflightAndCurrentPermission(t *testing.T) {
	mutations := 0
	var last sidecar.CreateThreadRequest
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/compatibility" {
			json.NewEncoder(w).Encode(sidecar.ProtocolInfo{ProtocolVersion: sidecar.ProtocolVersion})
			return
		}
		if r.Header.Get("X-Inline-Session-Namespace") != "a" {
			t.Error("lost account namespace")
		}
		switch r.URL.Path {
		case "/rpc/dialogs":
			writeConnectorSidecarResult(t, w, "dialogs", sidecar.DialogsPage{})
		case "/rpc/state/account":
			writeConnectorSidecarResult(t, w, "account_state", sidecar.AccountStateSnapshot{Spaces: []sidecar.SpaceRecord{{SpaceID: 10}}, SpaceAccess: []sidecar.SpaceAccessRecord{{SpaceID: 10, Status: "accessible"}}})
		case "/rpc/chat/create-thread":
			mutations++
			if err := json.NewDecoder(r.Body).Decode(&last); err != nil {
				t.Error(err)
			}
			json.NewEncoder(w).Encode(map[string]any{"protocol_version": sidecar.ProtocolVersion, "outcome": map[string]any{"status": "error", "data": sidecar.Error{Category: "permission_denied", Message: "current source policy denies creation"}}})
		default:
			t.Errorf("unexpected RPC %s", r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer s.Close()
	ic := directoryClient(s.URL, "a")
	if _, err := ic.CreateGroup(t.Context(), &bridgev2.GroupCreateParams{Type: "group_dm"}); err == nil || mutations != 0 {
		t.Fatalf("unsupported type caused source mutation: %v mutations%d", err, mutations)
	}
	_, err := ic.CreateGroup(t.Context(), &bridgev2.GroupCreateParams{Type: "public_thread", Parent: &networkid.PortalKey{ID: "space:10", Receiver: ic.UserLogin.ID}})
	var source *sidecar.Error
	if !errors.As(err, &source) || source.Category != "permission_denied" || mutations != 1 || !last.IsPublic || last.SpaceID == nil || *last.SpaceID != 10 || len(last.Participants) != 0 {
		t.Fatalf("public workspace request/current denial lost: %+v %v mutations%d", last, err, mutations)
	}
}

func TestDirectoryIntegratedPrivateChannelIsNeverInferredDM(t *testing.T) {
	s := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rpc/dialogs":
			writeConnectorSidecarResult(t, w, "dialogs", sidecar.DialogsPage{Dialogs: []sidecar.DialogRecord{{ChatID: 7, Structure: &sidecar.DialogStructure{Kind: "direct", PeerUserID: dirPtr(int64(42))}}}, Users: []sidecar.UserRecord{dirUser(42, "ada")}})
		case "/rpc/chat/create-thread":
			writeConnectorSidecarResult(t, w, "created_chat", sidecar.CreatedChat{ChatID: 99})
		default:
			t.Errorf("unexpected RPC %s", r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer s.Close()
	ic := directoryClient(s.URL, "a")
	got, err := ic.CreateGroup(t.Context(), &bridgev2.GroupCreateParams{Type: "private_thread", Participants: []networkid.UserID{"42"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.PortalKey.ID != "99" || got.PortalKey.Receiver != ic.UserLogin.ID || got.PortalInfo.Type == nil || *got.PortalInfo.Type != database.RoomTypeDefault || got.PortalInfo.Members == nil || got.PortalInfo.Members.OtherUserID != "" || got.PortalInfo.Members.IsFull {
		t.Fatalf("ordinary private channel misclassified or claimed complete membership: %+v", got.PortalInfo)
	}
}
