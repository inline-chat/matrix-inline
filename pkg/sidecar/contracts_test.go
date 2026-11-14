package sidecar

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestSourceMessageMetadataPreservesRevisionEntitiesAndFutureFields(t *testing.T) {
	input := []byte(`{"chat_id":7,"message_id":11,"sender_id":42,"timestamp":10,"is_outgoing":false,"content":{"type":"text","text":"😀 hello"},"metadata":{"revision":9,"edit_timestamp":20,"is_service":false,"entities":[{"kind":"TYPE_MENTION","offset":3,"length":5,"user_id":42,"agent_id":99}],"attachments":[],"actions":[],"future_native_field":{"nested":true}}}`)
	var message MessageRecord
	if err := json.Unmarshal(input, &message); err != nil {
		t.Fatal(err)
	}
	if message.Metadata.Revision == nil || *message.Metadata.Revision != 9 || len(message.Metadata.Entities) != 1 {
		t.Fatalf("metadata lost: %#v", message.Metadata)
	}
	entity := message.Metadata.Entities[0]
	if entity.AgentID == nil || *entity.AgentID != 99 || entity.Offset != 3 || entity.Length != 5 {
		t.Fatalf("entity lost source identity/ranges: %#v", entity)
	}
	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip MessageRecord
	if err = json.Unmarshal(encoded, &roundtrip); err != nil {
		t.Fatal(err)
	}
	if string(roundtrip.Metadata.Extra["future_native_field"]) != `{"nested":true}` {
		t.Fatalf("future metadata lost: %s", encoded)
	}
}

func TestDirectoryRPCsSendCurrentProtocolAndAccountNamespace(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls++
		if request.Header.Get("X-Inline-Protocol-Version") != strconv.Itoa(ProtocolVersion) || request.Header.Get("X-Inline-Session-Namespace") != "account-a" {
			t.Errorf("missing current protocol/account headers: %v", request.Header)
		}
		result := "users"
		payload := any(UsersPage{})
		switch request.URL.Path {
		case "/rpc/state/account/refresh":
			result = "account_state"
			payload = AccountStateSnapshot{SpacesComplete: true}
		case "/rpc/spaces/members":
			result = "space_members"
			payload = SpaceMembersPage{}
		case "/rpc/users", "/rpc/users/search":
		default:
			t.Errorf("unexpected route %s", request.URL.Path)
		}
		writeJSON(t, w, Response{ProtocolVersion: ProtocolVersion, ID: "contract", Outcome: ResponseOutcome{Status: "ok", Data: mustJSON(t, Result{Type: result, Data: mustJSON(t, payload)})}})
	}))
	defer server.Close()
	client := NewClient(server.URL).WithSessionNamespace("account-a")
	ctx := context.Background()
	if _, err := client.RefreshAccountState(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Users(ctx, UsersRequest{UserIDs: []int64{42}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SearchUsers(ctx, SearchUsersRequest{Query: "mo"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SpaceMembers(ctx, SpaceMembersRequest{SpaceID: 5}); err != nil {
		t.Fatal(err)
	}
	if calls != 4 {
		t.Fatalf("got %d calls", calls)
	}
}
