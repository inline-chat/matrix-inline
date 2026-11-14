package sidecar

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestV5RichRequestsPreserveEntitiesAndAccountHeaders(t *testing.T) {
	wantEntities := []MessageEntityRecord{{Kind: "TYPE_BOLD", Offset: 1, Length: 2}}
	seen := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Inline-Protocol-Version") != "5" || r.Header.Get("X-Inline-Session-Namespace") != "account/42" {
			t.Errorf("missing version/account isolation headers: %v", r.Header)
		}
		seen[r.URL.Path] = true
		if r.URL.Path == "/rpc/events/ack" {
			writeJSON(t, w, EventAckResponse{})
			return
		}
		var payload struct {
			Text     string                `json:"text"`
			Caption  string                `json:"caption"`
			Entities []MessageEntityRecord `json:"entities"`
		}
		var err error
		if r.URL.Path == "/rpc/upload" {
			if err = r.ParseMultipartForm(1024); err != nil {
				t.Error(err)
				http.Error(w, "invalid multipart", 400)
				return
			}
			defer r.MultipartForm.RemoveAll()
			err = json.Unmarshal([]byte(r.FormValue("metadata")), &payload)
			file, _, fileErr := r.FormFile("file")
			if fileErr != nil {
				t.Error(fileErr)
			} else {
				data, readErr := io.ReadAll(file)
				file.Close()
				if readErr != nil || string(data) != "attachment" {
					t.Errorf("attachment changed: %q %v", data, readErr)
				}
			}
		} else {
			err = json.NewDecoder(r.Body).Decode(&payload)
		}
		if err != nil || !reflect.DeepEqual(payload.Entities, wantEntities) || (payload.Text != "A😀B" && payload.Caption != "A😀B") {
			t.Errorf("rich payload lost UTF-16 entities: %+v %v", payload, err)
		}
		kind := "message"
		if r.URL.Path == "/rpc/edit" {
			kind = "empty"
		}
		writeRPCResult(t, w, kind, MessageMutation{MessageID: int64Ptr(11), State: TransactionCompleted})
	}))
	defer server.Close()
	client := NewClient(server.URL).WithSessionNamespace("account/42")
	if _, err := client.SendText(t.Context(), SendTextRequest{Peer: ChatPeer(7), Text: "A😀B", Entities: wantEntities}); err != nil {
		t.Fatal(err)
	}
	if err := client.EditMessage(t.Context(), EditMessageRequest{ChatID: 7, MessageID: 11, Text: "A😀B", Entities: wantEntities}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Upload(t.Context(), UploadRequest{Peer: ChatPeer(7), Kind: "document", Caption: stringPtr("A😀B"), Entities: wantEntities}, []byte("attachment")); err != nil {
		t.Fatal(err)
	}
	if err := client.AckEventsGeneration(t.Context(), "account/42", "generation", 5); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 4 {
		t.Fatalf("missing contract requests: %v", seen)
	}
}

func TestV5DirectoryAndStructureWireContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Inline-Protocol-Version") != "5" {
			t.Error("missing protocol version")
		}
		switch r.URL.Path {
		case "/rpc/state/account/refresh":
			writeRPCResult(t, w, "account_state", json.RawMessage(`{"spaces":[{"space_id":3,"name":"Work","creator":false,"date":1}],"space_memberships":[{"space_id":3,"user_id":42,"date":1,"can_access_public_chats":true}],"removed_space_ids":[4],"spaces_complete":false,"deleted_chat_ids":[]}`))
		case "/rpc/state/dialogs":
			writeRPCResult(t, w, "dialogs", json.RawMessage(`{"dialogs":[{"chat_id":7,"kind":"chat","structure_known":true,"structure_seq":9,"space_id":null,"parent_chat_id":8,"parent_message_id":11,"description":"Child"}]}`))
		case "/rpc/users", "/rpc/users/search":
			writeRPCResult(t, w, "users", json.RawMessage(`{"users":[{"user_id":42,"username":"person"}],"complete":false}`))
		case "/rpc/spaces/members":
			var request SpaceMembersRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.SpaceID != 3 || request.Cursor != "17" || request.Refresh {
				t.Errorf("roster request: %+v %v", request, err)
			}
			writeRPCResult(t, w, "space_members", json.RawMessage(`{"members":[{"space_id":3,"user_id":42,"date":1,"can_access_public_chats":true}],"users":[],"complete":false,"next_cursor":"42"}`))
		default:
			t.Errorf("unexpected endpoint: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := NewClient(server.URL)
	account, err := client.RefreshAccountState(t.Context())
	if err != nil || account == nil {
		t.Fatal(err)
	}
	if account.SpacesComplete || len(account.SpaceMemberships) != 1 || account.SpaceMemberships[0].UserID != 42 || !reflect.DeepEqual(account.RemovedSpaceIDs, []int64{4}) {
		t.Fatalf("authority/completeness lost: %+v", account)
	}
	dialogs, err := client.CachedDialogs(t.Context(), DialogsRequest{})
	if err != nil || dialogs == nil || len(dialogs.Dialogs) != 1 {
		t.Fatalf("dialogs: %+v %v", dialogs, err)
	}
	dialog := dialogs.Dialogs[0]
	if dialog.Kind != ConversationChat || !dialog.StructureKnown || dialog.SpaceID != nil || dialog.StructureSeq == nil || *dialog.StructureSeq != 9 || dialog.ParentChatID == nil || *dialog.ParentChatID != 8 || dialog.ParentMessageID == nil || *dialog.ParentMessageID != 11 {
		t.Fatalf("structure/detach lost: %+v", dialog)
	}
	users, err := client.Users(t.Context(), UsersRequest{UserIDs: []int64{42}})
	if err != nil || users == nil || users.Complete || len(users.Users) != 1 {
		t.Fatalf("users: %+v %v", users, err)
	}
	users, err = client.SearchUsers(t.Context(), SearchUsersRequest{Query: "person"})
	if err != nil || users == nil || users.Complete || len(users.Users) != 1 {
		t.Fatalf("search: %+v %v", users, err)
	}
	roster, err := client.SpaceMembers(t.Context(), SpaceMembersRequest{SpaceID: 3, Cursor: "17", Refresh: false})
	if err != nil || roster == nil || roster.Complete || roster.NextCursor != "42" || len(roster.Members) != 1 {
		t.Fatalf("roster: %+v %v", roster, err)
	}
}
