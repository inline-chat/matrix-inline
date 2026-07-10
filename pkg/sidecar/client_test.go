package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

func TestClientStatusDecodesVersionedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		writeJSON(t, w, Response{
			ProtocolVersion: ProtocolVersion,
			ID:              "http-1",
			Outcome: ResponseOutcome{
				Status: "ok",
				Data: mustJSON(t, Result{
					Type: "status",
					Data: mustJSON(t, Status{
						Protocol: ProtocolInfo{ProtocolVersion: ProtocolVersion, ClientVersion: "0.4.0"},
						Status:   StatusConnected,
					}),
				}),
			},
		})
	}))
	defer server.Close()

	status, err := NewClient(server.URL).Status(context.Background())
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if status.Status != StatusConnected {
		t.Fatalf("status = %q, want %q", status.Status, StatusConnected)
	}
}

func TestClientConnectReturnsSidecarError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/connect" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		writeJSON(t, w, Response{
			ProtocolVersion: ProtocolVersion,
			ID:              "http-1",
			Outcome: ResponseOutcome{
				Status: "error",
				Data: mustJSON(t, Error{
					Category: "AuthExpired",
					Message:  "token expired",
				}),
			},
		})
	}))
	defer server.Close()

	_, err := NewClient(server.URL).Connect(context.Background(), "secret-token", "team")
	if err == nil {
		t.Fatal("Connect() error = nil, want sidecar error")
	}
	sidecarErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("error type = %T, want *Error", err)
	}
	if sidecarErr.Category != "AuthExpired" {
		t.Fatalf("category = %q, want AuthExpired", sidecarErr.Category)
	}
}

func TestClientAuthStartAndVerify(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rpc/auth/start":
			var request AuthStartRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode auth start request: %v", err)
			}
			if request.Kind != AuthContactEmail || request.Contact != "mo@example.com" {
				t.Fatalf("auth start request = %#v, want email contact", request)
			}
			writeRPCResult(t, w, "auth_start", AuthStartResult{
				ExistingUser:   true,
				ChallengeToken: "challenge-token",
			})
		case "/rpc/auth/verify":
			var request AuthVerifyRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode auth verify request: %v", err)
			}
			if request.Code != "123456" || request.ChallengeToken != "challenge-token" {
				t.Fatalf("auth verify request = %#v, want code and challenge", request)
			}
			writeRPCResult(t, w, "auth_verify", AuthVerifyResult{
				UserID:           42,
				AccountNamespace: "42",
				Status: Status{
					Protocol: ProtocolInfo{ProtocolVersion: ProtocolVersion, ClientVersion: "0.4.0"},
					Status:   StatusConnected,
				},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	start, err := client.AuthStart(context.Background(), AuthStartRequest{
		Contact: "mo@example.com",
		Kind:    AuthContactEmail,
	})
	if err != nil {
		t.Fatalf("AuthStart() error = %v", err)
	}
	if !start.ExistingUser || start.ChallengeToken != "challenge-token" {
		t.Fatalf("start = %#v, want existing user with challenge", start)
	}

	verify, err := client.AuthVerify(context.Background(), AuthVerifyRequest{
		Contact:        "mo@example.com",
		Kind:           AuthContactEmail,
		Code:           "123456",
		ChallengeToken: start.ChallengeToken,
	})
	if err != nil {
		t.Fatalf("AuthVerify() error = %v", err)
	}
	if verify.UserID != 42 || verify.AccountNamespace != "42" || verify.Status.Status != StatusConnected {
		t.Fatalf("verify = %#v, want connected user 42", verify)
	}
}

func TestClientResumeDecodesStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/resume" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		writeRPCResult(t, w, "status", Status{
			Protocol: ProtocolInfo{ProtocolVersion: ProtocolVersion, ClientVersion: "0.4.0"},
			Status:   StatusConnected,
		})
	}))
	defer server.Close()

	status, err := NewClient(server.URL).Resume(context.Background())
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if status.Status != StatusConnected {
		t.Fatalf("status = %q, want connected", status.Status)
	}
}

func TestClientDialogsAndHistoryDecodeVersionedResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rpc/dialogs":
			synced := int64(11)
			peerUserID := int64(42)
			writeRPCResult(t, w, "dialogs", DialogsPage{
				Dialogs: []DialogRecord{{ChatID: 7, PeerUserID: &peerUserID, SyncedThroughMessageID: &synced}},
				Users: []UserRecord{{
					UserID:      42,
					DisplayName: stringPtr("Ada Lovelace"),
					IsBot:       boolPtr(false),
				}},
			})
		case "/rpc/history":
			var request HistoryRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode history request: %v", err)
			}
			if request.AfterMessageID == nil || *request.AfterMessageID != 10 {
				t.Fatalf("after_message_id = %#v, want 10", request.AfterMessageID)
			}
			writeRPCResult(t, w, "history", HistoryPage{
				Messages: []MessageRecord{{
					ChatID:    7,
					MessageID: 11,
					SenderID:  2,
					Content: MessageContent{
						Type: "text",
						Text: "hello",
					},
				}},
				Users: []UserRecord{{
					UserID:   43,
					Username: stringPtr("grace"),
				}},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	dialogs, err := client.Dialogs(context.Background(), DialogsRequest{})
	if err != nil {
		t.Fatalf("Dialogs() error = %v", err)
	}
	if len(dialogs.Dialogs) != 1 || dialogs.Dialogs[0].ChatID != 7 {
		t.Fatalf("dialogs = %#v, want chat 7", dialogs.Dialogs)
	}
	if dialogs.Dialogs[0].PeerUserID == nil || *dialogs.Dialogs[0].PeerUserID != 42 {
		t.Fatalf("peer_user_id = %#v, want 42", dialogs.Dialogs[0].PeerUserID)
	}
	if dialogs.Dialogs[0].SyncedThroughMessageID == nil || *dialogs.Dialogs[0].SyncedThroughMessageID != 11 {
		t.Fatalf("synced_through_message_id = %#v, want 11", dialogs.Dialogs[0].SyncedThroughMessageID)
	}
	if len(dialogs.Users) != 1 || dialogs.Users[0].DisplayName == nil || *dialogs.Users[0].DisplayName != "Ada Lovelace" {
		t.Fatalf("dialog users = %#v, want Ada Lovelace", dialogs.Users)
	}

	history, err := client.History(context.Background(), HistoryRequest{
		ChatID:         7,
		AfterMessageID: int64Ptr(10),
	})
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(history.Messages) != 1 || history.Messages[0].Content.Text != "hello" {
		t.Fatalf("history = %#v, want text message", history.Messages)
	}
	if len(history.Users) != 1 || history.Users[0].Username == nil || *history.Users[0].Username != "grace" {
		t.Fatalf("history users = %#v, want grace", history.Users)
	}
}

func TestClientParticipantsAndChatCreation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rpc/chat/participants":
			var request ChatParticipantsRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode participants request: %v", err)
			}
			if request.ChatID != 7 {
				t.Fatalf("ChatID = %d, want 7", request.ChatID)
			}
			writeRPCResult(t, w, "chat_participants", ChatParticipantsPage{
				Participants: []ChatParticipantRecord{{UserID: 42}},
			})
		case "/rpc/chat/participants/add":
			var request AddChatParticipantRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode add participant request: %v", err)
			}
			if request.ChatID != 7 || request.UserID != 42 {
				t.Fatalf("add participant request = %#v, want chat 7 user 42", request)
			}
			writeRPCResult(t, w, "empty", struct{}{})
		case "/rpc/chat/participants/remove":
			var request RemoveChatParticipantRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode remove participant request: %v", err)
			}
			if request.ChatID != 7 || request.UserID != 42 {
				t.Fatalf("remove participant request = %#v, want chat 7 user 42", request)
			}
			writeRPCResult(t, w, "empty", struct{}{})
		case "/rpc/chat/info":
			var request UpdateChatInfoRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode update chat info request: %v", err)
			}
			if request.ChatID != 7 || request.Title == nil || *request.Title != "Renamed" {
				t.Fatalf("update chat info request = %#v, want chat 7 Renamed", request)
			}
			writeRPCResult(t, w, "empty", struct{}{})
		case "/rpc/chat/delete":
			var request DeleteChatRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode delete chat request: %v", err)
			}
			if request.ChatID != 7 {
				t.Fatalf("delete chat request = %#v, want chat 7", request)
			}
			writeRPCResult(t, w, "empty", struct{}{})
		case "/rpc/marked-unread":
			var request SetMarkedUnreadRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode marked unread request: %v", err)
			}
			if request.ChatID != 7 || !request.Unread {
				t.Fatalf("marked unread request = %#v, want chat 7 unread", request)
			}
			writeRPCResult(t, w, "empty", struct{}{})
		case "/rpc/dialog/notifications":
			var request UpdateDialogNotificationsRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode dialog notifications request: %v", err)
			}
			if request.ChatID != 7 || request.Mode == nil || *request.Mode != DialogNotificationNone {
				t.Fatalf("dialog notifications request = %#v, want chat 7 none", request)
			}
			writeRPCResult(t, w, "empty", struct{}{})
		case "/rpc/chat/create-dm":
			var request CreateDMRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode create DM request: %v", err)
			}
			if request.UserID != 42 {
				t.Fatalf("UserID = %d, want 42", request.UserID)
			}
			writeRPCResult(t, w, "created_chat", CreatedChat{ChatID: 99})
		case "/rpc/chat/create-thread":
			var request CreateThreadRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode create thread request: %v", err)
			}
			if request.Title == nil || *request.Title != "Launch" {
				t.Fatalf("Title = %#v, want Launch", request.Title)
			}
			writeRPCResult(t, w, "created_chat", CreatedChat{ChatID: 100, Title: request.Title})
		case "/rpc/chat/create-reply-thread":
			var request CreateReplyThreadRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode create reply thread request: %v", err)
			}
			if request.ParentChatID != 7 || request.ParentMessageID == nil || *request.ParentMessageID != 11 {
				t.Fatalf("reply thread request = %#v, want parent 7 message 11", request)
			}
			writeRPCResult(t, w, "created_chat", CreatedChat{
				ChatID:          101,
				ParentChatID:    int64Ptr(7),
				ParentMessageID: int64Ptr(11),
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	participants, err := client.ChatParticipants(context.Background(), ChatParticipantsRequest{ChatID: 7})
	if err != nil {
		t.Fatalf("ChatParticipants() error = %v", err)
	}
	if len(participants.Participants) != 1 || participants.Participants[0].UserID != 42 {
		t.Fatalf("participants = %#v, want user 42", participants.Participants)
	}
	if err := client.AddChatParticipant(context.Background(), AddChatParticipantRequest{ChatID: 7, UserID: 42}); err != nil {
		t.Fatalf("AddChatParticipant() error = %v", err)
	}
	if err := client.RemoveChatParticipant(context.Background(), RemoveChatParticipantRequest{ChatID: 7, UserID: 42}); err != nil {
		t.Fatalf("RemoveChatParticipant() error = %v", err)
	}
	renamed := "Renamed"
	if err := client.UpdateChatInfo(context.Background(), UpdateChatInfoRequest{ChatID: 7, Title: &renamed}); err != nil {
		t.Fatalf("UpdateChatInfo() error = %v", err)
	}
	if err := client.SetMarkedUnread(context.Background(), SetMarkedUnreadRequest{ChatID: 7, Unread: true}); err != nil {
		t.Fatalf("SetMarkedUnread() error = %v", err)
	}
	muted := DialogNotificationNone
	if err := client.UpdateDialogNotifications(context.Background(), UpdateDialogNotificationsRequest{ChatID: 7, Mode: &muted}); err != nil {
		t.Fatalf("UpdateDialogNotifications() error = %v", err)
	}
	if err := client.DeleteChat(context.Background(), DeleteChatRequest{ChatID: 7}); err != nil {
		t.Fatalf("DeleteChat() error = %v", err)
	}

	dm, err := client.CreateDM(context.Background(), CreateDMRequest{UserID: 42})
	if err != nil {
		t.Fatalf("CreateDM() error = %v", err)
	}
	if dm.ChatID != 99 {
		t.Fatalf("DM ChatID = %d, want 99", dm.ChatID)
	}

	threadTitle := "Launch"
	thread, err := client.CreateThread(context.Background(), CreateThreadRequest{Title: &threadTitle})
	if err != nil {
		t.Fatalf("CreateThread() error = %v", err)
	}
	if thread.ChatID != 100 || thread.Title == nil || *thread.Title != "Launch" {
		t.Fatalf("thread = %#v, want chat 100 Launch", thread)
	}

	reply, err := client.CreateReplyThread(context.Background(), CreateReplyThreadRequest{
		ParentChatID:    7,
		ParentMessageID: int64Ptr(11),
	})
	if err != nil {
		t.Fatalf("CreateReplyThread() error = %v", err)
	}
	if reply.ChatID != 101 || reply.ParentMessageID == nil || *reply.ParentMessageID != 11 {
		t.Fatalf("reply thread = %#v, want chat 101 parent message 11", reply)
	}
}

func TestClientUploadSendsMultipartMetadataAndFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/upload" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data;") {
			t.Fatalf("content-type = %q, want multipart", r.Header.Get("Content-Type"))
		}
		if err := r.ParseMultipartForm(1024 * 1024); err != nil {
			t.Fatalf("ParseMultipartForm() error = %v", err)
		}
		var metadata UploadRequest
		if err := json.Unmarshal([]byte(r.FormValue("metadata")), &metadata); err != nil {
			t.Fatalf("decode metadata: %v", err)
		}
		if metadata.Kind != "photo" || metadata.Peer.ChatID != 7 {
			t.Fatalf("metadata = %#v", metadata)
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			t.Fatalf("FormFile() error = %v", err)
		}
		defer file.Close()
		data, err := io.ReadAll(file)
		if err != nil {
			t.Fatalf("ReadAll(file) error = %v", err)
		}
		if string(data) != "image bytes" {
			t.Fatalf("file data = %q", string(data))
		}
		writeRPCResult(t, w, "message", MessageMutation{
			Transaction: TransactionIdentity{TransactionID: "txn-1", RandomID: 9},
			MessageID:   int64Ptr(11),
			State:       TransactionCompleted,
		})
	}))
	defer server.Close()

	fileName := "image.png"
	mutation, err := NewClient(server.URL).Upload(context.Background(), UploadRequest{
		Peer:     ChatPeer(7),
		Kind:     "photo",
		FileName: &fileName,
	}, []byte("image bytes"))
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if mutation.MessageID == nil || *mutation.MessageID != 11 {
		t.Fatalf("mutation = %#v, want message 11", mutation)
	}
}

func TestEventEnvelopeDecodesMessageStored(t *testing.T) {
	raw := []byte(`{
		"protocol_version": 3,
		"session_namespace": "42",
		"sequence": 4,
		"reliability": "Lossless",
		"event": {
			"MessageStored": {
				"message": {
					"chat_id": 7,
					"message_id": 11,
					"sender_id": 2,
					"timestamp": 123,
					"is_outgoing": false,
					"content": {"type": "text", "text": "hello"}
				}
			}
		}
	}`)

	var envelope EventEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("unmarshal event envelope: %v", err)
	}
	if envelope.Event.Type != "MessageStored" {
		t.Fatalf("event type = %q, want MessageStored", envelope.Event.Type)
	}
	if envelope.Event.MessageStored == nil || envelope.Event.MessageStored.Message.MessageID != 11 {
		t.Fatalf("message stored event = %#v", envelope.Event.MessageStored)
	}
	if envelope.Event.MessageStored.Message.Content.Text != "hello" {
		t.Fatalf("message text = %q, want hello", envelope.Event.MessageStored.Message.Content.Text)
	}
}

func TestClientEventDecoderCoversEveryClientVariant(t *testing.T) {
	variants := map[string]string{
		"StatusChanged":           `{"status":"Connected"}`,
		"TransactionChanged":      `{"identity":{"transaction_id":"txn","random_id":1},"state":"Sent"}`,
		"ChatUpserted":            `{"chat_id":7}`,
		"ChatDeleted":             `{"chat_id":7}`,
		"ChatParticipantsChanged": `{"chat_id":7}`,
		"UserUpserted":            `{"user_id":2}`,
		"SpaceUpserted":           `{"space_id":3}`,
		"SpaceMemberChanged":      `{"space_id":3,"user_id":2,"removed":false}`,
		"UserSettingsChanged":     `{}`,
		"MessageActionInvoked":    `{"interaction_id":1,"chat_id":7,"message_id":8,"actor_user_id":2,"action_id":"ok","data":""}`,
		"MessageActionAnswered":   `{"interaction_id":1}`,
		"MessageUpserted":         `{"chat_id":7,"message_id":8}`,
		"MessageStored":           `{"message":{"chat_id":7,"message_id":8,"sender_id":2,"timestamp":1,"is_outgoing":false,"content":{"type":"text","text":"hello"}}}`,
		"MessageDeleted":          `{"chat_id":7,"message_id":8}`,
		"ChatHistoryCleared":      `{"chat_id":7}`,
		"ReactionChanged":         `{"chat_id":7,"message_id":8,"user_id":2,"reaction":"👍","removed":false}`,
		"ReadStateChanged":        `{"chat_id":7}`,
		"Typing":                  `{"chat_id":7,"user_id":2,"is_typing":true}`,
		"UserStatusChanged":       `{"user_id":2,"is_online":true}`,
		"BotPresenceChanged":      `{"bot_user_id":2,"kind":"Typing","avatar_changed":false}`,
		"NewMessageNotification":  `{"message":{"chat_id":7,"message_id":8,"sender_id":2,"timestamp":1,"is_outgoing":false,"content":{"type":"text","text":"hello"}},"reason":"Mention"}`,
	}
	for variant, payload := range variants {
		t.Run(variant, func(t *testing.T) {
			var event ClientEvent
			if err := json.Unmarshal([]byte(`{"`+variant+`":`+payload+`}`), &event); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if event.Type != variant {
				t.Fatalf("event type = %q, want %q", event.Type, variant)
			}
		})
	}
}

func TestClientEventDecoderRejectsUnknownAndAmbiguousVariants(t *testing.T) {
	for name, payload := range map[string]string{
		"unknown":   `{"FutureLosslessEvent":{}}`,
		"empty":     `{}`,
		"ambiguous": `{"ChatUpserted":{"chat_id":7},"ChatDeleted":{"chat_id":7}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var event ClientEvent
			if err := json.Unmarshal([]byte(payload), &event); err == nil {
				t.Fatal("Unmarshal() error = nil, want rejection")
			}
		})
	}
}

func TestEventEnvelopeDecodesChatHistoryCleared(t *testing.T) {
	raw := []byte(`{
		"protocol_version": 3,
		"session_namespace": "42",
		"sequence": 5,
		"reliability": "Lossless",
		"event": {"ChatHistoryCleared": {"chat_id": 7, "before_date": 123}}
	}`)

	var envelope EventEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("unmarshal event envelope: %v", err)
	}
	if envelope.Event.ChatHistoryCleared == nil || envelope.Event.ChatHistoryCleared.ChatID != 7 {
		t.Fatalf("history cleared event = %#v", envelope.Event.ChatHistoryCleared)
	}
	if envelope.Event.ChatHistoryCleared.BeforeDate == nil || *envelope.Event.ChatHistoryCleared.BeforeDate != 123 {
		t.Fatalf("before date = %#v, want 123", envelope.Event.ChatHistoryCleared.BeforeDate)
	}
}

func TestEventEnvelopeDecodesSpaceMemberChanged(t *testing.T) {
	raw := []byte(`{
		"protocol_version": 3,
		"session_namespace": "42",
		"sequence": 6,
		"reliability": "Lossless",
		"event": {"SpaceMemberChanged": {"space_id": 5, "user_id": 3, "removed": true}}
	}`)

	var envelope EventEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("unmarshal event envelope: %v", err)
	}
	if envelope.Event.SpaceMemberChanged == nil || !envelope.Event.SpaceMemberChanged.Removed {
		t.Fatalf("space member event = %#v", envelope.Event.SpaceMemberChanged)
	}
}

func TestEventsAfterRequestsNamespaceAndCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws/events" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("session_namespace"); got != "team/42" {
			t.Fatalf("session_namespace = %q, want team/42", got)
		}
		if got := r.URL.Query().Get("after_sequence"); got != "9" {
			t.Fatalf("after_sequence = %q, want 9", got)
		}
		if got := r.URL.Query().Get("generation"); got != "generation-1" {
			t.Fatalf("generation = %q, want generation-1", got)
		}
		if got := r.Header.Get("X-Inline-Session-Namespace"); got != "team-42" {
			t.Fatalf("session namespace header = %q, want team-42", got)
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		defer conn.CloseNow()
		payload := fmt.Sprintf(`{"protocol_version":%d,"session_namespace":"team/42","generation":"generation-1","sequence":10,"reliability":"Lossless","event":{"ChatUpserted":{"chat_id":7}}}`, ProtocolVersion)
		if err := conn.Write(r.Context(), websocket.MessageText, []byte(payload)); err != nil {
			t.Errorf("write event: %v", err)
		}
	}))
	defer server.Close()

	stream, err := NewClient(server.URL).WithSessionNamespace("team-42").EventsAfterGeneration(context.Background(), "team/42", "generation-1", 9)
	if err != nil {
		t.Fatalf("EventsAfter() error = %v", err)
	}
	defer stream.Close(websocket.StatusNormalClosure, "done")
	envelope, err := stream.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv() error = %v", err)
	}
	if envelope.Sequence == nil || *envelope.Sequence != 10 || envelope.SessionNamespace != "team/42" {
		t.Fatalf("event envelope = %#v, want namespace team/42 sequence 10", envelope)
	}
}

func TestEventsAfterMapsGoneToReplayUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGone)
		_, _ = w.Write([]byte(`{"error":"sidecar_event_replay_unavailable","requested_after_sequence":3,"oldest_retained_sequence":8,"latest_sequence":12,"event_generation":"generation-2"}`))
	}))
	defer server.Close()

	_, err := NewClient(server.URL).EventsAfter(context.Background(), "42", 3)
	if !errors.Is(err, ErrEventReplayUnavailable) {
		t.Fatalf("EventsAfter() error = %v, want ErrEventReplayUnavailable", err)
	}
	var replayErr *EventReplayUnavailableError
	if !errors.As(err, &replayErr) || replayErr.LatestSequence == nil || *replayErr.LatestSequence != 12 {
		t.Fatalf("EventsAfter() error = %#v, want structured latest sequence 12", err)
	}
}

func TestAckEventsPostsDurableCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/events/ack" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var request EventAckRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode ack request: %v", err)
		}
		if request.SessionNamespace != "42" || request.Generation != "generation-1" || request.Sequence != 11 {
			t.Fatalf("ack request = %#v, want namespace 42 sequence 11", request)
		}
		writeJSON(t, w, EventAckResponse{Generation: request.Generation, AcknowledgedSequence: request.Sequence})
	}))
	defer server.Close()

	if err := NewClient(server.URL).AckEventsGeneration(context.Background(), "42", "generation-1", 11); err != nil {
		t.Fatalf("AckEvents() error = %v", err)
	}
}

func TestWebsocketURLConvertsHTTPBaseURL(t *testing.T) {
	got := websocketURL("http://127.0.0.1:29342", "/ws/events")
	want := "ws://127.0.0.1:29342/ws/events"
	if got != want {
		t.Fatalf("websocketURL = %q, want %q", got, want)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func writeRPCResult(t *testing.T, w http.ResponseWriter, resultType string, data any) {
	t.Helper()
	writeJSON(t, w, Response{
		ProtocolVersion: ProtocolVersion,
		ID:              "http-1",
		Outcome: ResponseOutcome{
			Status: "ok",
			Data: mustJSON(t, Result{
				Type: resultType,
				Data: mustJSON(t, data),
			}),
		},
	})
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return raw
}

func int64Ptr(value int64) *int64 {
	return &value
}

func stringPtr(value string) *string {
	return &value
}

func boolPtr(value bool) *bool {
	return &value
}
