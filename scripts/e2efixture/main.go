package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

const (
	accountID      int64 = 16000
	peerUserID     int64 = 16001
	groupUserID    int64 = 16002
	dmChatID       int64 = 101
	groupChatID    int64 = 202
	fixtureVersion       = "fixture"
)

type fixtureServer struct {
	mu            sync.Mutex
	statePath     string
	loggedIn      bool
	nextMessageID int64
	dialogs       []sidecar.DialogRecord
	history       map[int64][]sidecar.MessageRecord
	users         []sidecar.UserRecord
	sent          []sentText
	clients       map[*websocket.Conn]struct{}
	sequence      uint64
	events        []fixtureEvent
}

type fixtureEvent struct {
	sequence uint64
	payload  []byte
}

type storedState struct {
	LoggedIn      bool  `json:"logged_in"`
	NextMessageID int64 `json:"next_message_id"`
}

type sentText struct {
	ChatID    int64  `json:"chat_id"`
	Text      string `json:"text"`
	MessageID int64  `json:"message_id"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("e2efixture", flag.ContinueOnError)
	bind := fs.String("bind", "127.0.0.1:29342", "HTTP bind address")
	statePath := fs.String("state", "", "fixture state file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*statePath) == "" {
		return errors.New("--state is required")
	}

	srv := newFixtureServer(*statePath)
	if err := srv.loadState(); err != nil {
		return err
	}
	mux := http.NewServeMux()
	srv.register(mux)
	log.Printf("fixture sidecar listening on %s", *bind)
	return http.ListenAndServe(*bind, mux)
}

func newFixtureServer(statePath string) *fixtureServer {
	dmTitle := "Ada Fixture"
	groupTitle := "Fixture Team"
	dmLast := int64(5001)
	groupLast := int64(6001)
	return &fixtureServer{
		statePath:     statePath,
		nextMessageID: 7000,
		users: []sidecar.UserRecord{
			{UserID: accountID, DisplayName: stringPtr("Fixture Self"), Username: stringPtr("fixture_self")},
			{UserID: peerUserID, DisplayName: stringPtr("Ada Fixture"), Username: stringPtr("ada_fixture")},
			{UserID: groupUserID, DisplayName: stringPtr("Lin Fixture"), Username: stringPtr("lin_fixture")},
		},
		dialogs: []sidecar.DialogRecord{
			{ChatID: dmChatID, PeerUserID: int64Ptr(peerUserID), Title: &dmTitle, LastMessageID: &dmLast},
			{ChatID: groupChatID, Title: &groupTitle, LastMessageID: &groupLast},
		},
		history: map[int64][]sidecar.MessageRecord{
			dmChatID: {{
				ChatID:    dmChatID,
				MessageID: dmLast,
				SenderID:  peerUserID,
				Timestamp: time.Now().Add(-2 * time.Minute).Unix(),
				Content:   sidecar.MessageContent{Type: "text", Text: "fixture dm hello"},
			}},
			groupChatID: {{
				ChatID:    groupChatID,
				MessageID: groupLast,
				SenderID:  groupUserID,
				Timestamp: time.Now().Add(-1 * time.Minute).Unix(),
				Content:   sidecar.MessageContent{Type: "text", Text: "fixture group hello"},
			}},
		},
		clients: make(map[*websocket.Conn]struct{}),
	}
}

func (srv *fixtureServer) register(mux *http.ServeMux) {
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/status", srv.handleStatus)
	mux.HandleFunc("/rpc/resume", srv.handleResume)
	mux.HandleFunc("/rpc/connect", srv.handleConnect)
	mux.HandleFunc("/rpc/logout", srv.handleLogout)
	mux.HandleFunc("/rpc/auth/start", srv.handleAuthStart)
	mux.HandleFunc("/rpc/auth/verify", srv.handleAuthVerify)
	mux.HandleFunc("/rpc/dialogs", srv.handleDialogs)
	mux.HandleFunc("/rpc/state/dialogs", srv.handleDialogs)
	mux.HandleFunc("/rpc/history", srv.handleHistory)
	mux.HandleFunc("/rpc/chat/participants", srv.handleParticipants)
	mux.HandleFunc("/rpc/chat/participants/add", srv.handleEmpty)
	mux.HandleFunc("/rpc/chat/participants/remove", srv.handleEmpty)
	mux.HandleFunc("/rpc/chat/info", srv.handleEmpty)
	mux.HandleFunc("/rpc/chat/delete", srv.handleEmpty)
	mux.HandleFunc("/rpc/send", srv.handleSend)
	mux.HandleFunc("/rpc/read", srv.handleEmpty)
	mux.HandleFunc("/rpc/marked-unread", srv.handleEmpty)
	mux.HandleFunc("/rpc/dialog/notifications", srv.handleEmpty)
	mux.HandleFunc("/rpc/typing", srv.handleEmpty)
	mux.HandleFunc("/rpc/events/ack", srv.handleEventAck)
	mux.HandleFunc("/ws/events", srv.handleEvents)
	mux.HandleFunc("/fixture/sent", srv.handleFixtureSent)
	mux.HandleFunc("/fixture/status", srv.handleFixtureStatus)
	mux.HandleFunc("/fixture/push-message", srv.handleFixturePushMessage)
}

func (srv *fixtureServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	srv.writeJSON(w, sidecar.Health{
		OK:       true,
		Protocol: srv.protocol(),
		Status:   srv.status().Status,
	})
}

func (srv *fixtureServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	srv.writeResult(w, "status", srv.status())
}

func (srv *fixtureServer) handleResume(w http.ResponseWriter, r *http.Request) {
	srv.writeResult(w, "status", srv.status())
}

func (srv *fixtureServer) handleConnect(w http.ResponseWriter, r *http.Request) {
	srv.mu.Lock()
	srv.loggedIn = true
	srv.mu.Unlock()
	if err := srv.saveState(); err != nil {
		srv.writeError(w, err)
		return
	}
	srv.writeResult(w, "status", srv.status())
}

func (srv *fixtureServer) handleLogout(w http.ResponseWriter, r *http.Request) {
	srv.mu.Lock()
	srv.loggedIn = false
	srv.mu.Unlock()
	_ = srv.saveState()
	srv.writeResult(w, "empty", struct{}{})
}

func (srv *fixtureServer) handleAuthStart(w http.ResponseWriter, r *http.Request) {
	var req sidecar.AuthStartRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.Contact == "" {
		srv.writeSidecarError(w, "Auth", "contact is required")
		return
	}
	srv.writeResult(w, "auth_start", sidecar.AuthStartResult{
		ExistingUser:   true,
		ChallengeToken: "fixture-challenge",
	})
}

func (srv *fixtureServer) handleAuthVerify(w http.ResponseWriter, r *http.Request) {
	var req sidecar.AuthVerifyRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Code) == "" {
		srv.writeSidecarError(w, "Auth", "verification code is required")
		return
	}
	srv.mu.Lock()
	srv.loggedIn = true
	srv.mu.Unlock()
	if err := srv.saveState(); err != nil {
		srv.writeError(w, err)
		return
	}
	srv.writeResult(w, "auth_verify", sidecar.AuthVerifyResult{
		UserID:           accountID,
		AccountNamespace: strconv.FormatInt(accountID, 10),
		Status:           srv.status(),
	})
}

func (srv *fixtureServer) handleDialogs(w http.ResponseWriter, r *http.Request) {
	var req sidecar.DialogsRequest
	if !decodeRequest(w, r, &req) || !srv.requireLogin(w) {
		return
	}
	limit := len(srv.dialogs)
	if req.Limit != nil && int(*req.Limit) < limit {
		limit = int(*req.Limit)
	}
	srv.writeResult(w, "dialogs", sidecar.DialogsPage{
		Dialogs: append([]sidecar.DialogRecord(nil), srv.dialogs[:limit]...),
		Users:   append([]sidecar.UserRecord(nil), srv.users...),
	})
}

func (srv *fixtureServer) handleHistory(w http.ResponseWriter, r *http.Request) {
	var req sidecar.HistoryRequest
	if !decodeRequest(w, r, &req) || !srv.requireLogin(w) {
		return
	}
	messages := append([]sidecar.MessageRecord(nil), srv.history[req.ChatID]...)
	messages = filterHistory(messages, req.BeforeMessageID, req.AfterMessageID)
	if req.Limit != nil && int(*req.Limit) < len(messages) {
		messages = messages[:int(*req.Limit)]
	}
	srv.writeResult(w, "history", sidecar.HistoryPage{
		Messages: messages,
		Users:    append([]sidecar.UserRecord(nil), srv.users...),
	})
}

func (srv *fixtureServer) handleParticipants(w http.ResponseWriter, r *http.Request) {
	var req sidecar.ChatParticipantsRequest
	if !decodeRequest(w, r, &req) || !srv.requireLogin(w) {
		return
	}
	participants := []sidecar.ChatParticipantRecord{{UserID: accountID}}
	if req.ChatID == groupChatID {
		participants = append(participants, sidecar.ChatParticipantRecord{UserID: peerUserID}, sidecar.ChatParticipantRecord{UserID: groupUserID})
	} else if req.ChatID == dmChatID {
		participants = append(participants, sidecar.ChatParticipantRecord{UserID: peerUserID})
	}
	srv.writeResult(w, "chat_participants", sidecar.ChatParticipantsPage{
		Participants: participants,
		Users:        append([]sidecar.UserRecord(nil), srv.users...),
	})
}

func (srv *fixtureServer) handleSend(w http.ResponseWriter, r *http.Request) {
	var req sidecar.SendTextRequest
	if !decodeRequest(w, r, &req) || !srv.requireLogin(w) {
		return
	}
	chatID := req.Peer.ChatID
	if chatID == 0 {
		chatID = dmChatID
	}
	srv.mu.Lock()
	srv.nextMessageID++
	messageID := srv.nextMessageID
	record := sidecar.MessageRecord{
		ChatID:     chatID,
		MessageID:  messageID,
		SenderID:   accountID,
		Timestamp:  time.Now().Unix(),
		IsOutgoing: true,
		Content:    sidecar.MessageContent{Type: "text", Text: req.Text},
		Transaction: &sidecar.TransactionIdentity{
			TransactionID:  fmt.Sprintf("fixture-%d", messageID),
			ExternalID:     req.ExternalID,
			RandomID:       messageID,
			FinalMessageID: &messageID,
		},
	}
	srv.history[chatID] = append(srv.history[chatID], record)
	srv.sent = append(srv.sent, sentText{ChatID: chatID, Text: req.Text, MessageID: messageID})
	srv.setLastMessageIDLocked(chatID, messageID)
	srv.mu.Unlock()
	_ = srv.saveState()
	srv.writeResult(w, "message", sidecar.MessageMutation{
		MessageID: &messageID,
		State:     sidecar.TransactionCompleted,
		Transaction: sidecar.TransactionIdentity{
			TransactionID:  fmt.Sprintf("fixture-%d", messageID),
			ExternalID:     req.ExternalID,
			RandomID:       messageID,
			FinalMessageID: &messageID,
		},
	})
}

func (srv *fixtureServer) handleEmpty(w http.ResponseWriter, r *http.Request) {
	if !srv.requireLogin(w) {
		return
	}
	srv.writeResult(w, "empty", struct{}{})
}

func (srv *fixtureServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	afterSequence, _ := strconv.ParseUint(r.URL.Query().Get("after_sequence"), 10, 64)
	srv.mu.Lock()
	for _, event := range srv.events {
		if event.sequence <= afterSequence {
			continue
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		err := conn.Write(ctx, websocket.MessageText, event.payload)
		cancel()
		if err != nil {
			srv.mu.Unlock()
			return
		}
	}
	srv.clients[conn] = struct{}{}
	srv.mu.Unlock()
	defer func() {
		srv.mu.Lock()
		delete(srv.clients, conn)
		srv.mu.Unlock()
		_ = conn.Close(websocket.StatusNormalClosure, "done")
	}()
	for {
		if _, _, err := conn.Reader(r.Context()); err != nil {
			return
		}
	}
}

func (srv *fixtureServer) handleEventAck(w http.ResponseWriter, r *http.Request) {
	var request sidecar.EventAckRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		srv.writeError(w, err)
		return
	}
	srv.writeJSON(w, sidecar.EventAckResponse{AcknowledgedSequence: request.Sequence})
}

func (srv *fixtureServer) handleFixtureSent(w http.ResponseWriter, r *http.Request) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.writeJSON(w, map[string]any{"sent_texts": srv.sent})
}

func (srv *fixtureServer) handleFixtureStatus(w http.ResponseWriter, r *http.Request) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.writeJSON(w, map[string]any{
		"logged_in":       srv.loggedIn,
		"event_clients":   len(srv.clients),
		"sent_texts":      len(srv.sent),
		"next_message_id": srv.nextMessageID,
	})
}

func (srv *fixtureServer) handleFixturePushMessage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ChatID    int64  `json:"chat_id"`
		SenderID  int64  `json:"sender_id"`
		MessageID int64  `json:"message_id"`
		Text      string `json:"text"`
	}
	if !decodeRequest(w, r, &req) || !srv.requireLogin(w) {
		return
	}
	if req.ChatID == 0 {
		req.ChatID = dmChatID
	}
	if req.SenderID == 0 {
		req.SenderID = peerUserID
	}
	if strings.TrimSpace(req.Text) == "" {
		req.Text = "fixture realtime hello"
	}

	srv.mu.Lock()
	if req.MessageID == 0 {
		srv.nextMessageID++
		req.MessageID = srv.nextMessageID
	}
	record := sidecar.MessageRecord{
		ChatID:    req.ChatID,
		MessageID: req.MessageID,
		SenderID:  req.SenderID,
		Timestamp: time.Now().Unix(),
		Content:   sidecar.MessageContent{Type: "text", Text: req.Text},
	}
	srv.history[req.ChatID] = append(srv.history[req.ChatID], record)
	srv.setLastMessageIDLocked(req.ChatID, req.MessageID)
	srv.mu.Unlock()
	_ = srv.saveState()

	if err := srv.broadcast(map[string]any{
		"MessageStored": map[string]any{"message": record},
	}); err != nil {
		srv.writeError(w, err)
		return
	}
	srv.writeJSON(w, map[string]any{"ok": true, "message_id": req.MessageID})
}

func (srv *fixtureServer) status() sidecar.Status {
	status := sidecar.StatusAuthRequired
	srv.mu.Lock()
	loggedIn := srv.loggedIn
	srv.mu.Unlock()
	if loggedIn {
		status = sidecar.StatusConnected
	}
	return sidecar.Status{
		Protocol: srv.protocol(),
		Status:   status,
	}
}

func (srv *fixtureServer) protocol() sidecar.ProtocolInfo {
	return sidecar.ProtocolInfo{
		ProtocolVersion: sidecar.ProtocolVersion,
		ClientVersion:   fixtureVersion,
	}
}

func (srv *fixtureServer) requireLogin(w http.ResponseWriter) bool {
	srv.mu.Lock()
	loggedIn := srv.loggedIn
	srv.mu.Unlock()
	if loggedIn {
		return true
	}
	srv.writeSidecarError(w, "Auth", "fixture sidecar is not logged in")
	return false
}

func (srv *fixtureServer) setLastMessageIDLocked(chatID, messageID int64) {
	for idx := range srv.dialogs {
		if srv.dialogs[idx].ChatID == chatID {
			srv.dialogs[idx].LastMessageID = &messageID
			return
		}
	}
}

func (srv *fixtureServer) broadcast(event map[string]any) error {
	srv.mu.Lock()
	srv.sequence++
	sequence := srv.sequence
	clients := make([]*websocket.Conn, 0, len(srv.clients))
	for conn := range srv.clients {
		clients = append(clients, conn)
	}
	srv.mu.Unlock()
	envelope := map[string]any{
		"protocol_version":  sidecar.ProtocolVersion,
		"session_namespace": strconv.FormatInt(accountID, 10),
		"sequence":          sequence,
		"reliability":       sidecar.EventLossless,
		"event":             event,
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	srv.mu.Lock()
	srv.events = append(srv.events, fixtureEvent{sequence: sequence, payload: data})
	srv.mu.Unlock()
	for _, conn := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := conn.Write(ctx, websocket.MessageText, data)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (srv *fixtureServer) loadState() error {
	data, err := os.ReadFile(srv.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("read fixture state: %w", err)
	}
	var state storedState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("parse fixture state: %w", err)
	}
	srv.loggedIn = state.LoggedIn
	if state.NextMessageID > 0 {
		srv.nextMessageID = state.NextMessageID
	}
	return nil
}

func (srv *fixtureServer) saveState() error {
	srv.mu.Lock()
	state := storedState{LoggedIn: srv.loggedIn, NextMessageID: srv.nextMessageID}
	srv.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(srv.statePath), 0o700); err != nil {
		return fmt.Errorf("create fixture state dir: %w", err)
	}
	data, err := json.MarshalIndent(state, "", "\t")
	if err != nil {
		return err
	}
	return os.WriteFile(srv.statePath, data, 0o600)
}

func (srv *fixtureServer) writeResult(w http.ResponseWriter, resultType string, data any) {
	resultData, err := json.Marshal(data)
	if err != nil {
		srv.writeError(w, err)
		return
	}
	result, err := json.Marshal(sidecar.Result{Type: resultType, Data: resultData})
	if err != nil {
		srv.writeError(w, err)
		return
	}
	srv.writeJSON(w, sidecar.Response{
		ProtocolVersion: sidecar.ProtocolVersion,
		ID:              "fixture",
		Outcome: sidecar.ResponseOutcome{
			Status: "ok",
			Data:   result,
		},
	})
}

func (srv *fixtureServer) writeSidecarError(w http.ResponseWriter, category, message string) {
	data, _ := json.Marshal(sidecar.Error{Category: category, Message: message})
	srv.writeJSON(w, sidecar.Response{
		ProtocolVersion: sidecar.ProtocolVersion,
		ID:              "fixture",
		Outcome: sidecar.ResponseOutcome{
			Status: "error",
			Data:   data,
		},
	})
}

func (srv *fixtureServer) writeError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (srv *fixtureServer) writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func decodeRequest(w http.ResponseWriter, r *http.Request, out any) bool {
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func filterHistory(messages []sidecar.MessageRecord, before, after *int64) []sidecar.MessageRecord {
	sort.SliceStable(messages, func(i, j int) bool {
		return messages[i].MessageID < messages[j].MessageID
	})
	filtered := messages[:0]
	for _, msg := range messages {
		if before != nil && msg.MessageID >= *before {
			continue
		}
		if after != nil && msg.MessageID <= *after {
			continue
		}
		filtered = append(filtered, msg)
	}
	return filtered
}

func stringPtr(value string) *string {
	return &value
}

func int64Ptr(value int64) *int64 {
	return &value
}
