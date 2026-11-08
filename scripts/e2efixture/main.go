package main

import (
	"context"
	"crypto/rand"
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
	saveMu        sync.Mutex
	generation    string
	spaces        []sidecar.SpaceRecord
	access        []sidecar.SpaceAccessRecord
	readStates    map[int64]sidecar.ReadStateRecord
	sourceCalls   map[string]int
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
	Sequence uint64          `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
}

type storedState struct {
	LoggedIn      bool                              `json:"logged_in"`
	NextMessageID int64                             `json:"next_message_id"`
	Generation    string                            `json:"generation"`
	Sequence      uint64                            `json:"sequence"`
	Events        []fixtureEvent                    `json:"events"`
	Dialogs       []sidecar.DialogRecord            `json:"dialogs"`
	Spaces        []sidecar.SpaceRecord             `json:"spaces"`
	Access        []sidecar.SpaceAccessRecord       `json:"access"`
	ReadStates    map[int64]sidecar.ReadStateRecord `json:"read_states"`
	History       map[int64][]sidecar.MessageRecord `json:"history"`
	SourceCalls   map[string]int                    `json:"source_calls"`
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
		generation:    rand.Text(),
		spaces:        []sidecar.SpaceRecord{{SpaceID: 301, Name: "Fixture Workspace A"}, {SpaceID: 302, Name: "Fixture Workspace B"}},
		access:        []sidecar.SpaceAccessRecord{{SpaceID: 301, Status: "accessible"}, {SpaceID: 302, Status: "accessible"}},
		readStates:    map[int64]sidecar.ReadStateRecord{101: {ChatID: 101}, 202: {ChatID: 202, ReadMaxID: int64Ptr(6000), MarkedUnread: true}, 208: {ChatID: 208, ReadMaxID: int64Ptr(10), MarkedUnread: true}},
		sourceCalls:   make(map[string]int),
		nextMessageID: 7000,
		users: []sidecar.UserRecord{
			{UserID: accountID, DisplayName: stringPtr("Fixture Self"), Username: stringPtr("fixture_self")},
			{UserID: peerUserID, DisplayName: stringPtr("Ada Fixture"), Username: stringPtr("ada_fixture")},
			{UserID: groupUserID, DisplayName: stringPtr("Lin Fixture"), Username: stringPtr("lin_fixture")},
		},
		dialogs: []sidecar.DialogRecord{
			{Structure: &sidecar.DialogStructure{Kind: "direct", PeerUserID: int64Ptr(peerUserID)}, ChatID: dmChatID, PeerUserID: int64Ptr(peerUserID), Title: &dmTitle, LastMessageID: &dmLast},
			{Structure: &sidecar.DialogStructure{Kind: "channel", SpaceID: int64Ptr(301)}, SpaceID: int64Ptr(301), NotificationMode: stringPtr("mentions"), IsPublic: boolPtr(false), ChatID: groupChatID, Title: &groupTitle, LastMessageID: &groupLast},
			{ChatID: 203, Title: stringPtr("Fixture Public"), Structure: &sidecar.DialogStructure{Kind: "channel", SpaceID: int64Ptr(301)}, SpaceID: int64Ptr(301), IsPublic: boolPtr(true), NotificationMode: stringPtr("all")},
			{ChatID: 208, Title: stringPtr("Fixture Receipt Order"), Structure: &sidecar.DialogStructure{Kind: "channel", SpaceID: int64Ptr(301)}, SpaceID: int64Ptr(301), LastMessageID: int64Ptr(20)},
			{ChatID: 404, Title: stringPtr("Fixture Hidden"), Structure: &sidecar.DialogStructure{Kind: "channel"}, ChatListHidden: boolPtr(true)},
			{ChatID: 405, Title: stringPtr("Fixture Child"), Structure: &sidecar.DialogStructure{Kind: "reply_child", ParentChatID: int64Ptr(202), ParentMessageID: int64Ptr(6001)}, ParentChatID: int64Ptr(202), ParentMessageID: int64Ptr(6001), ChatListHidden: boolPtr(true)},
			{ChatID: 406, Title: stringPtr("Fixture Unknown")},
			{ChatID: 407, Title: stringPtr("Fixture Archived"), Structure: &sidecar.DialogStructure{Kind: "channel"}, Archived: boolPtr(true), NotificationMode: stringPtr("none")},
		},
		history: map[int64][]sidecar.MessageRecord{
			208: {{ChatID: 208, MessageID: 20, SenderID: groupUserID, Timestamp: 100, Content: sidecar.MessageContent{Type: "text", Text: "fixture source unread 20"}}, {ChatID: 208, MessageID: 10, SenderID: groupUserID, Timestamp: 200, Content: sidecar.MessageContent{Type: "text", Text: "fixture source read 10"}}},
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
	mux.HandleFunc("/compatibility", func(w http.ResponseWriter, r *http.Request) { srv.writeJSON(w, srv.protocol()) })
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
	mux.HandleFunc("/rpc/state/chat/history", srv.handleHistory)
	mux.HandleFunc("/rpc/state/account", srv.handleAccountState)
	mux.HandleFunc("/rpc/state/chat", srv.handleChatState)
	mux.HandleFunc("/rpc/chat/participants", srv.handleParticipants)
	mux.HandleFunc("/rpc/chat/participants/add", srv.handleEmpty)
	mux.HandleFunc("/rpc/chat/participants/remove", srv.handleEmpty)
	mux.HandleFunc("/rpc/chat/info", srv.handleEmpty)
	mux.HandleFunc("/rpc/chat/delete", srv.handleEmpty)
	mux.HandleFunc("/rpc/send", srv.handleSend)
	mux.HandleFunc("/rpc/read", srv.handleRead)
	mux.HandleFunc("/rpc/marked-unread", srv.handleEmpty)
	mux.HandleFunc("/rpc/dialog/notifications", srv.handleEmpty)
	mux.HandleFunc("/rpc/typing", srv.handleEmpty)
	mux.HandleFunc("/rpc/events/ack", srv.handleEventAck)
	mux.HandleFunc("/ws/events", srv.handleEvents)
	mux.HandleFunc("/fixture/presentation", srv.handlePresentation)
	mux.HandleFunc("/fixture/sent", srv.handleFixtureSent)
	mux.HandleFunc("/fixture/status", srv.handleFixtureStatus)
	mux.HandleFunc("/fixture/push-message", srv.handleFixturePushMessage)
}

func (srv *fixtureServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	srv.writeJSON(w, sidecar.Health{
		OK:              true,
		EventGeneration: srv.generation,
		Protocol:        srv.protocol(),
		Status:          srv.status().Status,
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
	srv.mu.Lock()
	defer srv.mu.Unlock()
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
	srv.mu.Lock()
	defer srv.mu.Unlock()
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
	if r.URL.Path == "/rpc/marked-unread" || r.URL.Path == "/rpc/dialog/notifications" {
		srv.mu.Lock()
		srv.sourceCalls[r.URL.Path]++
		srv.mu.Unlock()
		_ = srv.saveState()
	}
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
		if event.Sequence <= afterSequence {
			continue
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		err := conn.Write(ctx, websocket.MessageText, event.Payload)
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
	srv.writeJSON(w, sidecar.EventAckResponse{Generation: request.Generation, AcknowledgedSequence: request.Sequence})
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
		"source_calls":    srv.sourceCalls,
		"read_states":     srv.readStates,
		"generation":      srv.generation,
		"sequence":        srv.sequence,
	})
}

func (srv *fixtureServer) handleFixturePushMessage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ChatID    int64  `json:"chat_id"`
		SenderID  int64  `json:"sender_id"`
		MessageID int64  `json:"message_id"`
		Text      string `json:"text"`
		Timestamp int64  `json:"timestamp"`
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
		Timestamp: req.Timestamp,
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
			if srv.dialogs[idx].LastMessageID != nil && *srv.dialogs[idx].LastMessageID > messageID {
				return
			}
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
		"generation":        srv.generation,
		"reliability":       sidecar.EventLossless,
		"event":             event,
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	srv.mu.Lock()
	srv.events = append(srv.events, fixtureEvent{Sequence: sequence, Payload: data})
	srv.mu.Unlock()
	if err := srv.saveState(); err != nil {
		return err
	}
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
	if state.Generation != "" {
		srv.generation = state.Generation
	}
	srv.sequence = state.Sequence
	srv.events = state.Events
	if state.Dialogs != nil {
		srv.dialogs = state.Dialogs
	}
	if state.Spaces != nil {
		srv.spaces = state.Spaces
	}
	if state.Access != nil {
		srv.access = state.Access
	}
	if state.ReadStates != nil {
		srv.readStates = state.ReadStates
	}
	if state.History != nil {
		srv.history = state.History
	}
	if state.SourceCalls != nil {
		srv.sourceCalls = state.SourceCalls
	}
	if state.NextMessageID > 0 {
		srv.nextMessageID = state.NextMessageID
	}
	return nil
}

func (srv *fixtureServer) saveState() error {
	srv.saveMu.Lock()
	defer srv.saveMu.Unlock()
	srv.mu.Lock()
	state := storedState{LoggedIn: srv.loggedIn, NextMessageID: srv.nextMessageID, Generation: srv.generation, Sequence: srv.sequence, Events: srv.events, Dialogs: srv.dialogs, Spaces: srv.spaces, Access: srv.access, ReadStates: srv.readStates, History: srv.history, SourceCalls: srv.sourceCalls}
	data, err := json.Marshal(state)
	srv.mu.Unlock()
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(srv.statePath), 0o700); err != nil {
		return err
	}
	tmp := srv.statePath + ".tmp"
	if err = os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, srv.statePath)
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

func boolPtr(v bool) *bool { return &v }
func (srv *fixtureServer) handleAccountState(w http.ResponseWriter, r *http.Request) {
	if !srv.requireLogin(w) {
		return
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.writeResult(w, "account_state", sidecar.AccountStateSnapshot{Spaces: srv.spaces, SpaceAccess: srv.access, SpaceAccessComplete: true, Users: srv.users, DeletedChatIDs: []int64{}})
}
func (srv *fixtureServer) handleChatState(w http.ResponseWriter, r *http.Request) {
	var req sidecar.ChatStateRequest
	if !decodeRequest(w, r, &req) || !srv.requireLogin(w) {
		return
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	state := sidecar.ChatStateSnapshot{ChatID: req.ChatID, DeletedMessageIDs: []int64{}, ReactionSnapshotMessageIDs: []int64{}}
	for _, d := range srv.dialogs {
		if d.ChatID == req.ChatID {
			copy := d
			state.Dialog = &copy
			break
		}
	}
	if read, ok := srv.readStates[req.ChatID]; ok {
		state.ReadState = &read
	}
	srv.writeResult(w, "chat_state", state)
}
func (srv *fixtureServer) handleRead(w http.ResponseWriter, r *http.Request) {
	var req sidecar.ReadRequest
	if !decodeRequest(w, r, &req) || !srv.requireLogin(w) {
		return
	}
	srv.mu.Lock()
	srv.sourceCalls["/rpc/read"]++
	state := srv.readStates[req.ChatID]
	state.ChatID = req.ChatID
	state.ReadMaxID = req.MaxMessageID
	state.MarkedUnread = false
	srv.readStates[req.ChatID] = state
	srv.mu.Unlock()
	_ = srv.saveState()
	srv.writeResult(w, "empty", struct{}{})
}
func (srv *fixtureServer) handlePresentation(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Action       string `json:"action"`
		ChatID       int64  `json:"chat_id"`
		SpaceID      int64  `json:"space_id"`
		ReadMaxID    int64  `json:"read_max_id"`
		MarkedUnread bool   `json:"marked_unread"`
	}
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.ChatID == 0 {
		req.ChatID = 202
	}
	var evt map[string]any
	srv.mu.Lock()
	switch req.Action {
	case "receipt_case":
		if req.ChatID <= 0 {
			srv.mu.Unlock()
			http.Error(w, "receipt case requires a positive chat ID", http.StatusBadRequest)
			return
		}
		for _, d := range srv.dialogs {
			if d.ChatID == req.ChatID {
				srv.mu.Unlock()
				http.Error(w, "receipt case already exists", http.StatusConflict)
				return
			}
		}
		srv.dialogs = append(srv.dialogs, sidecar.DialogRecord{ChatID: req.ChatID, Title: stringPtr("Fixture Receipt Order"), Structure: &sidecar.DialogStructure{Kind: "channel", SpaceID: int64Ptr(301)}, SpaceID: int64Ptr(301), LastMessageID: int64Ptr(20)})
		srv.history[req.ChatID] = []sidecar.MessageRecord{
			{ChatID: req.ChatID, MessageID: 20, SenderID: groupUserID, Timestamp: 100, Content: sidecar.MessageContent{Type: "text", Text: "fixture source unread 20"}},
			{ChatID: req.ChatID, MessageID: 10, SenderID: groupUserID, Timestamp: 200, Content: sidecar.MessageContent{Type: "text", Text: "fixture source read 10"}},
		}
		srv.readStates[req.ChatID] = sidecar.ReadStateRecord{ChatID: req.ChatID, ReadMaxID: int64Ptr(10), MarkedUnread: true}
		evt = map[string]any{"ChatUpserted": map[string]any{"chat_id": req.ChatID}}
	case "read":
		srv.readStates[req.ChatID] = sidecar.ReadStateRecord{ChatID: req.ChatID, ReadMaxID: int64Ptr(req.ReadMaxID), MarkedUnread: req.MarkedUnread}
		evt = map[string]any{"ReadStateChanged": map[string]any{"chat_id": req.ChatID}}
	case "move", "detach":
		found := false
		for i := range srv.dialogs {
			if srv.dialogs[i].ChatID == req.ChatID {
				d := &srv.dialogs[i]
				s := &sidecar.DialogStructure{Kind: "channel"}
				if req.Action == "move" {
					s.SpaceID = int64Ptr(req.SpaceID)
				}
				d.Structure = s
				d.SpaceID = s.SpaceID
				d.PeerUserID = nil
				d.ParentChatID = nil
				d.ParentMessageID = nil
				found = true
				break
			}
		}
		if !found {
			srv.mu.Unlock()
			http.Error(w, "unknown fixture chat", 400)
			return
		}
		evt = map[string]any{"ChatUpserted": map[string]any{"chat_id": req.ChatID}}
	case "access_removed", "access_restored":
		status := "removed"
		if req.Action == "access_restored" {
			status = "accessible"
		}
		for i := range srv.access {
			if srv.access[i].SpaceID == req.SpaceID {
				srv.access[i].Status = status
			}
		}
		evt = map[string]any{"SpaceMemberChanged": map[string]any{"space_id": req.SpaceID, "user_id": accountID}}
	case "rename":
		for i := range srv.spaces {
			if srv.spaces[i].SpaceID == req.SpaceID {
				srv.spaces[i].Name = "Fixture Renamed Workspace"
			}
		}
		evt = map[string]any{"SpaceUpserted": map[string]any{"space_id": req.SpaceID}}
	default:
		srv.mu.Unlock()
		http.Error(w, "unknown fixture action", 400)
		return
	}
	srv.mu.Unlock()
	if err := srv.saveState(); err != nil {
		srv.writeError(w, err)
		return
	}
	if err := srv.broadcast(evt); err != nil {
		srv.writeError(w, err)
		return
	}
	srv.writeJSON(w, map[string]any{"ok": true})
}
