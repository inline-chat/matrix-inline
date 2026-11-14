package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestWorkspaceFetchCannotReapplyAnOlderMembershipAfterRevocation(t *testing.T) {
	oldStarted, releaseOld, removalFetched := make(chan struct{}), make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rpc/state/account/refresh":
			close(oldStarted)
			select {
			case <-releaseOld:
			case <-r.Context().Done():
				return
			}
			writeConnectorSidecarResult(t, w, "account_state", sidecar.AccountStateSnapshot{
				Spaces: []sidecar.SpaceRecord{{SpaceID: 3, Name: "Work"}}, SpaceMemberships: []sidecar.SpaceMemberRecord{{SpaceID: 3, UserID: 1}}, SpacesComplete: true,
			})
		case "/rpc/state/account":
			close(removalFetched)
			writeConnectorSidecarResult(t, w, "account_state", sidecar.AccountStateSnapshot{RemovedSpaceIDs: []int64{3}, SpacesComplete: true})
		default:
			t.Errorf("unexpected source request: %s", r.URL.Path)
			http.Error(w, "unexpected request", 500)
		}
	}))
	defer server.Close()
	client := &InlineClient{AccountID: "1", UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "1", Metadata: &UserLoginMetadata{}}}, Sidecar: sidecar.NewClient(server.URL)}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- client.refreshWorkspaceNavigation(ctx) }()
	select {
	case <-oldStarted:
	case <-ctx.Done():
		close(releaseOld)
		t.Fatal("old snapshot fetch did not start")
	}
	go func() { results <- client.reconcileAccountDeletes(ctx) }()
	overlapped := false
	select {
	case <-removalFetched:
		overlapped = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseOld)
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("account reconciliation did not finish")
		}
	}
	if overlapped {
		t.Fatal("a second account snapshot was fetched before the first was reconciled")
	}
	if saved := client.workspaceNavigationSnapshot()["3"]; saved == nil || saved.Active {
		t.Fatalf("revoked workspace reactivated by older response: %+v", saved)
	}
}

func TestMatrixMutationsRejectTargetsFromAnotherChatOrLogin(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "foreign target reached source", 500)
	}))
	defer server.Close()
	client := &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "42"}}, Sidecar: sidecar.NewClient(server.URL)}
	portal := testPortal("7")
	portal.Receiver = "42"
	for _, target := range []*database.Message{
		{ID: "8/11", Room: portal.PortalKey},
		{ID: "7/11", Room: networkid.PortalKey{ID: "7", Receiver: "43"}},
		{ID: "7/0", Room: portal.PortalKey},
	} {
		if err := client.HandleMatrixMessageRemove(t.Context(), &bridgev2.MatrixMessageRemove{MatrixEventBase: bridgev2.MatrixEventBase[*event.RedactionEventContent]{Portal: portal}, TargetMessage: target}); err == nil {
			t.Fatal("foreign redaction target accepted")
		}
		if _, err := client.HandleMatrixReaction(t.Context(), &bridgev2.MatrixReaction{MatrixEventBase: bridgev2.MatrixEventBase[*event.ReactionEventContent]{Portal: portal, Content: &event.ReactionEventContent{RelatesTo: event.RelatesTo{Key: "👍"}}}, TargetMessage: target}); err == nil {
			t.Fatal("foreign reaction target accepted")
		}
		if err := client.HandleMatrixReactionRemove(t.Context(), &bridgev2.MatrixReactionRemove{MatrixEventBase: bridgev2.MatrixEventBase[*event.RedactionEventContent]{Portal: portal}, TargetReaction: &database.Reaction{Room: target.Room, MessageID: target.ID, Emoji: "👍"}}); err == nil {
			t.Fatal("foreign reaction removal target accepted")
		}
		if err := client.HandleMatrixReadReceipt(t.Context(), &bridgev2.MatrixReadReceipt{Portal: portal, ExactMessage: target}); err == nil {
			t.Fatal("foreign receipt target accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("foreign target made %d source mutations", calls.Load())
	}
}

func TestMatrixReceiptRequiresExplicitMappedTarget(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/read" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"chunk":[{"event_id":"$read","type":"m.room.message","content":{"msgtype":"m.text","body":"x"}}]}`)
			return
		}
		calls.Add(1)
		var request sidecar.ReadRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if r.URL.Path != "/rpc/read" || request.ChatID != 7 || request.MaxMessageID == nil || *request.MaxMessageID != 11 {
			t.Errorf("receipt did not preserve exact target: %s %+v", r.URL.Path, request)
		}
		writeConnectorSidecarResult(t, w, "empty", struct{}{})
	}))
	defer server.Close()
	db := testBridgeDatabase(t)
	bridge, _ := identityTestBridge(t, db)
	matrixClient, err := mautrix.NewClient(server.URL, "@bridge:example.org", "test-token")
	if err != nil {
		t.Fatal(err)
	}
	bridge.Bot = &matrix.ASIntent{Matrix: &appservice.IntentAPI{Client: matrixClient}}
	client := &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "42"}, Bridge: bridge}, Sidecar: sidecar.NewClient(server.URL), history: map[int64]int64{7: 10}, historyLoaded: map[int64]struct{}{7: {}}}
	portal := testPortal("7")
	portal.Receiver = "42"
	portal.MXID = "!room:example.org"
	target := &database.Message{ID: "7/11", Room: portal.PortalKey, MXID: "$read", SenderID: "1", Timestamp: time.Now()}
	if err := db.Portal.Insert(t.Context(), portal.Portal); err != nil {
		t.Fatal(err)
	}
	if err := db.Ghost.Insert(t.Context(), &database.Ghost{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Message.Insert(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	if err := client.HandleMatrixReadReceipt(t.Context(), &bridgev2.MatrixReadReceipt{Portal: portal}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("unknown receipt read all source messages")
	}
	if err := client.HandleMatrixReadReceipt(t.Context(), &bridgev2.MatrixReadReceipt{Portal: portal, EventID: "$read", ExactMessage: target}); err == nil {
		t.Fatal("live receipt advanced past incomplete source history")
	}
	if calls.Load() != 0 {
		t.Fatal("uncovered source history was marked read")
	}
	client.history[7] = 11
	if err := client.HandleMatrixReadReceipt(t.Context(), &bridgev2.MatrixReadReceipt{Portal: portal, EventID: "$read", ExactMessage: target}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one exact receipt, got %d", calls.Load())
	}
}

func TestSourceReadReceiptRequiresTimelineOrderAsWellAsHistoryCoverage(t *testing.T) {
	key := networkid.PortalKey{ID: "7", Receiver: "42"}
	lookup := func(_ context.Context, eventID id.EventID) (*database.Message, error) {
		var messageID int64
		switch eventID {
		case "$99":
			messageID = 99
		case "$100":
			messageID = 100
		case "$101":
			messageID = 101
		default:
			return nil, nil
		}
		return &database.Message{ID: makeMessageID(7, messageID), Room: key, MXID: eventID}, nil
	}
	for _, test := range []struct {
		name string
		ids  []id.EventID
		safe bool
	}{
		{"contiguous", []id.EventID{"$99", "$100", "$101"}, true},
		{"older source delivered after live target", []id.EventID{"$100", "$99"}, false},
		{"unknown timeline event", []id.EventID{"$99", "$100", "$unknown"}, false},
		{"target absent", []id.EventID{"$99", "$101"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var timeline []*event.Event
			for _, eventID := range test.ids {
				timeline = append(timeline, &event.Event{ID: eventID, Type: event.EventMessage})
			}
			safe, err := verifiedSourceReadReceipt(t.Context(), timeline, key, "$100", 100, lookup)
			if err != nil || safe != test.safe {
				t.Fatalf("receipt proof = %v, %v; want %v", safe, err, test.safe)
			}
		})
	}
}

func TestReadTraversalOnlyTreatsVerifiedPlaintextEditsAsTransparent(t *testing.T) {
	key := networkid.PortalKey{ID: "7", Receiver: "42"}
	lookup := func(_ context.Context, eventID id.EventID) (*database.Message, error) {
		if eventID == "$original" {
			return &database.Message{ID: "7/1", Room: key, MXID: eventID, SenderMXID: "@alice:example.org"}, nil
		}
		if eventID == "$next" {
			return &database.Message{ID: "7/2", Room: key, MXID: eventID, SenderMXID: "@bob:example.org"}, nil
		}
		return nil, nil
	}
	original := &event.Event{ID: "$original", Type: event.EventMessage, Sender: "@alice:example.org"}
	next := &event.Event{ID: "$next", Type: event.EventMessage, Sender: "@bob:example.org"}
	for _, test := range []struct {
		name   string
		edit   *event.Event
		first  bool
		prefix id.EventID
		safe   bool
	}{
		{"verified raw edit", &event.Event{ID: "$edit", Type: event.EventMessage, Sender: original.Sender, Content: event.Content{VeryRaw: json.RawMessage(`{"msgtype":"m.text","body":"* changed","m.new_content":{"msgtype":"m.text","body":"changed"},"m.relates_to":{"rel_type":"m.replace","event_id":"$original"}}`)}}, false, "$next", true},
		{"sender spoof", &event.Event{ID: "$edit", Type: event.EventMessage, Sender: "@mallory:example.org", Content: event.Content{Parsed: &event.MessageEventContent{NewContent: &event.MessageEventContent{Body: "changed"}, RelatesTo: &event.RelatesTo{Type: event.RelReplace, EventID: "$original"}}}}, false, "$original", false},
		{"missing original", &event.Event{ID: "$edit", Type: event.EventMessage, Sender: original.Sender, Content: event.Content{Parsed: &event.MessageEventContent{NewContent: &event.MessageEventContent{Body: "changed"}, RelatesTo: &event.RelatesTo{Type: event.RelReplace, EventID: "$original"}}}}, true, "", false},
		{"encrypted relation", &event.Event{ID: "$edit", Type: event.EventEncrypted, Sender: original.Sender}, false, "$original", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			timeline := []*event.Event{original, test.edit, next}
			if test.first {
				timeline = []*event.Event{test.edit, original, next}
			}
			prefix, err := verifiedReadPrefix(t.Context(), timeline, key, 2, lookup)
			if err != nil || prefix != test.prefix {
				t.Fatalf("read prefix = %s, %v; want %s", prefix, err, test.prefix)
			}
			safe, err := verifiedSourceReadReceipt(t.Context(), timeline, key, "$next", 2, lookup)
			if err != nil || safe != test.safe {
				t.Fatalf("source receipt = %v, %v; want %v", safe, err, test.safe)
			}
			if test.safe {
				prefix, err := verifiedReadPrefix(t.Context(), timeline, key, 1, lookup)
				if err != nil || prefix != "$original" {
					t.Fatalf("edit became a receipt target: %s %v", prefix, err)
				}
				foreign := key
				foreign.Receiver = "43"
				if safe, err := verifiedTimelineEdit(t.Context(), test.edit, foreign, map[id.EventID]id.UserID{"$original": original.Sender}, lookup); err != nil || safe {
					t.Fatalf("edit accepted another login's original mapping: %v %v", safe, err)
				}
			}
		})
	}
}

func TestPendingWorkspaceLinkRetryRechecksPrivateChatAccess(t *testing.T) {
	db := testBridgeDatabase(t)
	bridge, api := identityTestBridge(t, db)
	login := &database.UserLogin{ID: "1", UserMXID: "@owner:example.org", Metadata: &UserLoginMetadata{WorkspaceNavigation: map[string]*WorkspaceNavigation{"3": {SpaceID: 3, Active: true, RoomID: "!space:example.org", Links: map[string]WorkspaceRoomLink{"7": {RoomID: "!private:example.org"}}}}}}
	if err := db.User.Insert(t.Context(), &database.User{MXID: login.UserMXID}); err != nil {
		t.Fatal(err)
	}
	if err := db.UserLogin.Insert(t.Context(), login); err != nil {
		t.Fatal(err)
	}
	var allowed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !allowed.Load() {
			http.Error(w, "private access removed", http.StatusForbidden)
			return
		}
		writeConnectorSidecarResult(t, w, "chat_participants", sidecar.ChatParticipantsPage{})
	}))
	defer server.Close()
	client := &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: login, Bridge: bridge}, Sidecar: sidecar.NewClient(server.URL)}
	client.retryWorkspaceLinks(t.Context())
	if len(api.states) != 0 || client.workspaceNavigationSnapshot()["3"].Links["7"].Attached {
		t.Fatal("retry advertised a private chat after source access failed")
	}
	allowed.Store(true)
	client.retryWorkspaceLinks(t.Context())
	if len(api.states) != 1 || !client.workspaceNavigationSnapshot()["3"].Links["7"].Attached {
		t.Fatal("verified pending navigation failed to recover")
	}
}

func TestDeletedChatNavigationCleanupRetriesAndPreservesOtherRooms(t *testing.T) {
	db := testBridgeDatabase(t)
	bridge, api := identityTestBridge(t, db)
	login := &database.UserLogin{ID: "1", UserMXID: "@owner:example.org", Metadata: &UserLoginMetadata{WorkspaceNavigation: map[string]*WorkspaceNavigation{"3": {SpaceID: 3, Active: true, RoomID: "!space:example.org", Links: map[string]WorkspaceRoomLink{"7": {RoomID: "!deleted:example.org", Attached: true}, "8": {RoomID: "!retained:example.org", Attached: true}}}}}}
	if err := db.User.Insert(t.Context(), &database.User{MXID: login.UserMXID}); err != nil {
		t.Fatal(err)
	}
	if err := db.UserLogin.Insert(t.Context(), login); err != nil {
		t.Fatal(err)
	}
	client := &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: login, Bridge: bridge}}
	api.failure = errors.New("Matrix unavailable")
	if err := client.removeChatWorkspaceLinks(t.Context(), 7); err == nil {
		t.Fatal("failed link removal was accepted")
	}
	if len(client.workspaceNavigationSnapshot()["3"].Links) != 2 {
		t.Fatal("failed cleanup discarded its retry state")
	}
	api.failure = nil
	if err := client.removeChatWorkspaceLinks(t.Context(), 7); err != nil {
		t.Fatal(err)
	}
	stored, err := db.UserLogin.GetByID(t.Context(), "1")
	if err != nil {
		t.Fatal(err)
	}
	links := stored.Metadata.(*UserLoginMetadata).WorkspaceNavigation["3"].Links
	if len(links) != 1 || links["8"].RoomID != "!retained:example.org" || len(api.states) != 1 || api.states[0].key != "!deleted:example.org" || len(api.states[0].content.Parsed.(*event.SpaceChildEventContent).Via) != 0 {
		t.Fatalf("deleted-chat cleanup changed unrelated navigation: %+v %+v", links, api.states)
	}
}

func TestMatrixCaptionCanBeClearedWithoutReplacingAttachment(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var request sidecar.EditMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if r.URL.Path != "/rpc/edit" || request.ChatID != 7 || request.MessageID != 11 || request.Text != "" || len(request.Entities) != 0 {
			t.Errorf("caption clear payload = %s %+v", r.URL.Path, request)
		}
		writeConnectorSidecarResult(t, w, "empty", struct{}{})
	}))
	defer server.Close()
	client := &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "42"}}, Sidecar: sidecar.NewClient(server.URL)}
	portal := testPortal("7")
	portal.Receiver = "42"
	original := &event.MessageEventContent{MsgType: event.MsgImage, Body: "old caption", FileName: "picture.jpg", URL: "mxc://example.org/stable"}
	for _, body := range []string{"", "picture.jpg"} {
		cleared := cloneMatrixContent(original)
		cleared.Body = body
		target := &database.Message{ID: "7/11", Room: portal.PortalKey, Metadata: &MessageMetadata{MatrixContent: original}}
		if err := client.HandleMatrixEdit(t.Context(), &bridgev2.MatrixEdit{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: portal, Content: cleared}, EditTarget: target}); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("expected two caption-only mutations, got %d", calls.Load())
	}
}

func TestBackfillRejectsIncompleteOrDuplicatePages(t *testing.T) {
	message := sidecar.MessageRecord{ChatID: 7, MessageID: 11, SenderID: 1, Content: sidecar.MessageContent{Type: "text", Text: "message"}}
	if _, _, err := (&InlineClient{}).backfillMessagesFromHistory(t.Context(), nil, []sidecar.MessageRecord{message, message}); err == nil {
		t.Fatal("duplicate backfill identities accepted")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeConnectorSidecarResult(t, w, "history", sidecar.HistoryPage{HasMore: true})
	}))
	defer server.Close()
	client := &InlineClient{Sidecar: sidecar.NewClient(server.URL)}
	if _, err := client.FetchMessages(t.Context(), bridgev2.FetchMessagesParams{Portal: testPortal("7")}); err == nil {
		t.Fatal("empty nonterminal backfill silently finished")
	}
}

func TestCachedHistoryRejectsForeignChatBeforeQueueing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeConnectorSidecarResult(t, w, "history", sidecar.HistoryPage{Messages: []sidecar.MessageRecord{{ChatID: 8, MessageID: 11}}})
	}))
	defer server.Close()
	client := &InlineClient{Sidecar: sidecar.NewClient(server.URL)}
	if err := client.reconcileCachedHistory(t.Context(), 7); err == nil {
		t.Fatal("foreign cached row accepted")
	}
	if err := client.syncMessageUpsert(t.Context(), 7, 11); err == nil {
		t.Fatal("foreign row accepted as requested message")
	}
}
