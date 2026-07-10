package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

func TestMakeMessageIDIsChatScoped(t *testing.T) {
	got := makeMessageID(7, 11)
	want := networkid.MessageID("7/11")
	if got != want {
		t.Fatalf("makeMessageID = %q, want %q", got, want)
	}
}

func TestUpsertExistingInlineMessageSkipsUnchangedReplay(t *testing.T) {
	message := sidecar.MessageRecord{
		ChatID:     7,
		MessageID:  11,
		SenderID:   42,
		IsOutgoing: false,
		Content:    sidecar.MessageContent{Type: "text", Text: "hello"},
	}
	existing := []*database.Message{{
		Metadata: &MessageMetadata{InlineFingerprint: inlineMessageFingerprint(message)},
	}}

	portal := testPortal("7")
	result, err := upsertExistingInlineMessage(t.Context(), portal, nil, existing, message)
	if err != nil {
		t.Fatalf("upsertExistingInlineMessage() error = %v", err)
	}
	if result.ContinueMessageHandling || len(result.SubEvents) != 0 {
		t.Fatalf("unchanged replay result = %#v, want no Matrix edit", result)
	}

	message.Content.Text = "edited"
	result, err = upsertExistingInlineMessage(t.Context(), portal, nil, existing, message)
	if err != nil {
		t.Fatalf("edited upsert error = %v", err)
	}
	if len(result.SubEvents) != 1 {
		t.Fatalf("edited upsert result = %#v, want one Matrix edit", result)
	}
}

func TestUpsertExistingInlineMediaRetriesLegacyUnavailableProjection(t *testing.T) {
	message := sidecar.MessageRecord{
		ChatID:    7,
		MessageID: 11,
		SenderID:  42,
		Content: sidecar.MessageContent{
			Type: "media",
			URL:  "https://cdn.inline.test/file",
		},
	}
	fingerprint := inlineMessageFingerprint(message)
	portal := testPortal("7")

	legacy := []*database.Message{{
		Metadata: &MessageMetadata{InlineFingerprint: fingerprint},
	}}
	result, err := upsertExistingInlineMessage(t.Context(), portal, nil, legacy, message)
	if err != nil {
		t.Fatalf("legacy media upsert error = %v", err)
	}
	if len(result.SubEvents) != 1 {
		t.Fatalf("legacy media result = %#v, want retry edit", result)
	}

	handled := []*database.Message{{
		Metadata: &MessageMetadata{InlineFingerprint: fingerprint, MediaHandled: true},
	}}
	result, err = upsertExistingInlineMessage(t.Context(), portal, nil, handled, message)
	if err != nil {
		t.Fatalf("handled media upsert error = %v", err)
	}
	if len(result.SubEvents) != 0 || result.ContinueMessageHandling {
		t.Fatalf("handled media result = %#v, want no-op", result)
	}
}

func TestMaxInlineMessageIDForChatUsesDurableBridgeRows(t *testing.T) {
	messages := []*database.Message{
		{ID: makeMessageID(7, 11)},
		{ID: makeMessageID(8, 99)},
		{ID: makeMessageID(7, 13)},
		{ID: networkid.MessageID("invalid")},
	}

	if got := maxInlineMessageIDForChat(messages, 7); got != 13 {
		t.Fatalf("maxInlineMessageIDForChat() = %d, want 13", got)
	}
}

func TestHistoryRequestForBackfillUsesCursorForBackwardPagination(t *testing.T) {
	request, err := historyRequestForBackfill(bridgev2.FetchMessagesParams{
		Portal: testPortal("7"),
		Cursor: networkid.PaginationCursor("7/21"),
		Count:  25,
	})
	if err != nil {
		t.Fatalf("historyRequestForBackfill() error = %v", err)
	}
	if request.ChatID != 7 {
		t.Fatalf("ChatID = %d, want 7", request.ChatID)
	}
	if request.Limit == nil || *request.Limit != 25 {
		t.Fatalf("Limit = %#v, want 25", request.Limit)
	}
	if request.BeforeMessageID == nil || *request.BeforeMessageID != 21 {
		t.Fatalf("BeforeMessageID = %#v, want 21", request.BeforeMessageID)
	}
	if request.AfterMessageID != nil {
		t.Fatalf("AfterMessageID = %#v, want nil", request.AfterMessageID)
	}
}

func TestHistoryRequestForBackfillUsesAnchorForForwardPagination(t *testing.T) {
	request, err := historyRequestForBackfill(bridgev2.FetchMessagesParams{
		Portal:        testPortal("7"),
		Forward:       true,
		AnchorMessage: &database.Message{ID: makeMessageID(7, 21)},
		Count:         -1,
	})
	if err != nil {
		t.Fatalf("historyRequestForBackfill() error = %v", err)
	}
	if request.Limit == nil || *request.Limit != 50 {
		t.Fatalf("Limit = %#v, want default 50", request.Limit)
	}
	if request.AfterMessageID == nil || *request.AfterMessageID != 21 {
		t.Fatalf("AfterMessageID = %#v, want 21", request.AfterMessageID)
	}
	if request.BeforeMessageID != nil {
		t.Fatalf("BeforeMessageID = %#v, want nil", request.BeforeMessageID)
	}
}

func TestBackfillMessagesFromHistorySortsAndBuildsCursor(t *testing.T) {
	replyTo := int64(10)
	records := []sidecar.MessageRecord{{
		ChatID:    7,
		MessageID: 12,
		SenderID:  4,
		Timestamp: 300,
		Content: sidecar.MessageContent{
			Type: "text",
			Text: "third",
		},
	}, {
		ChatID:     7,
		MessageID:  11,
		SenderID:   1,
		Timestamp:  200,
		IsOutgoing: true,
		Content: sidecar.MessageContent{
			Type: "text",
			Text: "second",
		},
		ReplyToMessageID: &replyTo,
	}, {
		ChatID:    7,
		MessageID: 10,
		SenderID:  2,
		Timestamp: 100,
		Content: sidecar.MessageContent{
			Type: "text",
			Text: "first",
		},
	}}

	messages, cursor := (&InlineClient{}).backfillMessagesFromHistory(context.Background(), nil, records)
	if cursor != networkid.PaginationCursor("7/10") {
		t.Fatalf("cursor = %q, want 7/10", cursor)
	}
	if len(messages) != 3 {
		t.Fatalf("messages = %d, want 3", len(messages))
	}
	if messages[0].ID != networkid.MessageID("7/10") || messages[1].ID != networkid.MessageID("7/11") || messages[2].ID != networkid.MessageID("7/12") {
		t.Fatalf("message order = %q, %q, %q", messages[0].ID, messages[1].ID, messages[2].ID)
	}
	if messages[1].Timestamp.Unix() != 200 {
		t.Fatalf("timestamp = %d, want 200", messages[1].Timestamp.Unix())
	}
	if !messages[1].Sender.IsFromMe || messages[1].Sender.Sender != makeUserID("1") {
		t.Fatalf("sender = %#v, want outgoing user 1", messages[1].Sender)
	}
	if messages[1].ConvertedMessage.ReplyTo == nil || messages[1].ConvertedMessage.ReplyTo.MessageID != networkid.MessageID("7/10") {
		t.Fatalf("reply target = %#v, want 7/10", messages[1].ConvertedMessage.ReplyTo)
	}
}

func TestFetchMessagesUsesSidecarHistory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/history" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var request sidecar.HistoryRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode history request: %v", err)
		}
		if request.ChatID != 7 || request.BeforeMessageID == nil || *request.BeforeMessageID != 21 || request.Limit == nil || *request.Limit != 2 {
			t.Fatalf("history request = %#v, want chat 7 before 21 limit 2", request)
		}
		writeConnectorSidecarResult(t, w, "history", sidecar.HistoryPage{
			Messages: []sidecar.MessageRecord{{
				ChatID:    7,
				MessageID: 20,
				SenderID:  3,
				Timestamp: 200,
				Content: sidecar.MessageContent{
					Type: "text",
					Text: "newer",
				},
			}, {
				ChatID:    7,
				MessageID: 19,
				SenderID:  2,
				Timestamp: 100,
				Content: sidecar.MessageContent{
					Type: "text",
					Text: "older",
				},
			}},
			Users: []sidecar.UserRecord{{
				UserID:      2,
				DisplayName: stringPtr("Ada"),
			}},
			HasMore: true,
		})
	}))
	defer server.Close()

	ic := &InlineClient{
		Sidecar: sidecar.NewClient(server.URL),
		users:   make(map[int64]sidecar.UserRecord),
	}
	resp, err := ic.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: testPortal("7"),
		Cursor: networkid.PaginationCursor("7/21"),
		Count:  2,
	})
	if err != nil {
		t.Fatalf("FetchMessages() error = %v", err)
	}
	if !resp.HasMore || !resp.MarkRead || resp.Forward {
		t.Fatalf("response flags = has_more:%v mark_read:%v forward:%v", resp.HasMore, resp.MarkRead, resp.Forward)
	}
	if resp.Cursor != networkid.PaginationCursor("7/19") {
		t.Fatalf("cursor = %q, want 7/19", resp.Cursor)
	}
	if len(resp.Messages) != 2 || resp.Messages[0].ID != networkid.MessageID("7/19") || resp.Messages[1].ID != networkid.MessageID("7/20") {
		t.Fatalf("messages = %#v, want 7/19 then 7/20", resp.Messages)
	}
	if _, ok := ic.cachedUser(2); !ok {
		t.Fatal("expected FetchMessages to cache sidecar users")
	}
}

func TestGetChatInfoFetchesFullInlineMembers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/chat/participants" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var request sidecar.ChatParticipantsRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode participants request: %v", err)
		}
		if request.ChatID != 7 {
			t.Fatalf("ChatID = %d, want 7", request.ChatID)
		}
		writeConnectorSidecarResult(t, w, "chat_participants", sidecar.ChatParticipantsPage{
			Participants: []sidecar.ChatParticipantRecord{
				{UserID: 1},
				{UserID: 2},
			},
			Users: []sidecar.UserRecord{{
				UserID:      2,
				DisplayName: stringPtr("Ada Lovelace"),
			}},
		})
	}))
	defer server.Close()

	ic := &InlineClient{
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID("1")}},
		AccountID: "1",
		Sidecar:   sidecar.NewClient(server.URL),
		users:     make(map[int64]sidecar.UserRecord),
	}
	info, err := ic.GetChatInfo(context.Background(), testPortal("7"))
	if err != nil {
		t.Fatalf("GetChatInfo() error = %v", err)
	}
	if info.Members == nil || !info.Members.IsFull {
		t.Fatalf("Members = %#v, want full member list", info.Members)
	}
	if info.Members.TotalMemberCount != 2 || len(info.Members.MemberMap) != 2 {
		t.Fatalf("member count = %d/%d, want 2", info.Members.TotalMemberCount, len(info.Members.MemberMap))
	}
	if info.Type == nil || *info.Type != database.RoomTypeGroupDM {
		t.Fatalf("Type = %#v, want group DM", info.Type)
	}
	if info.Members.OtherUserID != "" {
		t.Fatalf("OtherUserID = %q, want empty for group chat", info.Members.OtherUserID)
	}
	member := info.Members.MemberMap[makeUserID("2")]
	if member.Membership != event.MembershipJoin {
		t.Fatalf("membership = %s, want join", member.Membership)
	}
	if member.UserInfo == nil || member.UserInfo.Name == nil || *member.UserInfo.Name != "Ada Lovelace" {
		t.Fatalf("member user info = %#v, want Ada Lovelace", member.UserInfo)
	}
}

func TestGetChatInfoUsesCachedDMDialogMembers(t *testing.T) {
	calledParticipants := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calledParticipants = true
		t.Fatalf("unexpected sidecar call %s", r.URL.Path)
	}))
	defer server.Close()

	peerUserID := int64(2)
	ic := &InlineClient{
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID("1")}},
		AccountID: "1",
		Sidecar:   sidecar.NewClient(server.URL),
		users:     make(map[int64]sidecar.UserRecord),
		dialogs: map[int64]sidecar.DialogRecord{
			7: {
				ChatID:     7,
				PeerUserID: &peerUserID,
				Title:      stringPtr("Ada Lovelace"),
			},
		},
	}
	ic.cacheUsers([]sidecar.UserRecord{{
		UserID:      2,
		DisplayName: stringPtr("Ada Lovelace"),
	}})

	info, err := ic.GetChatInfo(context.Background(), testPortal("7"))
	if err != nil {
		t.Fatalf("GetChatInfo() error = %v", err)
	}
	if calledParticipants {
		t.Fatal("GetChatInfo called participants endpoint for cached DM dialog")
	}
	if info.Type == nil || *info.Type != database.RoomTypeDM {
		t.Fatalf("Type = %#v, want DM", info.Type)
	}
	if info.Name != nil {
		t.Fatalf("Name = %#v, want nil so bridgev2 uses DM ghost metadata", *info.Name)
	}
	if info.Members == nil || info.Members.OtherUserID != makeUserID("2") {
		t.Fatalf("Members = %#v, want DM members with other user 2", info.Members)
	}
	if _, ok := info.Members.MemberMap[makeUserID("1")]; !ok {
		t.Fatalf("member map = %#v, missing self user", info.Members.MemberMap)
	}
	if member, ok := info.Members.MemberMap[makeUserID("2")]; !ok || member.Membership != event.MembershipJoin {
		t.Fatalf("peer member = %#v, want joined user 2", member)
	}
}

func TestChatInfoForDialogIncludesSelfForGroupWithoutParticipantFetch(t *testing.T) {
	title := "Project"
	ic := &InlineClient{
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID("1")}},
		AccountID: "1",
	}

	info := ic.chatInfoForDialog(context.Background(), sidecar.DialogRecord{
		ChatID: 7,
		Title:  &title,
	}, &title)

	if info.Type == nil || *info.Type != database.RoomTypeGroupDM {
		t.Fatalf("Type = %#v, want group DM", info.Type)
	}
	if info.Name == nil || *info.Name != title {
		t.Fatalf("Name = %#v, want %q", info.Name, title)
	}
	if info.Members == nil {
		t.Fatal("Members = nil, want partial self member list")
	}
	if info.Members.IsFull {
		t.Fatal("Members.IsFull = true, want partial group list")
	}
	if info.Members.OtherUserID != "" {
		t.Fatalf("OtherUserID = %q, want empty for group chat", info.Members.OtherUserID)
	}
	self, ok := info.Members.MemberMap[makeUserID("1")]
	if !ok {
		t.Fatalf("MemberMap = %#v, missing self user", info.Members.MemberMap)
	}
	if !self.IsFromMe || self.Membership != event.MembershipJoin {
		t.Fatalf("self member = %#v, want joined self", self)
	}
}

func TestResolveIdentifierCreatesDM(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/chat/create-dm" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var request sidecar.CreateDMRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode create DM request: %v", err)
		}
		if request.UserID != 42 {
			t.Fatalf("UserID = %d, want 42", request.UserID)
		}
		writeConnectorSidecarResult(t, w, "created_chat", sidecar.CreatedChat{
			ChatID: 99,
			Title:  stringPtr("Ada Lovelace"),
		})
	}))
	defer server.Close()

	ic := &InlineClient{
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID("1")}},
		AccountID: "1",
		Sidecar:   sidecar.NewClient(server.URL),
		users:     make(map[int64]sidecar.UserRecord),
	}
	ic.cacheUsers([]sidecar.UserRecord{{
		UserID:      42,
		DisplayName: stringPtr("Ada Lovelace"),
	}})
	resp, err := ic.ResolveIdentifier(context.Background(), "42", true)
	if err != nil {
		t.Fatalf("ResolveIdentifier() error = %v", err)
	}
	if resp.UserID != makeUserID("42") {
		t.Fatalf("UserID = %q, want 42", resp.UserID)
	}
	if resp.Chat == nil || resp.Chat.PortalKey.ID != makePortalID("99") {
		t.Fatalf("Chat = %#v, want portal 99", resp.Chat)
	}
	if resp.Chat.PortalInfo == nil || resp.Chat.PortalInfo.Members == nil || resp.Chat.PortalInfo.Members.OtherUserID != makeUserID("42") {
		t.Fatalf("PortalInfo = %#v, want DM member info", resp.Chat.PortalInfo)
	}
}

func TestHandleMatrixViewingChatMarksInlineChatRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/read" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var request sidecar.ReadRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode read request: %v", err)
		}
		if request.ChatID != 7 {
			t.Fatalf("ChatID = %d, want 7", request.ChatID)
		}
		if request.MaxMessageID != nil {
			t.Fatalf("MaxMessageID = %#v, want nil", request.MaxMessageID)
		}
		writeConnectorSidecarResult(t, w, "empty", struct{}{})
	}))
	defer server.Close()

	ic := &InlineClient{Sidecar: sidecar.NewClient(server.URL)}
	if err := ic.HandleMatrixViewingChat(context.Background(), &bridgev2.MatrixViewingChat{
		Portal: testPortal("7"),
	}); err != nil {
		t.Fatalf("HandleMatrixViewingChat() error = %v", err)
	}
}

func TestChatManagementCapabilitiesAreScopedToInlineThreads(t *testing.T) {
	ic := &InlineClient{}
	group := testPortal("7")
	features := ic.GetCapabilities(context.Background(), group)
	if !features.MarkAsUnread || !features.DeleteChat || !features.DeleteChatForEveryone {
		t.Fatalf("group capabilities = %#v, want marked unread and delete-chat support", features)
	}
	if name := features.State[event.StateRoomName.Type]; name == nil || name.Level != event.CapLevelFullySupported {
		t.Fatalf("room-name capability = %#v, want fully supported", name)
	}
	for _, action := range []event.MemberAction{
		event.MemberActionInvite,
		event.MemberActionKick,
		event.MemberActionRevokeInvite,
	} {
		if features.MemberActions[action] != event.CapLevelFullySupported {
			t.Fatalf("member action %q = %v, want fully supported", action, features.MemberActions[action])
		}
	}

	dm := testPortal("8")
	dm.OtherUserID = makeUserID("42")
	dmFeatures := ic.GetCapabilities(context.Background(), dm)
	if !dmFeatures.MarkAsUnread {
		t.Fatal("DM marked-unread capability = false, want true")
	}
	if dmFeatures.DeleteChat || len(dmFeatures.State) != 0 || len(dmFeatures.MemberActions) != 0 {
		t.Fatalf("DM chat-management capabilities = %#v, want no thread-only operations", dmFeatures)
	}
}

func TestDialogDisplayNameKeepsEmojiSeparateFromTitle(t *testing.T) {
	title := "General"
	emoji := "✨"
	if got := dialogDisplayName(sidecar.DialogRecord{Title: &title, Emoji: &emoji}); got != "✨ General" {
		t.Fatalf("dialogDisplayName() = %q, want %q", got, "✨ General")
	}
	if got := dialogDisplayName(sidecar.DialogRecord{Title: &title}); got != "General" {
		t.Fatalf("dialogDisplayName() without emoji = %q, want General", got)
	}
}

func TestDialogSyncUsesOneLiveSnapshotThenCachedPages(t *testing.T) {
	liveCalls := 0
	cachedCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rpc/dialogs":
			liveCalls++
			writeConnectorSidecarResult(t, w, "dialogs", sidecar.DialogsPage{NextCursor: "page-2"})
		case "/rpc/state/dialogs":
			cachedCalls++
			writeConnectorSidecarResult(t, w, "dialogs", sidecar.DialogsPage{})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	ic := &InlineClient{Sidecar: sidecar.NewClient(server.URL)}
	if err := ic.syncDialogs(context.Background(), true); err != nil {
		t.Fatalf("live syncDialogs() error = %v", err)
	}
	if liveCalls != 1 || cachedCalls != 1 {
		t.Fatalf("live/cached calls = %d/%d, want 1/1", liveCalls, cachedCalls)
	}
	if err := ic.syncDialogs(context.Background(), false); err != nil {
		t.Fatalf("cached syncDialogs() error = %v", err)
	}
	if liveCalls != 1 || cachedCalls != 2 {
		t.Fatalf("live/cached calls after poll = %d/%d, want 1/2", liveCalls, cachedCalls)
	}
}

func TestReplayReconciliationUsesOneLiveSnapshotThenCachedPages(t *testing.T) {
	liveCalls := 0
	cachedCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rpc/dialogs":
			liveCalls++
			writeConnectorSidecarResult(t, w, "dialogs", sidecar.DialogsPage{NextCursor: "page-2"})
		case "/rpc/state/dialogs":
			cachedCalls++
			writeConnectorSidecarResult(t, w, "dialogs", sidecar.DialogsPage{})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	ic := &InlineClient{Sidecar: sidecar.NewClient(server.URL)}
	dialogs, err := ic.fetchAllDialogsForReconciliation(context.Background())
	if err != nil {
		t.Fatalf("fetchAllDialogsForReconciliation() error = %v", err)
	}
	if len(dialogs) != 0 {
		t.Fatalf("dialogs = %#v, want empty", dialogs)
	}
	if liveCalls != 1 || cachedCalls != 1 {
		t.Fatalf("live/cached calls = %d/%d, want 1/1", liveCalls, cachedCalls)
	}
}

func TestRateLimitRetryDelayUsesTypedHint(t *testing.T) {
	seconds := uint64(17)
	err := &sidecar.Error{Category: "RateLimited", Message: "slow down", RetryAfterSeconds: &seconds}
	if got := rateLimitRetryDelay(err, dialogRateLimitCooldown); got != 17*time.Second {
		t.Fatalf("rateLimitRetryDelay() = %v, want 17s", got)
	}

	seconds = uint64(dialogRateLimitMaxDelay/time.Second) + 1
	if got := rateLimitRetryDelay(err, dialogRateLimitCooldown); got != dialogRateLimitMaxDelay {
		t.Fatalf("rateLimitRetryDelay() capped = %v, want %v", got, dialogRateLimitMaxDelay)
	}
}

func TestMatrixChatManagementCallsSidecar(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rpc/marked-unread":
			var request sidecar.SetMarkedUnreadRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode marked unread request: %v", err)
			}
			if request.ChatID != 7 || !request.Unread {
				t.Fatalf("marked unread request = %#v, want chat 7 unread", request)
			}
		case "/rpc/dialog/notifications":
			var request sidecar.UpdateDialogNotificationsRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode notification request: %v", err)
			}
			if request.ChatID != 7 || request.Mode == nil || *request.Mode != sidecar.DialogNotificationNone {
				t.Fatalf("notification request = %#v, want chat 7 none", request)
			}
		case "/rpc/chat/info":
			var request sidecar.UpdateChatInfoRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode update chat request: %v", err)
			}
			if request.ChatID != 7 || request.Title == nil || *request.Title != "Renamed" {
				t.Fatalf("update chat request = %#v, want chat 7 Renamed", request)
			}
		case "/rpc/chat/participants/add":
			var request sidecar.AddChatParticipantRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode add participant request: %v", err)
			}
			if request.ChatID != 7 || request.UserID != 42 {
				t.Fatalf("add participant request = %#v, want chat 7 user 42", request)
			}
		case "/rpc/chat/participants/remove":
			var request sidecar.RemoveChatParticipantRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode remove participant request: %v", err)
			}
			if request.ChatID != 7 || request.UserID != 42 {
				t.Fatalf("remove participant request = %#v, want chat 7 user 42", request)
			}
		case "/rpc/chat/delete":
			var request sidecar.DeleteChatRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode delete chat request: %v", err)
			}
			if request.ChatID != 7 {
				t.Fatalf("delete chat request = %#v, want chat 7", request)
			}
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		writeConnectorSidecarResult(t, w, "empty", struct{}{})
	}))
	defer server.Close()

	ic := &InlineClient{Sidecar: sidecar.NewClient(server.URL)}
	portal := testPortal("7")
	if err := ic.HandleMarkedUnread(context.Background(), &bridgev2.MatrixMarkedUnread{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MarkedUnreadEventContent]{
			Portal:  portal,
			Content: &event.MarkedUnreadEventContent{Unread: true},
		},
	}); err != nil {
		t.Fatalf("HandleMarkedUnread() error = %v", err)
	}
	if err := ic.HandleMute(context.Background(), &bridgev2.MatrixMute{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.BeeperMuteEventContent]{
			Portal:  portal,
			Content: &event.BeeperMuteEventContent{MutedUntil: -1},
		},
	}); err != nil {
		t.Fatalf("HandleMute() error = %v", err)
	}
	changed, err := ic.HandleMatrixRoomName(context.Background(), &bridgev2.MatrixRoomName{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.RoomNameEventContent]{
			Portal:  portal,
			Content: &event.RoomNameEventContent{Name: " Renamed "},
		},
	})
	if err != nil || !changed {
		t.Fatalf("HandleMatrixRoomName() = %v, %v; want true, nil", changed, err)
	}
	if portal.Name != "Renamed" || !portal.NameSet {
		t.Fatalf("portal name state = %q/%v, want Renamed/true", portal.Name, portal.NameSet)
	}

	ghost := &bridgev2.Ghost{Ghost: &database.Ghost{ID: makeUserID("42")}}
	for _, change := range []bridgev2.MembershipChangeType{bridgev2.Invite, bridgev2.Kick} {
		if _, err := ic.HandleMatrixMembership(context.Background(), &bridgev2.MatrixMembershipChange{
			MatrixRoomMeta: bridgev2.MatrixRoomMeta[*event.MemberEventContent]{
				MatrixEventBase: bridgev2.MatrixEventBase[*event.MemberEventContent]{Portal: portal},
			},
			Target: ghost,
			Type:   change,
		}); err != nil {
			t.Fatalf("HandleMatrixMembership(%v) error = %v", change, err)
		}
	}
	if err := ic.HandleMatrixDeleteChat(context.Background(), &bridgev2.MatrixDeleteChat{
		Portal: portal,
	}); err != nil {
		t.Fatalf("HandleMatrixDeleteChat() error = %v", err)
	}
}

func TestConvertInlineTextMessage(t *testing.T) {
	converted, err := convertInlineMessage(context.Background(), nil, nil, sidecar.MessageRecord{
		ChatID:    7,
		MessageID: 11,
		Content: sidecar.MessageContent{
			Type: "text",
			Text: "hello",
		},
	})
	if err != nil {
		t.Fatalf("convertInlineMessage() error = %v", err)
	}
	if len(converted.Parts) != 1 {
		t.Fatalf("parts = %d, want 1", len(converted.Parts))
	}
	content := converted.Parts[0].Content
	if converted.Parts[0].Type != event.EventMessage || content.MsgType != event.MsgText || content.Body != "hello" {
		t.Fatalf("converted content = %#v", converted.Parts[0])
	}
}

func TestConvertInlineReply(t *testing.T) {
	replyTo := int64(10)
	converted, err := convertInlineMessage(context.Background(), nil, nil, sidecar.MessageRecord{
		ChatID:           7,
		MessageID:        11,
		ReplyToMessageID: &replyTo,
		Content: sidecar.MessageContent{
			Type: "text",
			Text: "reply",
		},
	})
	if err != nil {
		t.Fatalf("convertInlineMessage() error = %v", err)
	}
	if converted.ReplyTo == nil {
		t.Fatal("ReplyTo = nil, want reply target")
	}
	if converted.ReplyTo.MessageID != networkid.MessageID("7/10") {
		t.Fatalf("ReplyTo.MessageID = %q, want 7/10", converted.ReplyTo.MessageID)
	}
}

func TestConvertUnsupportedInlineContentToNotice(t *testing.T) {
	fileName := "clip.mov"
	converted, err := convertedContentFromInline(context.Background(), nil, nil, sidecar.MessageContent{
		Type:     "video",
		FileName: &fileName,
	})
	if err != nil {
		t.Fatalf("convertedContentFromInline() error = %v", err)
	}
	if len(converted.Parts) != 1 {
		t.Fatalf("parts = %d, want 1", len(converted.Parts))
	}
	content := converted.Parts[0].Content
	if content.MsgType != event.MsgNotice {
		t.Fatalf("MsgType = %q, want notice", content.MsgType)
	}
	if content.Body != "[Unsupported Inline video: clip.mov]" {
		t.Fatalf("Body = %q", content.Body)
	}
}

func TestConvertInlineMediaWithoutURLToNotice(t *testing.T) {
	fileName := "clip.mov"
	caption := "watch this"
	converted, err := convertedContentFromInline(context.Background(), nil, nil, sidecar.MessageContent{
		Type:     "media",
		Kind:     "video",
		FileID:   "44",
		FileName: &fileName,
		Caption:  &caption,
	})
	if err != nil {
		t.Fatalf("convertedContentFromInline() error = %v", err)
	}
	if len(converted.Parts) != 1 {
		t.Fatalf("parts = %d, want 1", len(converted.Parts))
	}
	content := converted.Parts[0].Content
	if content.MsgType != event.MsgNotice {
		t.Fatalf("MsgType = %q, want notice", content.MsgType)
	}
	if content.Body != "[Inline media unavailable: clip.mov]\nwatch this" {
		t.Fatalf("Body = %q", content.Body)
	}
}

func TestInlineMediaMessageType(t *testing.T) {
	cases := map[string]event.MessageType{
		"photo":    event.MsgImage,
		"video":    event.MsgVideo,
		"voice":    event.MsgAudio,
		"document": event.MsgFile,
	}
	for kind, want := range cases {
		if got := inlineMediaMessageType(sidecar.MessageContent{Kind: kind}); got != want {
			t.Fatalf("inlineMediaMessageType(%q) = %q, want %q", kind, got, want)
		}
	}
}

func TestEffectiveMatrixMessageTypeInfersMediaFromFileMime(t *testing.T) {
	cases := map[string]struct {
		mime string
		want event.MessageType
		kind string
	}{
		"image": {mime: "image/png", want: event.MsgImage, kind: "photo"},
		"video": {mime: "video/mp4", want: event.MsgVideo, kind: "video"},
		"audio": {mime: "audio/ogg", want: event.MsgAudio, kind: "voice"},
		"file":  {mime: "application/pdf", want: event.MsgFile, kind: "document"},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			content := &event.MessageEventContent{
				MsgType: event.MsgFile,
				Info:    &event.FileInfo{MimeType: tt.mime},
			}
			if got := effectiveMatrixMessageType(content); got != tt.want {
				t.Fatalf("effectiveMatrixMessageType() = %q, want %q", got, tt.want)
			}
			if got := sidecarUploadKindForMatrix(content); got != tt.kind {
				t.Fatalf("sidecarUploadKindForMatrix() = %q, want %q", got, tt.kind)
			}
		})
	}
}

func TestInlineUserDisplayNameFallbacks(t *testing.T) {
	displayName := "  Ada Lovelace  "
	if got := inlineUserDisplayName(sidecar.UserRecord{
		UserID:      42,
		DisplayName: &displayName,
	}); got != "Ada Lovelace" {
		t.Fatalf("display name fallback = %q, want Ada Lovelace", got)
	}

	firstName := "Grace"
	lastName := "Hopper"
	if got := inlineUserDisplayName(sidecar.UserRecord{
		UserID:    43,
		FirstName: &firstName,
		LastName:  &lastName,
	}); got != "Grace Hopper" {
		t.Fatalf("first/last fallback = %q, want Grace Hopper", got)
	}

	username := "  linus  "
	if got := inlineUserDisplayName(sidecar.UserRecord{
		UserID:   44,
		Username: &username,
	}); got != "linus" {
		t.Fatalf("username fallback = %q, want linus", got)
	}

	if got := inlineUserDisplayName(sidecar.UserRecord{UserID: 45}); got != "45" {
		t.Fatalf("id fallback = %q, want 45", got)
	}
}

func TestNeedsHistoryDeliveryUsesBridgeCheckpoint(t *testing.T) {
	last := int64(20)
	synced := int64(20)
	dialog := sidecar.DialogRecord{ChatID: 7, LastMessageID: &last, SyncedThroughMessageID: &synced}

	ic := &InlineClient{}
	if !ic.needsHistoryDelivery(dialog) {
		t.Fatal("needsHistoryDelivery = false before the bridge has delivered the sidecar-cached message")
	}

	ic.rememberHistoryDelivered(7, 20)
	if ic.needsHistoryDelivery(dialog) {
		t.Fatal("needsHistoryDelivery = true after the bridge delivered the latest message")
	}

	last = 21
	if !ic.needsHistoryDelivery(dialog) {
		t.Fatal("needsHistoryDelivery = false for newer dialog message")
	}

	if ic.needsHistoryDelivery(sidecar.DialogRecord{ChatID: 7}) {
		t.Fatal("needsHistoryDelivery = true for dialog without last message")
	}
}

func TestHistoryRequestForDeliveryUsesBridgeCheckpoint(t *testing.T) {
	last := int64(20)
	synced := int64(20)
	request := historyRequestForDelivery(sidecar.DialogRecord{
		ChatID:                 7,
		LastMessageID:          &last,
		SyncedThroughMessageID: &synced,
	}, 11, 5)

	if request.ChatID != 7 {
		t.Fatalf("ChatID = %d, want 7", request.ChatID)
	}
	if request.Limit == nil || *request.Limit != 5 {
		t.Fatalf("Limit = %#v, want 5", request.Limit)
	}
	if request.AfterMessageID == nil || *request.AfterMessageID != 11 {
		t.Fatalf("AfterMessageID = %#v, want bridge checkpoint 11", request.AfterMessageID)
	}
}

func TestRememberHistoryDeliveredDoesNotMoveBackward(t *testing.T) {
	ic := &InlineClient{}
	ic.rememberHistoryDelivered(7, 20)
	ic.rememberHistoryDelivered(7, 19)
	if got := ic.historyCheckpoint(7); got != 20 {
		t.Fatalf("historyCheckpoint = %d, want 20", got)
	}
}

func TestPrioritizedHistoryDialogsPrefersActiveDMsAndNewest(t *testing.T) {
	peer100 := int64(100)
	peer101 := int64(101)
	last5 := int64(5)
	last9 := int64(9)
	last22 := int64(22)
	last30 := int64(30)
	last40 := int64(40)
	ic := &InlineClient{}
	ic.rememberHistoryDelivered(4, 20)
	ic.rememberHistoryDelivered(5, 9)

	got := ic.prioritizedHistoryDialogs([]sidecar.DialogRecord{
		{ChatID: 2, LastMessageID: &last40},
		{ChatID: 5, LastMessageID: &last9},
		{ChatID: 1, PeerUserID: &peer100, LastMessageID: &last5},
		{ChatID: 4, LastMessageID: &last22},
		{ChatID: 3, PeerUserID: &peer101, LastMessageID: &last30},
	})

	want := []int64{4, 3, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("prioritizedHistoryDialogs len = %d, want %d: %#v", len(got), len(want), got)
	}
	for i, dialog := range got {
		if dialog.ChatID != want[i] {
			t.Fatalf("prioritizedHistoryDialogs[%d] = %d, want %d", i, dialog.ChatID, want[i])
		}
	}
}

func TestRememberDialogDoesNotInvalidateDetailsForMessageCheckpointChange(t *testing.T) {
	last := int64(10)
	next := int64(11)
	title := "Project"
	ic := &InlineClient{
		dialogs: map[int64]sidecar.DialogRecord{
			7: {
				ChatID:        7,
				Title:         &title,
				LastMessageID: &last,
			},
		},
		details: map[int64]struct{}{7: {}},
	}

	changed := ic.rememberDialog(sidecar.DialogRecord{
		ChatID:        7,
		Title:         &title,
		LastMessageID: &next,
	})
	if !changed {
		t.Fatal("rememberDialog changed = false, want true for checkpoint update")
	}
	if ic.needsDialogDetailsSync(7) {
		t.Fatal("details were invalidated by a message checkpoint change")
	}

	peerUserID := int64(2)
	ic.rememberDialog(sidecar.DialogRecord{
		ChatID:     7,
		Title:      &title,
		PeerUserID: &peerUserID,
	})
	if !ic.needsDialogDetailsSync(7) {
		t.Fatal("details were not invalidated when dialog peer identity changed")
	}
}

func TestIsRateLimitedErrorDetectsInlineFloodResponses(t *testing.T) {
	cases := []error{
		&sidecar.Error{Category: "Network", Message: "websocket error: HTTP error: 420 <unknown status code>"},
		&sidecar.Error{Category: "FLOOD", Message: "too many requests"},
		errors.New("Inline sidecar GET /rpc/dialogs returned HTTP 420"),
	}
	for _, err := range cases {
		if !isRateLimitedError(err) {
			t.Fatalf("isRateLimitedError(%v) = false, want true", err)
		}
	}
	if isRateLimitedError(errors.New("temporary network disconnect")) {
		t.Fatal("isRateLimitedError(non-rate-limit) = true, want false")
	}
}

func TestSyncDeferredDialogWorkStopsOnRateLimitedHistory(t *testing.T) {
	var historyCalls int
	var participantsCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rpc/history":
			historyCalls++
			writeConnectorSidecarError(t, w, "Network", "websocket error: HTTP error: 420 <unknown status code>")
		case "/rpc/chat/participants":
			participantsCalls++
			t.Fatalf("participants should not be fetched after a rate-limited history request")
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	last := int64(20)
	ic := &InlineClient{Sidecar: sidecar.NewClient(server.URL)}
	err := ic.syncDeferredDialogWork(context.Background(), []sidecar.DialogRecord{{
		ChatID:        7,
		LastMessageID: &last,
	}}, dialogSyncRPCBudget)
	if !isRateLimitedError(err) {
		t.Fatalf("syncDeferredDialogWork() error = %v, want rate-limit error", err)
	}
	if historyCalls != 1 {
		t.Fatalf("history calls = %d, want 1", historyCalls)
	}
	if participantsCalls != 0 {
		t.Fatalf("participants calls = %d, want 0", participantsCalls)
	}
}

func TestGetUserInfoUsesCachedInlineUser(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/avatar.jpg" {
			t.Fatalf("path = %s, want /avatar.jpg", r.URL.Path)
		}
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write([]byte("avatar bytes"))
	}))
	defer server.Close()

	isBot := true
	ic := &InlineClient{}
	ic.cacheUsers([]sidecar.UserRecord{{
		UserID:      42,
		DisplayName: stringPtr("Ada Lovelace"),
		AvatarURL:   stringPtr(server.URL + "/avatar.jpg?token=secret"),
		IsBot:       &isBot,
	}})

	info, err := ic.GetUserInfo(context.Background(), &bridgev2.Ghost{
		Ghost: &database.Ghost{ID: makeUserID("42")},
	})
	if err != nil {
		t.Fatalf("GetUserInfo() error = %v", err)
	}
	if info.Name == nil || *info.Name != "Ada Lovelace" {
		t.Fatalf("Name = %#v, want Ada Lovelace", info.Name)
	}
	if info.IsBot == nil || *info.IsBot != true {
		t.Fatalf("IsBot = %#v, want true", info.IsBot)
	}
	if info.Avatar == nil {
		t.Fatal("Avatar = nil, want avatar")
	}
	if strings.Contains(string(info.Avatar.ID), "secret") || strings.Contains(string(info.Avatar.ID), server.URL) {
		t.Fatalf("Avatar.ID leaks source URL: %q", info.Avatar.ID)
	}
	data, err := info.Avatar.Get(context.Background())
	if err != nil {
		t.Fatalf("Avatar.Get() error = %v", err)
	}
	if string(data) != "avatar bytes" {
		t.Fatalf("Avatar.Get() = %q, want avatar bytes", data)
	}
}

func TestDownloadInlineMedia(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		_, _ = w.Write([]byte("media bytes"))
	}))
	defer server.Close()

	data, err := downloadInlineMedia(context.Background(), server.URL+"/file.bin")
	if err != nil {
		t.Fatalf("downloadInlineMedia() error = %v", err)
	}
	if string(data) != "media bytes" {
		t.Fatalf("data = %q", string(data))
	}
}

func TestDownloadInlineMediaRejectsUnsupportedScheme(t *testing.T) {
	if _, err := downloadInlineMedia(context.Background(), "file:///tmp/media.bin"); err == nil {
		t.Fatal("downloadInlineMedia() error = nil, want unsupported scheme error")
	}
}

func TestTransactionIDForMessage(t *testing.T) {
	got := transactionIDForMessage(sidecar.MessageRecord{
		Transaction: &sidecar.TransactionIdentity{
			TransactionID: "txn-1",
		},
	})
	if got != networkid.TransactionID("txn-1") {
		t.Fatalf("transactionIDForMessage = %q, want txn-1", got)
	}
}

func TestMatrixSendResponseRejectsTerminalOrUnknownTransactionState(t *testing.T) {
	ic := &InlineClient{}
	for name, mutation := range map[string]*sidecar.MessageMutation{
		"uncertain": {
			Transaction: sidecar.TransactionIdentity{TransactionID: "txn-uncertain"},
			State:       sidecar.TransactionAcked,
		},
		"failed": {
			Transaction: sidecar.TransactionIdentity{TransactionID: "txn-failed"},
			State:       sidecar.TransactionFailed,
			Failure:     &sidecar.Failure{Category: "PermissionDenied", Message: "not allowed"},
		},
		"completed without id": {
			Transaction: sidecar.TransactionIdentity{TransactionID: "txn-completed"},
			State:       sidecar.TransactionCompleted,
		},
		"unknown": {
			Transaction: sidecar.TransactionIdentity{TransactionID: "txn-unknown"},
			State:       sidecar.TransactionState("FutureState"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ic.matrixSendResponse(nil, 7, mutation); err == nil {
				t.Fatal("matrixSendResponse() error = nil, want explicit failure")
			}
		})
	}
}

func TestSidecarErrorCertaintyOnlyMarksPermanentFailures(t *testing.T) {
	if sidecarErrorIsCertain(&sidecar.Error{Category: "Timeout", Message: "timed out"}) {
		t.Fatal("Timeout should be uncertain and retryable")
	}
	if !sidecarErrorIsCertain(&sidecar.Error{Category: "PermissionDenied", Message: "denied"}) {
		t.Fatal("PermissionDenied should be certain")
	}
	if sidecarErrorIsCertain(errors.New("connection reset")) {
		t.Fatal("transport error should be uncertain")
	}
}

func TestDuplicateSidecarReplayOnlyReacknowledgesContiguousCursor(t *testing.T) {
	acked := make(chan sidecar.EventAckRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request sidecar.EventAckRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode ack request: %v", err)
		}
		acked <- request
		if err := json.NewEncoder(w).Encode(sidecar.EventAckResponse{AcknowledgedSequence: request.Sequence}); err != nil {
			t.Fatalf("encode ack response: %v", err)
		}
	}))
	defer server.Close()
	sequence := uint64(9)
	ic := &InlineClient{
		Sidecar:             sidecar.NewClient(server.URL),
		storeNamespace:      "team",
		lastSidecarSequence: 10,
	}

	if err := ic.handleSequencedSidecarEvent(t.Context(), &sidecar.EventEnvelope{
		SessionNamespace: "team",
		Sequence:         &sequence,
		Reliability:      sidecar.EventLossless,
	}); err != nil {
		t.Fatalf("handleSequencedSidecarEvent() error = %v", err)
	}
	request := <-acked
	if request.SessionNamespace != "team" || request.Sequence != 10 {
		t.Fatalf("ack = %#v, want team cursor 10", request)
	}
}

func testPortal(chatID string) *bridgev2.Portal {
	return &bridgev2.Portal{
		Portal: &database.Portal{
			PortalKey: networkid.PortalKey{ID: makePortalID(chatID)},
		},
	}
}

func writeConnectorSidecarResult(t *testing.T, w http.ResponseWriter, resultType string, data any) {
	t.Helper()
	resultData := mustConnectorJSON(t, data)
	responseData := mustConnectorJSON(t, sidecar.Result{
		Type: resultType,
		Data: resultData,
	})
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(sidecar.Response{
		ProtocolVersion: sidecar.ProtocolVersion,
		ID:              "test-1",
		Outcome: sidecar.ResponseOutcome{
			Status: "ok",
			Data:   responseData,
		},
	}); err != nil {
		t.Fatalf("encode sidecar response: %v", err)
	}
}

func writeConnectorSidecarError(t *testing.T, w http.ResponseWriter, category, message string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(sidecar.Response{
		ProtocolVersion: sidecar.ProtocolVersion,
		ID:              "test-1",
		Outcome: sidecar.ResponseOutcome{
			Status: "error",
			Data: mustConnectorJSON(t, sidecar.Error{
				Category: category,
				Message:  message,
			}),
		},
	}); err != nil {
		t.Fatalf("encode sidecar response: %v", err)
	}
}

func mustConnectorJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return data
}

func stringPtr(value string) *string {
	return &value
}
