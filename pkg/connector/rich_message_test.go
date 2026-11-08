package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"hegel.dev/go/hegel"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Losing the sidecar entities at the bridge boundary must lose this formatting.
func TestRichMessageIncomingEntities(t *testing.T) {
	data := sidecar.MessageRecord{ChatID: 7, MessageID: 11, Content: sidecar.MessageContent{Type: "text", Text: "hi 😀"}, Metadata: sidecar.MessageMetadata{Entities: []sidecar.MessageEntityRecord{{Kind: "TYPE_BOLD", Offset: 3, Length: 2}}}}
	got, err := convertInlineMessage(t.Context(), nil, nil, data)
	if err != nil {
		t.Fatal(err)
	}
	c := got.Parts[0].Content
	if c.Body != "hi 😀" || c.FormattedBody != "hi <strong>😀</strong>" || c.Format != event.FormatHTML {
		t.Fatalf("lost entities at bridge boundary: %+v", c)
	}
}

type richMatrixConnector struct{ bridgev2.MatrixConnector }

type richGhostIntent struct {
	bridgev2.MatrixAPI
	user networkid.UserID
}

func (r richGhostIntent) GetMXID() id.UserID { return id.UserID("@inline_" + string(r.user) + ":test") }
func (richMatrixConnector) GhostIntent(user networkid.UserID) bridgev2.MatrixAPI {
	return richGhostIntent{user: user}
}
func (richMatrixConnector) ParseGhostMXID(user id.UserID) (networkid.UserID, bool) {
	if user == "@inline_42:test" {
		return "42", true
	}
	return "", false
}
func richClient() *InlineClient {
	return &InlineClient{AccountID: "1", Sidecar: &sidecar.Client{SessionNamespace: "a"}, users: map[int64]sidecar.UserRecord{42: {UserID: 42}}, UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login-a"}, Bridge: &bridgev2.Bridge{Matrix: richMatrixConnector{}}, User: &bridgev2.User{User: &database.User{MXID: "@self:test"}}}}
}
func TestRichMessageLiveMentionsAreAccountScoped(t *testing.T) {
	ic := richClient()
	user := int64(42)
	data := sidecar.MessageRecord{Content: sidecar.MessageContent{Type: "text", Text: "Ada"}, Metadata: sidecar.MessageMetadata{Entities: []sidecar.MessageEntityRecord{{Kind: "TYPE_MENTION", Offset: 0, Length: 3, UserID: &user}}}}
	for _, allowed := range []bool{true, false} {
		if !allowed {
			ic.users = nil
		}
		got, err := ic.remoteMessageEvent(data).ConvertMessageFunc(t.Context(), nil, nil, data)
		if err != nil {
			t.Fatal(err)
		}
		mentions := got.Parts[0].Content.Mentions
		if allowed && (mentions == nil || len(mentions.UserIDs) != 1 || mentions.UserIDs[0] != "@inline_42:test") {
			t.Fatalf("lost verified mention: %+v", mentions)
		}
		if !allowed && mentions != nil && len(mentions.UserIDs) > 0 {
			t.Fatalf("cross-account mention: %+v", mentions)
		}
	}
}

func TestRichMessageOutgoingEntitiesReachSource(t *testing.T) {
	calls := 0
	s := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Text          string                        `json:"text"`
			Entities      []sidecar.MessageEntityRecord `json:"entities"`
			ParseMarkdown bool                          `json:"parse_markdown"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		calls++
		if body.Text != "😀" || body.ParseMarkdown || len(body.Entities) != 1 || body.Entities[0].Kind != "TYPE_BOLD" || body.Entities[0].Offset != 0 || body.Entities[0].Length != 2 {
			t.Errorf("lost native UTF16 entities: %+v", body)
		}
		switch r.URL.Path {
		case "/rpc/send":
			mid := int64(11)
			writeConnectorSidecarResult(t, w, "message", sidecar.MessageMutation{State: sidecar.TransactionCompleted, MessageID: &mid})
		case "/rpc/edit":
			writeConnectorSidecarResult(t, w, "empty", map[string]bool{"ok": true})
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
		}
	}))
	defer s.Close()
	ic := richClient()
	ic.Sidecar = sidecar.NewClient(s.URL)
	c := &event.MessageEventContent{MsgType: event.MsgText, Body: "😀", Format: event.FormatHTML, FormattedBody: "<b>😀</b>"}
	if _, err := ic.HandleMatrixMessage(t.Context(), &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: testPortal("7"), Content: c}}); err != nil {
		t.Fatal(err)
	}
	if err := ic.HandleMatrixEdit(t.Context(), &bridgev2.MatrixEdit{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: testPortal("7"), Content: c}, EditTarget: &database.Message{ID: "7/11", Room: testPortal("7").PortalKey}}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("send/edit calls %d", calls)
	}
}

func TestRichMessageReplyFallbackPreservesReplyTarget(t *testing.T) {
	calls := 0
	s := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request sidecar.SendTextRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		calls++
		if r.URL.Path != "/rpc/send" || request.Text != "reply 😀" || request.ReplyToMessageID == nil || *request.ReplyToMessageID != 10 {
			t.Errorf("reply context or body lost: path=%s request=%+v", r.URL.Path, request)
		}
		if len(request.Entities) != 1 || request.Entities[0].Kind != "TYPE_BOLD" || request.Entities[0].Offset != 6 || request.Entities[0].Length != 2 {
			t.Errorf("reply formatting lost: %+v", request.Entities)
		}
		mid := int64(11)
		writeConnectorSidecarResult(t, w, "message", sidecar.MessageMutation{State: sidecar.TransactionCompleted, MessageID: &mid})
	}))
	defer s.Close()
	ic := richClient()
	ic.Sidecar = sidecar.NewClient(s.URL)
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: "> quoted\n\nreply 😀", Format: event.FormatHTML, FormattedBody: "<mx-reply><blockquote>quoted</blockquote></mx-reply>reply <b>😀</b>"}
	portal := testPortal("7")
	msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: portal, Content: content}, ReplyTo: &database.Message{ID: "7/10", Room: portal.PortalKey}}
	if _, err := ic.HandleMatrixMessage(t.Context(), msg); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("send calls %d", calls)
	}
}

func TestRichMessageOutgoingMediaReplacementRejected(t *testing.T) {
	calls := 0
	s := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		writeConnectorSidecarResult(t, w, "empty", struct{}{})
	}))
	defer s.Close()
	ic := richClient()
	ic.Sidecar = sidecar.NewClient(s.URL)
	old := &event.MessageEventContent{MsgType: event.MsgImage, URL: "mxc://test/old", FileName: "image.jpg"}
	for _, target := range []*database.Message{{ID: "7/11", Metadata: &MessageMetadata{InlineMedia: old}}, {ID: "7/11"}} {
		target.Room = testPortal("7").PortalKey
		content := &event.MessageEventContent{MsgType: event.MsgImage, Body: "replacement", URL: "mxc://test/new", FileName: "image.jpg"}
		err := ic.HandleMatrixEdit(t.Context(), &bridgev2.MatrixEdit{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: testPortal("7"), Content: content}, EditTarget: target})
		if err == nil || calls != 0 {
			t.Fatalf("replacement or unproven legacy edit silently succeeded: %v calls%d", err, calls)
		}
	}
}

func TestRichMessageOutgoingMediaAckKeepsCaptionAndEchoIdentity(t *testing.T) {
	ic := richClient()
	content := &event.MessageEventContent{MsgType: event.MsgImage, Body: "before", FileName: "image.jpg", URL: "mxc://test/original"}
	mid := int64(11)
	response, err := ic.matrixSendResponse(&bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Content: content}}, 7, &sidecar.MessageMutation{State: sidecar.TransactionCompleted, MessageID: &mid})
	if err != nil {
		t.Fatal(err)
	}
	if !matrixCaptionEditAllowed(response.DB, content) {
		t.Fatal("completed media acknowledgement discarded caption identity")
	}
	// The first source echo binds the source media ID to that exact completed
	// message; it must reuse the already uploaded Matrix object.
	data := sidecar.MessageRecord{ChatID: 7, MessageID: 11, Content: sidecar.MessageContent{Type: "media", Kind: "photo", FileID: "native-file", URL: "http://unused.test/media", Caption: stringPtr("after")}}
	intent := &richMediaIntent{}
	edit, err := convertInlineMessageEdit(t.Context(), testPortal("7"), intent, []*database.Message{response.DB}, data)
	if err != nil || intent.uploads != 0 || len(edit.ModifiedParts) != 1 || edit.ModifiedParts[0].Content.URL != "mxc://test/original" || edit.ModifiedParts[0].Content.Body != "after" {
		t.Fatalf("source echo retransferred/lost original media: %+v %v uploads%d", edit, err, intent.uploads)
	}
}

func TestRichMessageOutgoingCaptionEditUsesEditRPC(t *testing.T) {
	calls := 0
	s := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/edit" {
			t.Errorf("caption used transfer route %s", r.URL.Path)
		}
		var body sidecar.EditMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.Text != "new caption" {
			t.Errorf("caption became %q", body.Text)
		}
		calls++
		writeConnectorSidecarResult(t, w, "empty", map[string]bool{"ok": true})
	}))
	defer s.Close()
	ic := richClient()
	ic.Sidecar = sidecar.NewClient(s.URL)
	c := &event.MessageEventContent{MsgType: event.MsgImage, Body: "new caption", FileName: "image.jpg", URL: "mxc://test/existing"}
	if err := ic.HandleMatrixEdit(t.Context(), &bridgev2.MatrixEdit{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: testPortal("7"), Content: c}, EditTarget: &database.Message{ID: "7/11", Room: testPortal("7").PortalKey, Metadata: &MessageMetadata{InlineMedia: &event.MessageEventContent{MsgType: event.MsgImage, URL: "mxc://test/existing", FileName: "image.jpg"}}}}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("caption edit calls %d", calls)
	}
}

func TestRichMessageRevisionlessReplayRemainsFingerprintBased(t *testing.T) {
	data := sidecar.MessageRecord{Content: sidecar.MessageContent{Type: "text", Text: "before"}}
	meta := &MessageMetadata{InlineFingerprint: inlineMessageFingerprint(data)}
	data.Content.Text = "after"
	got, err := upsertExistingInlineMessage(t.Context(), testPortal("7"), nil, []*database.Message{{Metadata: meta}}, data)
	if err != nil || len(got.SubEvents) != 1 {
		t.Fatalf("revisionless change lost: %+v %v", got, err)
	}
}

func TestRichMessageFingerprintIncludesEntitySemantics(t *testing.T) {
	a := sidecar.MessageRecord{Content: sidecar.MessageContent{Type: "text", Text: "hello"}}
	b := a
	b.Metadata.Entities = []sidecar.MessageEntityRecord{{Kind: "TYPE_BOLD", Offset: 0, Length: 5}}
	if inlineMessageFingerprint(a) == inlineMessageFingerprint(b) {
		t.Fatal("entity-only edit collapsed into replay")
	}
}

// A lower or equal revision can never emit an edit, even with a later timestamp.
func TestRichMessageRevisionReplayProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		accepted := hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, math.MaxInt64))
		incoming := hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, accepted))
		meta := &MessageMetadata{}
		raw, _ := json.Marshal(map[string]any{"inline_revision": accepted})
		if err := json.Unmarshal(raw, meta); err != nil {
			ht.Fatal(err)
		}
		data := sidecar.MessageRecord{ChatID: 7, MessageID: 11, Timestamp: 999, Content: sidecar.MessageContent{Type: "text", Text: "stale"}, Metadata: sidecar.MessageMetadata{Revision: &incoming}}
		got, err := upsertExistingInlineMessage(t.Context(), testPortal("7"), nil, []*database.Message{{ID: "7/11", Metadata: meta}}, data)
		if err != nil || len(got.SubEvents) != 0 || got.ContinueMessageHandling {
			ht.Fatalf("revision %d replaced accepted %d: %+v %v", incoming, accepted, got, err)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

type richMediaIntent struct {
	bridgev2.MatrixAPI
	uploads int
}

func (m *richMediaIntent) UploadMedia(context.Context, id.RoomID, []byte, string, string) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	m.uploads++
	return "mxc://test/new-upload", nil, nil
}

func TestRichMessageCaptionEditReusesPersistedMatrixMedia(t *testing.T) {
	downloads := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { downloads++; w.Write([]byte("image bytes")) }))
	defer s.Close()
	data := sidecar.MessageRecord{ChatID: 7, MessageID: 11, Content: sidecar.MessageContent{Type: "media", Kind: "photo", FileID: "file-1", URL: s.URL, Caption: stringPtr("before")}}
	intent := &richMediaIntent{}
	original, err := convertInlineMessage(t.Context(), testPortal("7"), intent, data)
	if err != nil {
		t.Fatal(err)
	}
	if intent.uploads != 1 || downloads != 1 {
		t.Fatal("initial media did not transfer")
	}
	// Persist/reopen the JSON metadata; in-memory caching alone is insufficient.
	raw, err := json.Marshal(original.Parts[0].DBMetadata)
	if err != nil {
		t.Fatal(err)
	}
	meta := &MessageMetadata{}
	if err := json.Unmarshal(raw, meta); err != nil {
		t.Fatal(err)
	}
	data.Content.Caption = stringPtr("after")
	data.Metadata.Entities = []sidecar.MessageEntityRecord{{Kind: "TYPE_BOLD", Offset: 0, Length: 5}}
	edit, err := convertInlineMessageEdit(t.Context(), testPortal("7"), intent, []*database.Message{{ID: "7/11", Metadata: meta}}, data)
	if err != nil {
		t.Fatal(err)
	}
	if intent.uploads != 1 || downloads != 1 {
		t.Fatalf("caption retransferred media: uploads=%d downloads=%d", intent.uploads, downloads)
	}
	c := edit.ModifiedParts[0].Content
	if c.URL != "mxc://test/new-upload" || c.Body != "after" || c.FormattedBody != "<strong>after</strong>" {
		t.Fatalf("caption lost media or entities: %+v", c)
	}
}

func TestRichMessageFrameworkCaptionEditTargetIsolation(t *testing.T) {
	for _, mode := range []string{"cross-receiver", "cross-chat", "same-room"} {
		t.Run(mode, func(t *testing.T) {
			ic := fixtureHistoryClient(t, filepath.Join(t.TempDir(), "caption.db"))
			ic.AccountID = "1"
			ic.UserLogin.Client = ic
			br := ic.UserLogin.Bridge
			portal, err := br.GetPortalByKey(t.Context(), ic.portalKeyForChat(7))
			if err != nil {
				t.Fatal(err)
			}
			portal.MXID = "!current:test"
			if err = portal.Save(t.Context()); err != nil {
				t.Fatal(err)
			}
			target := portal.PortalKey
			targetID := networkid.MessageID("7/9")
			if mode == "cross-receiver" {
				target.Receiver = "other-login"
			}
			if mode == "cross-chat" {
				target.ID = "8"
				targetID = "8/9"
			}
			if mode != "same-room" {
				if err = br.DB.Portal.Insert(t.Context(), &database.Portal{PortalKey: target, MXID: "!other:test", Metadata: &PortalMetadata{}}); err != nil {
					t.Fatal(err)
				}
			}
			old := &event.MessageEventContent{MsgType: event.MsgImage, URL: "mxc://test/media", FileName: "image.jpg", Info: &event.FileInfo{MimeType: "image/jpeg"}}
			if err = br.DB.Message.Insert(t.Context(), &database.Message{ID: targetID, Room: target, MXID: "$target", Metadata: &MessageMetadata{InlineMedia: old}}); err != nil {
				t.Fatal(err)
			}
			calls := 0
			server := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != "/rpc/edit" {
					t.Errorf("unexpected route %s", r.URL.Path)
				}
				var req sidecar.EditMessageRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				if mode == "same-room" && (req.ChatID != 7 || req.MessageID != 9 || req.Text != "caption") {
					t.Errorf("caption changed %+v", req)
				}
				json.NewEncoder(w).Encode(map[string]any{"protocol_version": sidecar.ProtocolVersion, "outcome": map[string]any{"status": "error", "data": sidecar.Error{Category: "permission_denied", Message: "fixture stops after mutation admission"}}})
			}))
			defer server.Close()
			ic.Sidecar = sidecar.NewClient(server.URL).WithSessionNamespace("fixture")
			wire, err := json.Marshal(old)
			if err != nil {
				t.Fatal(err)
			}
			var fresh event.MessageEventContent
			if err = json.Unmarshal(wire, &fresh); err != nil {
				t.Fatal(err)
			}
			fresh.Body = "caption"
			outer := fresh
			outer.NewContent = &fresh
			outer.RelatesTo = &event.RelatesTo{Type: event.RelReplace, EventID: "$target"}
			result := portal.Internal().HandleMatrixMessage(t.Context(), ic.UserLogin, nil, &event.Event{ID: "$edit", RoomID: portal.MXID, Type: event.EventMessage, Content: event.Content{Parsed: &outer}})
			want := 0
			if mode == "same-room" {
				want = 1
			}
			if calls != want || result.Error == nil {
				t.Fatalf("mode%s admitted%d want%d: %+v", mode, calls, want, result)
			}
		})
	}
}

func TestRichMessageRevisionDirectionsAndPersistedAcceptance(t *testing.T) {
	ic := fixtureHistoryClient(t, filepath.Join(t.TempDir(), "revisions.db"))
	key := ic.portalKeyForChat(7)
	if err := ic.UserLogin.Bridge.DB.Portal.Insert(t.Context(), &database.Portal{PortalKey: key, Metadata: &PortalMetadata{}}); err != nil {
		t.Fatal(err)
	}
	seq := int64(0)
	hegel.Test(t, func(ht *hegel.T) {
		accepted := hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, math.MaxInt64))
		incoming := hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, math.MaxInt64))
		oldRevision := hegel.Draw(ht, hegel.Booleans())
		newRevision := hegel.Draw(ht, hegel.Booleans())
		oldDate := hegel.Draw(ht, hegel.Booleans())
		newDate := hegel.Draw(ht, hegel.Booleans())
		meta := &MessageMetadata{InlineFingerprint: "before"}
		if oldRevision {
			meta.InlineRevision = &accepted
		}
		if oldDate {
			meta.InlineEditTime = &accepted
		}
		seq++
		data := sidecar.MessageRecord{ChatID: 7, MessageID: seq, Content: sidecar.MessageContent{Type: "text", Text: "after"}, Metadata: sidecar.MessageMetadata{Entities: []sidecar.MessageEntityRecord{{Kind: "TYPE_BOLD", Offset: 0, Length: 5}}}}
		if newRevision {
			data.Metadata.Revision = &incoming
		}
		if newDate {
			data.Metadata.EditTimestamp = &incoming
		}
		row := &database.Message{ID: makeMessageID(7, seq), MXID: id.EventID(fmt.Sprintf("$revision-%d", seq)), Room: key, Metadata: meta}
		db := ic.UserLogin.Bridge.DB.Message
		if err := db.Insert(t.Context(), row); err != nil {
			ht.Fatal(err)
		}
		row, err := db.GetFirstPartByID(t.Context(), key.Receiver, row.ID)
		if err != nil {
			ht.Fatal(err)
		}
		stale := false
		if oldRevision {
			stale = !newRevision || incoming <= accepted
		} else if !newRevision && oldDate {
			stale = !newDate || incoming <= accepted
		}
		edit, err := convertInlineMessageEdit(t.Context(), testPortal("7"), nil, []*database.Message{row}, data)
		if err != nil {
			ht.Fatal(err)
		}
		if stale {
			if len(edit.ModifiedParts) != 0 {
				ht.Fatal("stale revision emitted content")
			}
			return
		}
		if len(edit.ModifiedParts) != 1 || edit.ModifiedParts[0].Content.Body != "after" || edit.ModifiedParts[0].Content.FormattedBody != "<strong>after</strong>" {
			ht.Fatalf("new revision dropped: %+v", edit)
		}
		// A failed delivery does not save converted metadata. Reload the original
		// row and require the same edit on retry before simulating successful save.
		retry, err := db.GetFirstPartByID(t.Context(), key.Receiver, row.ID)
		if err != nil {
			ht.Fatal(err)
		}
		repeat, err := convertInlineMessageEdit(t.Context(), testPortal("7"), nil, []*database.Message{retry}, data)
		if err != nil || len(repeat.ModifiedParts) != 1 {
			ht.Fatalf("undelivered conversion consumed revision: %+v %v", repeat, err)
		}
		row.Metadata = edit.ModifiedParts[0].Part.Metadata
		if err = db.Update(t.Context(), row); err != nil {
			ht.Fatal(err)
		}
		saved, err := db.GetFirstPartByID(t.Context(), key.Receiver, row.ID)
		if err != nil {
			ht.Fatal(err)
		}
		replay, err := upsertExistingInlineMessage(t.Context(), testPortal("7"), nil, []*database.Message{saved}, data)
		if err != nil || len(replay.SubEvents) != 0 || replay.ContinueMessageHandling {
			ht.Fatalf("saved revision replayed: %+v %v", replay, err)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}
