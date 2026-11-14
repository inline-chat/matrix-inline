package connector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/crypto/attachment"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func fidelityInt(v int64) *int64      { return &v }
func fidelityString(v string) *string { return &v }

func TestMatrixRichTextUsesUTF16AndVerifiedMentions(t *testing.T) {
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: "fallback", Format: event.FormatHTML, FormattedBody: `<p>😀 <strong>bold <em>and italic</em></strong> <code>*literal*</code> <a href="https://example.com/?a=1&amp;b=2">link</a> <a href="https://matrix.to/#/@known:test">Known</a> <a href="javascript:alert(1)">unsafe</a></p><pre><code class="language-go">x &lt; y</code></pre>`}
	text, entities, err := matrixTextToInline(content, false, func(mxid id.UserID) *int64 {
		if mxid == "@known:test" {
			return fidelityInt(42)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(text, "😀 bold and italic *literal* link Known unsafe\n") {
		t.Fatalf("text = %q", text)
	}
	kinds := map[string]sidecar.MessageEntityRecord{}
	for _, entity := range entities {
		kinds[entity.Kind] = entity
	}
	if kinds["TYPE_BOLD"].Offset != 3 || kinds["TYPE_BOLD"].Length != 15 {
		t.Fatalf("bold range = %+v", kinds["TYPE_BOLD"])
	}
	if kinds["TYPE_ITALIC"].Offset != 8 {
		t.Fatalf("italic = %+v", kinds["TYPE_ITALIC"])
	}
	if kinds["TYPE_MENTION"].UserID == nil || *kinds["TYPE_MENTION"].UserID != 42 {
		t.Fatalf("mention = %+v", kinds["TYPE_MENTION"])
	}
	if kinds["TYPE_PRE"].Value == nil || *kinds["TYPE_PRE"].Value != "go" {
		t.Fatalf("pre = %+v", kinds["TYPE_PRE"])
	}
	if len(entities) != 6 {
		t.Fatalf("entities = %#v", entities)
	}
}

func TestInlineRichTextEscapesCrossingsAndMalformedRanges(t *testing.T) {
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: "😀 <ab>&"}
	applyInlineEntities(content, content.Body, []sidecar.MessageEntityRecord{
		{Kind: "TYPE_BOLD", Offset: 0, Length: 5},
		{Kind: "TYPE_ITALIC", Offset: 3, Length: 4}, // crosses bold
		{Kind: "TYPE_CODE", Offset: 1, Length: 1},   // half surrogate, ignored
		{Kind: "TYPE_PRE", Offset: -1, Length: 2},
		{Kind: "TYPE_BOLD", Offset: 0, Length: 1 << 62},
		{Kind: "TYPE_TEXT_URL", Offset: 3, Length: 2, Value: fidelityString("javascript:alert(1)")},
	}, nil)
	if content.Format != event.FormatHTML || strings.Contains(content.FormattedBody, "<ab>") || strings.Contains(content.FormattedBody, "javascript") || strings.Contains(content.FormattedBody, "<code>") {
		t.Fatalf("HTML = %q", content.FormattedBody)
	}
	roundTrip, entities, err := matrixTextToInline(content, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if roundTrip != content.Body {
		t.Fatalf("round trip = %q, want %q", roundTrip, content.Body)
	}
	if len(entities) < 2 {
		t.Fatalf("lost formatting: %+v", entities)
	}
	if content.Mentions == nil || content.Mentions.Room {
		t.Fatalf("mentions = %+v", content.Mentions)
	}
}

func TestInlineMentionsDoNotEscalateBroadcastOrUnknownIdentities(t *testing.T) {
	content := &event.MessageEventContent{Body: "Alice @room Unknown"}
	applyInlineEntities(content, content.Body, []sidecar.MessageEntityRecord{
		{Kind: "TYPE_MENTION", Offset: 0, Length: 5, UserID: fidelityInt(42)},
		{Kind: "TYPE_MENTION", Offset: 6, Length: 5, GroupID: fidelityInt(1)},
		{Kind: "TYPE_MENTION", Offset: 12, Length: 7, UserID: fidelityInt(99)},
	}, func(userID int64) id.UserID {
		if userID == 42 {
			return "@alice:test"
		}
		return ""
	})
	if content.Mentions.Room || len(content.Mentions.UserIDs) != 1 || content.Mentions.UserIDs[0] != "@alice:test" {
		t.Fatalf("mentions = %+v", content.Mentions)
	}
	if strings.Count(content.FormattedBody, "<a ") != 1 || !strings.Contains(content.FormattedBody, "Unknown") {
		t.Fatalf("HTML = %q", content.FormattedBody)
	}
}

func TestFormattingOnlyEditAndStaleSourceRevision(t *testing.T) {
	portal := testPortal("7")
	original := sidecar.MessageRecord{ChatID: 7, MessageID: 11, SenderID: 42, Content: sidecar.MessageContent{Type: "text", Text: "same"}, Metadata: sidecar.MessageMetadata{Revision: fidelityInt(3), EditTimestamp: fidelityInt(100)}}
	converted, err := convertInlineMessage(t.Context(), portal, nil, original)
	if err != nil {
		t.Fatal(err)
	}
	row := &database.Message{ID: makeMessageID(7, 11), Room: portal.PortalKey, Metadata: converted.Parts[0].DBMetadata}
	changed := original
	changed.Metadata.Revision = fidelityInt(4)
	changed.Metadata.EditTimestamp = fidelityInt(101)
	changed.Metadata.Entities = []sidecar.MessageEntityRecord{{Kind: "TYPE_BOLD", Offset: 0, Length: 4}}
	result, err := upsertExistingInlineMessage(t.Context(), portal, nil, []*database.Message{row}, changed)
	if err != nil || len(result.SubEvents) != 1 {
		t.Fatalf("format-only edit = %+v, %v", result, err)
	}
	edit, err := convertInlineMessageEdit(t.Context(), portal, nil, []*database.Message{row}, changed)
	if err != nil || len(edit.ModifiedParts) != 1 || edit.ModifiedParts[0].Content.Format != event.FormatHTML {
		t.Fatalf("converted edit = %+v, %v", edit, err)
	}
	conflict := original
	conflict.Content.Text = "different content at the same accepted revision"
	result, err = upsertExistingInlineMessage(t.Context(), portal, nil, []*database.Message{row}, conflict)
	if err != nil || len(result.SubEvents) != 0 {
		t.Fatalf("same-revision conflict = %+v, %v", result, err)
	}
	stale := original
	stale.Metadata.Revision = fidelityInt(2)
	stale.Content.Text = "older"
	result, err = upsertExistingInlineMessage(t.Context(), portal, nil, []*database.Message{row}, stale)
	if err != nil || len(result.SubEvents) != 0 {
		t.Fatalf("stale result = %+v, %v", result, err)
	}
	unversioned := original
	unversioned.Metadata = sidecar.MessageMetadata{}
	result, err = upsertExistingInlineMessage(t.Context(), portal, nil, []*database.Message{row}, unversioned)
	if err != nil || len(result.SubEvents) != 0 {
		t.Fatalf("unversioned replay = %+v, %v", result, err)
	}
}

func TestMessageTargetRejectsOtherChatAndReceiver(t *testing.T) {
	client := &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "account-a"}}}
	portal := testPortal("7")
	portal.Receiver = "account-a"
	for _, target := range []*database.Message{
		{ID: makeMessageID(8, 11), Room: portal.PortalKey},
		{ID: makeMessageID(7, 11), Room: networkid.PortalKey{ID: "7", Receiver: "account-b"}},
	} {
		if _, _, err := client.validatedTarget(portal, target); err == nil {
			t.Fatalf("accepted foreign target %+v", target)
		}
		if _, err := client.validatedReply(portal, target); err == nil {
			t.Fatalf("accepted foreign reply %+v", target)
		}
	}
	correct := &database.Message{ID: makeMessageID(7, 11), Room: portal.PortalKey}
	if _, _, err := client.validatedTarget(portal, correct); err != nil {
		t.Fatal(err)
	}
	portal.Receiver = "account-b"
	if _, _, err := client.validatedTarget(portal, correct); err == nil {
		t.Fatal("accepted other login's portal")
	}
}

func TestCaptionEditReloadReusesMatrixMediaEnvelope(t *testing.T) {
	portal := testPortal("7")
	source := sidecar.MessageRecord{ChatID: 7, MessageID: 11, SenderID: 42, Content: sidecar.MessageContent{Type: "media", Kind: "photo", FileID: "native-1", URL: "https://must-not-download.invalid/file", Caption: fidelityString("new caption")}, Metadata: sidecar.MessageMetadata{Revision: fidelityInt(2), Entities: []sidecar.MessageEntityRecord{{Kind: "TYPE_BOLD", Offset: 0, Length: 3}}}}
	saved := MessageMetadata{MediaHandled: true, MediaIdentity: inlineMediaIdentity(source.Content), MatrixContent: &event.MessageEventContent{MsgType: event.MsgImage, Body: "old caption", FileName: "image.jpg", URL: "mxc://test/stable", Info: &event.FileInfo{MimeType: "image/jpeg", Size: 123}}}
	path := filepath.Join(t.TempDir(), "caption.db")
	db := openTestBridgeDatabase(t, path)
	if err := db.Portal.Insert(t.Context(), &database.Portal{PortalKey: portal.PortalKey, MXID: "!media:test"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Ghost.Insert(t.Context(), &database.Ghost{ID: "42"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Message.Insert(t.Context(), &database.Message{ID: makeMessageID(7, 11), Room: portal.PortalKey, SenderID: "42", MXID: "$media", Timestamp: time.Now(), Metadata: &saved}); err != nil {
		t.Fatal(err)
	}
	if err := db.RawDB.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestBridgeDatabase(t, path)
	rows, err := db.Message.GetAllPartsByID(t.Context(), portal.Receiver, makeMessageID(7, 11))
	if err != nil || len(rows) != 1 {
		t.Fatalf("reload caption mapping: %+v, %v", rows, err)
	}
	row := rows[0]

	converted, err := convertInlineMessageEdit(t.Context(), portal, nil, []*database.Message{row}, source)
	if err != nil {
		t.Fatal(err)
	}
	content := converted.ModifiedParts[0].Content
	if content.URL != "mxc://test/stable" || content.Body != "new caption" || !strings.Contains(content.FormattedBody, "<strong>new</strong>") {
		t.Fatalf("caption edit = %+v", content)
	}
	replacement := cloneMatrixContent(content)
	replacement.URL = "mxc://test/replacement"
	if err := validateCaptionEdit(content, replacement); err == nil {
		t.Fatal("accepted attachment replacement")
	}
	textOnly := &event.MessageEventContent{MsgType: event.MsgText, Body: "replace attachment"}
	if err := validateCaptionEdit(content, textOnly); err == nil {
		t.Fatal("accepted attachment-to-text replacement")
	}
}

func TestCompletedMatrixUploadEchoReusesOriginalMedia(t *testing.T) {
	portal := testPortal("7")
	source := sidecar.MessageRecord{ChatID: 7, MessageID: 11, SenderID: 42, IsOutgoing: true, Content: sidecar.MessageContent{Type: "media", Kind: "photo", FileID: "native", URL: "https://must-not-download.invalid"}}
	row := &database.Message{ID: makeMessageID(7, 11), Room: portal.PortalKey, SenderID: makeUserID("42"), Metadata: &MessageMetadata{MatrixContent: &event.MessageEventContent{MsgType: event.MsgImage, Body: "picture.jpg", URL: "mxc://test/original"}}}
	edit, err := convertInlineMessageEdit(t.Context(), portal, nil, []*database.Message{row}, source)
	if err != nil {
		t.Fatal(err)
	}
	if edit.ModifiedParts[0].Content.URL != "mxc://test/original" || edit.ModifiedParts[0].Content.Body != "picture.jpg" {
		t.Fatalf("echo = %+v", edit)
	}
}

func TestAuthenticatedMatrixMediaTransportBoundsAndClassifies(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		length    int64
		data      string
		max       int64
		permanent bool
		wantErr   bool
	}{
		{"small", 200, 4, "data", 8, false, false},
		{"declared oversize", 200, 100, "data", 8, true, true},
		{"chunked oversize", 200, -1, strings.Repeat("x", 33), 8, true, true},
		{"throttle", 429, 0, "", 8, false, true},
		{"server", 503, 0, "", 8, false, true},
		{"invalid", 404, 0, "", 8, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer secret-test-token" {
					t.Errorf("missing authenticated media request")
				}
				if r.URL.Path != "/_matrix/client/v1/media/download/test/file" {
					t.Errorf("media route = %s", r.URL.Path)
				}
				if test.length >= 0 {
					w.Header().Set("Content-Length", fmt.Sprint(test.length))
				}
				w.WriteHeader(test.status)
				if test.length < 0 {
					w.(http.Flusher).Flush()
				}
				_, _ = io.WriteString(w, test.data)
			}))
			defer server.Close()
			client, err := mautrix.NewClient(server.URL, "@bridge:test", "secret-test-token")
			if err != nil {
				t.Fatal(err)
			}
			client.DefaultHTTPRetries = 0
			data, err := downloadBoundedMatrixMedia(t.Context(), client, &event.MessageEventContent{URL: "mxc://test/file"}, test.max)
			if (err != nil) != test.wantErr {
				t.Fatalf("download bytes %q, error %v", data, err)
			}
			if err != nil && isPermanentMediaFailure(err) != test.permanent {
				t.Fatalf("error classification = %v: %v", isPermanentMediaFailure(err), err)
			}
		})
	}
}

type fidelityDownloadFunc func(context.Context, id.ContentURI) (*http.Response, error)

func (f fidelityDownloadFunc) Download(ctx context.Context, uri id.ContentURI) (*http.Response, error) {
	return f(ctx, uri)
}
func TestBoundedMatrixMediaRejectsLyingLengthAndEncryptedHashMismatch(t *testing.T) {
	file := attachment.NewEncryptedFile()
	encrypted := file.Encrypt([]byte("plaintext"))
	encrypted[0] ^= 0xff
	downloader := fidelityDownloadFunc(func(context.Context, id.ContentURI) (*http.Response, error) {
		return &http.Response{StatusCode: 200, ContentLength: 1, Body: io.NopCloser(strings.NewReader(string(encrypted)))}, nil
	})
	content := &event.MessageEventContent{File: &event.EncryptedFileInfo{EncryptedFile: *file, URL: "mxc://test/file"}}
	if _, err := downloadBoundedMatrixMedia(t.Context(), downloader, content, 64); err == nil || !isPermanentMediaFailure(err) {
		t.Fatalf("hash mismatch = %v", err)
	}
	if _, err := downloadBoundedMatrixMedia(t.Context(), downloader, content, 2); err == nil || !isPermanentMediaFailure(err) {
		t.Fatalf("lying Content-Length = %v", err)
	}
	content.Info = &event.FileInfo{Size: 100}
	calls := 0
	neverDownload := fidelityDownloadFunc(func(context.Context, id.ContentURI) (*http.Response, error) {
		calls++
		return nil, fmt.Errorf("unexpected download")
	})
	if _, err := downloadBoundedMatrixMedia(t.Context(), neverDownload, content, 2); err == nil || calls != 0 {
		t.Fatalf("declared size = %v, calls %d", err, calls)
	}
}

func TestBoundedMatrixMediaNetworkFailureIsRetryable(t *testing.T) {
	downloader := fidelityDownloadFunc(func(context.Context, id.ContentURI) (*http.Response, error) { return nil, context.DeadlineExceeded })
	if _, err := downloadBoundedMatrixMedia(t.Context(), downloader, &event.MessageEventContent{URL: "mxc://test/file"}, 8); err == nil || isPermanentMediaFailure(err) {
		t.Fatalf("network error = %v", err)
	}
}

func TestMatrixEditRejectsAttachmentReplacementBeforeSourceMutation(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "unexpected source mutation", 500)
	}))
	defer server.Close()
	client := &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "42"}}, Sidecar: sidecar.NewClient(server.URL)}
	portal := testPortal("7")
	portal.Receiver = "42"
	original := &event.MessageEventContent{MsgType: event.MsgImage, Body: "picture.jpg", URL: "mxc://test/file", Info: &event.FileInfo{MimeType: "image/jpeg"}}
	target := &database.Message{ID: makeMessageID(7, 11), Room: portal.PortalKey, Metadata: &MessageMetadata{MatrixContent: original}}
	replacements := []*event.MessageEventContent{
		{MsgType: event.MsgText, Body: "replace with text"},
		{MsgType: event.MsgImage, Body: "new caption", FileName: "picture.jpg", URL: "mxc://test/replacement"},
	}
	for _, replacement := range replacements {
		err := client.HandleMatrixEdit(t.Context(), &bridgev2.MatrixEdit{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: portal, Content: replacement}, EditTarget: target})
		if err == nil {
			t.Fatal("accepted attachment replacement")
		}
	}
	if calls != 0 {
		t.Fatalf("source mutations before validation: %d", calls)
	}
	legitimate := cloneMatrixContent(original)
	legitimate.Body = "caption"
	legitimate.FileName = "picture.jpg"
	if err := validateCaptionEdit(original, legitimate); err != nil {
		t.Fatalf("caption with original filename rejected: %v", err)
	}
}
