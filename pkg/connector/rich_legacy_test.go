package connector

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/id"
)

func TestRichLegacyFingerprintReplayAndRealChanges(t *testing.T) {
	// Golden hashes of the exact JSON emitted by 537417d, before metadata was
	// hashed. They deliberately do not call either current fingerprint helper.
	const textHash = "f06b734cbc22a298797630f8c4f1a123d4846bf13e8c1d003dddfb3ac7fcc14d"
	const mediaHash = "8395a1073f8264675a6b4e1fc2a66ec30341f460579980645c216e768213e147"
	zero, no := int64(0), false
	cases := []struct {
		name                 string
		change               func(*sidecar.MessageRecord)
		media, handled, edit bool
	}{
		{name: "identical"},
		{name: "empty-slices", change: func(m *sidecar.MessageRecord) {
			m.Metadata.Entities = []sidecar.MessageEntityRecord{}
			m.Metadata.Attachments = []sidecar.MessageAttachmentRecord{}
			m.Metadata.Actions = []sidecar.MessageActionRecord{}
		}},
		{name: "text", edit: true, change: func(m *sidecar.MessageRecord) { m.Content.Text = "edited" }},
		{name: "sender", edit: true, change: func(m *sidecar.MessageRecord) { m.SenderID = 43 }},
		{name: "direction", edit: true, change: func(m *sidecar.MessageRecord) { m.IsOutgoing = true }},
		{name: "reply", edit: true, change: func(m *sidecar.MessageRecord) { m.ReplyToMessageID = &zero }},
		{name: "entities", edit: true, change: func(m *sidecar.MessageRecord) {
			m.Metadata.Entities = []sidecar.MessageEntityRecord{{Kind: "TYPE_BOLD", Offset: 0, Length: 5}}
		}},
		{name: "revision-zero", edit: true, change: func(m *sidecar.MessageRecord) { m.Metadata.Revision = &zero }},
		{name: "edit-time-zero", edit: true, change: func(m *sidecar.MessageRecord) { m.Metadata.EditTimestamp = &zero }},
		{name: "mentioned-false", edit: true, change: func(m *sidecar.MessageRecord) { m.Metadata.Mentioned = &no }},
		{name: "bot-false", edit: true, change: func(m *sidecar.MessageRecord) { m.Metadata.SenderIsBot = &no }},
		{name: "service", edit: true, change: func(m *sidecar.MessageRecord) { m.Metadata.IsService = true }},
		{name: "attachment", edit: true, change: func(m *sidecar.MessageRecord) { m.Metadata.Attachments = []sidecar.MessageAttachmentRecord{{}} }},
		{name: "action", edit: true, change: func(m *sidecar.MessageRecord) { m.Metadata.Actions = []sidecar.MessageActionRecord{{}} }},
		{name: "agent-session", edit: true, change: func(m *sidecar.MessageRecord) { m.Metadata.AgentSession = &sidecar.AgentSessionMessageMetadata{} }},
		{name: "media-handled", media: true, handled: true},
		{name: "media-retry", media: true, edit: true},
		{name: "media-changed", media: true, handled: true, edit: true, change: func(m *sidecar.MessageRecord) { m.Content.URL += "-new" }},
	}
	ic := fixtureHistoryClient(t, filepath.Join(t.TempDir(), "legacy.db"))
	key := ic.portalKeyForChat(7)
	if err := ic.UserLogin.Bridge.DB.Portal.Insert(t.Context(), &database.Portal{PortalKey: key, Metadata: &PortalMetadata{}}); err != nil {
		t.Fatal(err)
	}
	for n, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := sidecar.MessageRecord{ChatID: 7, MessageID: int64(n + 1), SenderID: 42, Content: sidecar.MessageContent{Type: "text", Text: "hello"}}
			hash := textHash
			if tc.media {
				data.Content = sidecar.MessageContent{Type: "media", URL: "https://cdn.inline.test/file"}
				hash = mediaHash
			}
			row := &database.Message{ID: makeMessageID(7, data.MessageID), MXID: id.EventID(fmt.Sprintf("$legacy-%d", n)), Room: key, Metadata: &MessageMetadata{InlineFingerprint: hash, MediaHandled: tc.handled}}
			db := ic.UserLogin.Bridge.DB.Message
			if err := db.Insert(t.Context(), row); err != nil {
				t.Fatal(err)
			}
			row, err := db.GetFirstPartByID(t.Context(), key.Receiver, row.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.change != nil {
				tc.change(&data)
			}
			got, err := upsertExistingInlineMessage(t.Context(), testPortal("7"), nil, []*database.Message{row}, data)
			want := 0
			if tc.edit {
				want = 1
			}
			if err != nil || len(got.SubEvents) != want || got.ContinueMessageHandling {
				t.Fatalf("legacy replay result %+v %v; want %d edits", got, err, want)
			}
		})
	}
}
