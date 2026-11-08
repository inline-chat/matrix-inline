// Package msgconv converts text and entities without network or account state.
package msgconv

import (
	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// InlineToMatrix resolves only identities verified for the current account.
type InlineToMatrix func(int64) (id.UserID, bool)

// MatrixToInline resolves only identities verified for the current account.
type MatrixToInline func(id.UserID) (int64, bool)

// ToMatrix retains the exact plain body and renders bold, italic, code/pre,
// safe links and explicitly resolved user mentions. Other kinds stay readable.
// Invalid spans/payloads and ambiguous overlapping links are omitted. Formatting
// is swept in O((text+entities) log entities) work, without recursive traversal.
// NUL/invalid UTF-8 or markup exceeding a linear input budget falls back to the
// exact body with no formatted body or mentions. No message length limit is
// expanded here: the host must enforce its existing text/transfer limits.
func ToMatrix(text string, entities []sidecar.MessageEntityRecord, resolve InlineToMatrix) event.MessageEventContent {
	return render(text, entities, resolve)
}

// FromMatrix preserves unformatted Body exactly. Explicit HTML is tokenized
// iteratively: bold/italic/code/pre, safe links and verified user pills become
// UTF-16 entities; unsupported blocks become readable text separated by lines.
// Active content and mx-reply fallback are skipped. HTML CRLF/CR becomes LF and
// NUL becomes U+FFFD; invalid UTF-8/tokenizer errors use the exact Body fallback.
// Only typed Matrix user links with an affirmative resolver can become mentions.
// No markup, URL, ID prefix or display name is trusted as account verification.
func FromMatrix(content *event.MessageEventContent, resolve MatrixToInline) (string, []sidecar.MessageEntityRecord) {
	if content == nil {
		return "", nil
	}
	return parse(content, resolve)
}
