package connector

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

// Matrix threads share their parent's audience; Inline child chats retain
// independent identity and access. No native flattening/creation is enabled.
func validateInlineReplyTarget(msg *bridgev2.MatrixMessage, chatID int64) error {
	if msg.ThreadRoot != nil || (msg.Content != nil && msg.Content.RelatesTo != nil && msg.Content.RelatesTo.Type == event.RelThread) {
		return errors.New("Inline native Matrix thread routing is unsupported; use the separate child conversation")
	}
	if msg.ReplyTo != nil {
		if msg.Portal == nil || msg.Portal.Portal == nil || msg.ReplyTo.Room != msg.Portal.PortalKey {
			return errors.New("Inline reply target must belong to this conversation and login")
		}
		parent, root, err := parseMessageID(msg.ReplyTo.ID)
		if err != nil || parent != chatID || root <= 0 {
			return errors.New("Inline reply target must belong to this conversation")
		}
	}
	return nil
}

// This text is written only inside the eligible child, never into the parent.
// Missing/deleted/unprojected roots retain readable source context. Lookup is
// existing-only, receiver-scoped and never creates a portal or fetches history.
func (ic *InlineClient) replyChildTopic(ctx context.Context, d sidecar.DialogRecord) (string, error) {
	kind := classifyDialog(d)
	if kind != roomReplyChild {
		if kind == roomChannel {
			return optionalStringValue(d.Description), nil
		}
		return "", nil
	}
	structure := d.Structure
	text := fmt.Sprintf("Inline reply context: chat %d", *structure.ParentChatID)
	if structure.ParentMessageID != nil {
		text += fmt.Sprintf(", message %d", *structure.ParentMessageID)
	}
	text += ". This is a separate conversation."
	if ic.UserLogin != nil && ic.UserLogin.UserLogin != nil && ic.UserLogin.ID != "" && ic.UserLogin.Bridge != nil && ic.UserLogin.Bridge.DB != nil && structure.ParentMessageID != nil {
		key := ic.portalKeyForChat(*structure.ParentChatID)
		parent, err := ic.UserLogin.Bridge.GetExistingPortalByKey(ctx, key)
		if err != nil {
			return "", fmt.Errorf("read parent portal mapping: %w", err)
		}
		if parent != nil && parent.PortalKey == key && parent.MXID != "" {
			row, err := ic.UserLogin.Bridge.DB.Message.GetFirstPartByID(ctx, key.Receiver, makeMessageID(*structure.ParentChatID, *structure.ParentMessageID))
			if err != nil {
				return "", fmt.Errorf("read parent message mapping: %w", err)
			}
			if row != nil && row.Room == key && row.MXID != "" && !row.HasFakeMXID() {
				link := url.URL{Scheme: "https", Host: "matrix.to", Fragment: "/" + string(parent.MXID) + "/" + string(row.MXID)}
				text += " Parent message: " + link.String()
			}
		}
	}
	if description := strings.TrimSpace(optionalStringValue(d.Description)); description != "" {
		return description + "\n\n" + text, nil
	}
	return text, nil
}
