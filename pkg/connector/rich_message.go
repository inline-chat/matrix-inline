package connector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/inline-chat/matrix-inline/pkg/msgconv"
	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Partial caption-edit support requires the exact saved media envelope. Clients
// that normalize or omit attachment metadata may receive an explicit unsupported
// error; silently discarding those changes or weakening crypto identity is unsafe.
func matrixCaptionEditAllowed(target *database.Message, content *event.MessageEventContent) bool {
	if target == nil || content == nil {
		return false
	}
	meta, ok := target.Metadata.(*MessageMetadata)
	if !ok || meta == nil || meta.InlineMedia == nil {
		return false
	}
	previous := meta.InlineMedia
	return previous.MsgType == content.MsgType && previous.URL == content.URL && reflect.DeepEqual(previous.File, content.File) && previous.FileName == content.FileName && reflect.DeepEqual(previous.Info, content.Info)
}

func (ic *InlineClient) incomingMention(userID int64) (id.UserID, bool) {
	if userID <= 0 || ic.Sidecar == nil || ic.Sidecar.SessionNamespace == "" || ic.UserLogin == nil || ic.UserLogin.UserLogin == nil || ic.UserLogin.ID == "" {
		return "", false
	}
	if strconv.FormatInt(userID, 10) == ic.AccountID && ic.UserLogin.User != nil && ic.UserLogin.User.User != nil {
		return ic.UserLogin.User.MXID, ic.UserLogin.User.MXID != ""
	}
	if user, ok := ic.cachedUser(userID); !ok || user.UserID != userID || ic.UserLogin.Bridge == nil || ic.UserLogin.Bridge.Matrix == nil {
		return "", false
	}
	intent := ic.UserLogin.Bridge.Matrix.GhostIntent(makeUserID(strconv.FormatInt(userID, 10)))
	if intent == nil {
		return "", false
	}
	return intent.GetMXID(), intent.GetMXID() != ""
}

func (ic *InlineClient) outgoingMentionResolver(content *event.MessageEventContent) msgconv.MatrixToInline {
	return func(user id.UserID) (int64, bool) {
		if content != nil && content.Mentions != nil {
			listed := false
			for _, mentioned := range content.Mentions.UserIDs {
				if mentioned == user {
					listed = true
					break
				}
			}
			if !listed {
				return 0, false
			}
		}
		if ic.Sidecar == nil || ic.Sidecar.SessionNamespace == "" || ic.UserLogin == nil || ic.UserLogin.UserLogin == nil || ic.UserLogin.ID == "" {
			return 0, false
		}
		if ic.UserLogin.User != nil && ic.UserLogin.User.User != nil && user == ic.UserLogin.User.MXID {
			self, err := parseInlineUserID(ic.AccountID)
			return self, err == nil
		}
		if ic.UserLogin.Bridge == nil || ic.UserLogin.Bridge.Matrix == nil {
			return 0, false
		}
		remote, ok := ic.UserLogin.Bridge.Matrix.ParseGhostMXID(user)
		if !ok {
			return 0, false
		}
		value, err := parseInlineUserID(string(remote))
		if err != nil {
			return 0, false
		}
		verified, ok := ic.incomingMention(value)
		return value, ok && verified == user
	}
}

func (ic *InlineClient) convertInlineMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data sidecar.MessageRecord) (*bridgev2.ConvertedMessage, error) {
	return convertInlineMessageWithResolver(ctx, portal, intent, data, ic.incomingMention)
}

func (ic *InlineClient) convertInlineMessageEdit(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data sidecar.MessageRecord) (*bridgev2.ConvertedEdit, error) {
	return convertInlineMessageEditWithResolver(ctx, portal, intent, existing, data, ic.incomingMention)
}

func (ic *InlineClient) upsertExistingInlineMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data sidecar.MessageRecord) (bridgev2.UpsertResult, error) {
	result, err := upsertExistingInlineMessage(ctx, portal, intent, existing, data)
	for _, sub := range result.SubEvents {
		if edit, ok := sub.(*simplevent.Message[sidecar.MessageRecord]); ok {
			edit.ConvertEditFunc = ic.convertInlineMessageEdit
		}
	}
	return result, err
}

// Native revisions take precedence over edit dates. A revisionless observation
// cannot overwrite a revision-bearing one. With neither revision nor edit date,
// fingerprints suppress identical replays but cannot establish a total order.
func staleInlineRevision(existing *database.Message, data sidecar.MessageRecord) bool {
	if existing == nil {
		return false
	}
	meta, ok := existing.Metadata.(*MessageMetadata)
	if !ok || meta == nil {
		return false
	}
	if meta.InlineRevision != nil {
		return data.Metadata.Revision == nil || *data.Metadata.Revision <= *meta.InlineRevision
	}
	if data.Metadata.Revision != nil {
		return false
	}
	if meta.InlineEditTime != nil {
		return data.Metadata.EditTimestamp == nil || *data.Metadata.EditTimestamp <= *meta.InlineEditTime
	}
	return false
}

func applyInlineRichContent(converted *bridgev2.ConvertedMessage, data sidecar.MessageRecord, resolve msgconv.InlineToMatrix) {
	if converted == nil || len(converted.Parts) == 0 || converted.Parts[0] == nil || converted.Parts[0].Content == nil {
		return
	}
	if data.Content.Type != "" && data.Content.Type != "text" && data.Content.Type != "media" {
		return
	}
	text := data.Content.Text
	if data.Content.Type == "media" {
		text = optionalStringValue(data.Content.Caption)
		if text == "" || !converted.Parts[0].Content.MsgType.IsMedia() {
			return
		}
	}
	if text == "" {
		text = " "
	}
	rich := msgconv.ToMatrix(text, data.Metadata.Entities, resolve)
	c := converted.Parts[0].Content
	c.Body, c.Format, c.FormattedBody, c.Mentions = rich.Body, rich.Format, rich.FormattedBody, rich.Mentions
}

// Signed CDN URLs may rotate while the stable native file identity stays the
// same. All other media attributes remain part of the identity; no caption
// change can trigger another download/upload, including after a restart.
func inlineMediaIdentity(content sidecar.MessageContent) string {
	content.Caption, content.Text = nil, ""
	if content.FileID != "" {
		content.URL = ""
	}
	raw, _ := json.Marshal(content)
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func convertedInlineEditContent(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing *database.Message, data sidecar.MessageRecord) (*bridgev2.ConvertedMessage, error) {
	if data.Content.Type == "media" && existing != nil {
		if meta, ok := existing.Metadata.(*MessageMetadata); ok && meta != nil && meta.InlineMedia != nil {
			// A completed outbound mutation already binds this exact native
			// message ID to the original Matrix media. Its first echo records
			// the native file identity without transferring the bytes again.
			unboundOutbound := meta.InlineMediaID == "" && existing.ID == makeMessageID(data.ChatID, data.MessageID)
			if !unboundOutbound && meta.InlineMediaID != inlineMediaIdentity(data.Content) {
				return nil, fmt.Errorf("Inline media replacement is unsupported: %w", bridgev2.ErrUnsupportedMessageType)
			}
			content := *meta.InlineMedia
			// A completed upload may have used the native document fallback.
			// Preserve its Matrix object while presenting the observed source kind.
			content.MsgType = inlineMediaMessageType(data.Content)
			if data.Content.Kind != "voice" || content.MsgType != event.MsgAudio {
				content.MSC3245Voice, content.MSC1767Audio = nil, nil
			}
			content.Body = inlineMediaBody(data.Content, content.FileName)
			content.Format, content.FormattedBody, content.Mentions = "", "", nil
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{Type: event.EventMessage, Content: &content}}}, nil
		}
	}
	return convertedContentFromInline(ctx, portal, intent, data.Content)
}
