package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
)

func unsupportedMatrixOperation(err error) error {
	return bridgev2.WrapErrorInStatus(err).WithErrorAsMessage().WithIsCertain(true).WithSendNotice(true).WithErrorReason(event.MessageStatusUnsupported)
}

func (ic *InlineClient) validateMessagePortal(portal *bridgev2.Portal) (int64, error) {
	if portal == nil || ic.UserLogin == nil || portal.Receiver != ic.UserLogin.ID {
		return 0, fmt.Errorf("Inline message portal belongs to another login")
	}
	chatID, err := strconv.ParseInt(string(portal.ID), 10, 64)
	if err != nil || chatID <= 0 {
		return 0, fmt.Errorf("invalid Inline chat portal")
	}
	return chatID, nil
}
func (ic *InlineClient) validatedTarget(portal *bridgev2.Portal, target *database.Message) (int64, int64, error) {
	chatID, err := ic.validateMessagePortal(portal)
	if err != nil {
		return 0, 0, err
	}
	if target == nil || target.Room != portal.PortalKey {
		return 0, 0, fmt.Errorf("Inline target belongs to another room or login")
	}
	targetChatID, messageID, err := parseMessageID(target.ID)
	if err != nil || targetChatID != chatID || messageID <= 0 {
		return 0, 0, fmt.Errorf("Inline target does not match this chat")
	}
	return chatID, messageID, nil
}
func (ic *InlineClient) validatedReply(portal *bridgev2.Portal, target *database.Message) (*int64, error) {
	if target == nil {
		return nil, nil
	}
	_, messageID, err := ic.validatedTarget(portal, target)
	if err != nil {
		return nil, err
	}
	return &messageID, nil
}

func cloneMatrixContent(content *event.MessageEventContent) *event.MessageEventContent {
	if content == nil {
		return nil
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return nil
	}
	var clone event.MessageEventContent
	if json.Unmarshal(raw, &clone) != nil {
		return nil
	}
	clone.RelatesTo = nil
	clone.NewContent = nil
	return &clone
}
func inlineMediaIdentity(content sidecar.MessageContent) string {
	if content.Type != "media" {
		return ""
	}
	if content.FileID != "" {
		return content.Kind + "/" + content.FileID
	}
	return content.Kind + "/" + content.URL
}
func inlineRevisionIsStale(metadata *MessageMetadata, data sidecar.MessageRecord) bool {
	if metadata == nil {
		return false
	}
	if metadata.SourceRevision != nil {
		if data.Metadata.Revision == nil {
			return true
		}
		if *data.Metadata.Revision != *metadata.SourceRevision {
			return *data.Metadata.Revision < *metadata.SourceRevision
		}
		if metadata.InlineFingerprint != "" && metadata.InlineFingerprint != inlineMessageFingerprint(data) {
			return true
		}
	}
	if metadata.SourceEditTimestamp != nil {
		return data.Metadata.EditTimestamp == nil || *data.Metadata.EditTimestamp < *metadata.SourceEditTimestamp
	}
	return false
}
func inlineEditTimestamp(data sidecar.MessageRecord) int64 {
	if data.Metadata.EditTimestamp != nil {
		return *data.Metadata.EditTimestamp
	}
	return data.Timestamp
}
func reuseInlineMedia(existing *database.Message, data sidecar.MessageRecord) *bridgev2.ConvertedMessage {
	metadata, ok := existing.Metadata.(*MessageMetadata)
	if !ok || metadata == nil || metadata.MatrixContent == nil || data.Content.Type != "media" {
		return nil
	}
	if metadata.MediaIdentity != inlineMediaIdentity(data.Content) {
		// The completed Matrix upload row already has the authoritative chat/message
		// ID. Its first native echo learns the source file identity without transfer.
		if metadata.MediaIdentity != "" || !data.IsOutgoing || existing.SenderID != makeUserID(strconv.FormatInt(data.SenderID, 10)) {
			return nil
		}
	}
	content := cloneMatrixContent(metadata.MatrixContent)
	if content == nil || (content.URL == "" && content.File == nil) {
		return nil
	}
	fileName := content.GetFileName()
	if fileName == "" {
		fileName = inlineMediaFileName(data.Content)
	}
	content.FileName = fileName
	content.Body = inlineMediaBody(data.Content, fileName)
	content.Format = ""
	content.FormattedBody = ""
	content.Mentions = &event.Mentions{}
	return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{Type: event.EventMessage, Content: content}}}
}

// Matrix only edits the existing attachment's caption. Check every attachment
// field before calling the source mutation so unsupported replacements cannot
// partially edit the Inline message.
func validateCaptionEdit(original, updated *event.MessageEventContent) error {
	if original == nil || updated == nil {
		return fmt.Errorf("Inline attachment caption cannot be edited without its saved media identity")
	}
	if original.MsgType != updated.MsgType || original.URL != updated.URL || original.GetFileName() != updated.GetFileName() {
		return fmt.Errorf("replacing an Inline attachment is not supported; send a new message")
	}
	oldFile, _ := json.Marshal(original.File)
	newFile, _ := json.Marshal(updated.File)
	oldInfo, _ := json.Marshal(struct {
		Info  *event.FileInfo
		Voice *event.MSC3245Voice
		Audio *event.MSC1767Audio
	}{original.Info, original.MSC3245Voice, original.MSC1767Audio})
	newInfo, _ := json.Marshal(struct {
		Info  *event.FileInfo
		Voice *event.MSC3245Voice
		Audio *event.MSC1767Audio
	}{updated.Info, updated.MSC3245Voice, updated.MSC1767Audio})
	if string(oldFile) != string(newFile) || string(oldInfo) != string(newInfo) {
		return fmt.Errorf("replacing Inline attachment data or metadata is not supported")
	}
	return nil
}

func inlineVoiceMetadata(content sidecar.MessageContent) *event.MSC3245Voice {
	if content.Kind == "voice" {
		return &event.MSC3245Voice{}
	}
	return nil
}

func nonemptyCaption(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// Old bridge rows have no saved envelope. Resolve the original Matrix event
// before deciding whether the requested edit is text, a caption or replacement.
func (ic *InlineClient) originalEditContent(ctx context.Context, msg *bridgev2.MatrixEdit, metadata *MessageMetadata) (*event.MessageEventContent, error) {
	if metadata != nil && metadata.MatrixContent != nil {
		return metadata.MatrixContent, nil
	}
	if ic.UserLogin == nil || ic.UserLogin.Bridge == nil || ic.UserLogin.Bridge.Bot == nil {
		return nil, fmt.Errorf("cannot verify original Inline message content")
	}
	original, err := ic.UserLogin.Bridge.Bot.GetEvent(ctx, msg.Portal.MXID, msg.EditTarget.MXID)
	if err != nil {
		return nil, fmt.Errorf("fetch original Matrix message before Inline edit: %w", err)
	}
	if original == nil || original.Type != event.EventMessage || original.ID != msg.EditTarget.MXID || (original.RoomID != "" && original.RoomID != msg.Portal.MXID) {
		return nil, fmt.Errorf("original Inline edit event identity is invalid")
	}
	return original.Content.AsMessage(), nil
}
