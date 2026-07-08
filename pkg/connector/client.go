package connector

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

const (
	startupDialogPageLimit   = 100
	startupHistoryLimit      = 20
	sidecarEventReconnectLag = 3 * time.Second
	maxInlineMediaDownload   = 100 << 20
	maxInlineAvatarDownload  = 8 << 20
)

var inlineMediaHTTPClient = &http.Client{Timeout: 2 * time.Minute}

type InlineClient struct {
	UserLogin *bridgev2.UserLogin
	AccountID string
	Sidecar   *sidecar.Client

	mu       sync.RWMutex
	loggedIn bool
	users    map[int64]sidecar.UserRecord

	runCancel context.CancelFunc
	wg        sync.WaitGroup
}

var _ bridgev2.NetworkAPI = (*InlineClient)(nil)
var _ bridgev2.NetworkAPIWithUserID = (*InlineClient)(nil)
var _ bridgev2.BackfillingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.EditHandlingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.RedactionHandlingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.ReactionHandlingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.ReadReceiptHandlingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.ChatViewingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.TypingHandlingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.IdentifierResolvingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.GhostDMCreatingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.GroupCreatingNetworkAPI = (*InlineClient)(nil)

func (ic *InlineClient) Connect(ctx context.Context) {
	ic.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnecting})
	current, err := ic.Sidecar.Status(ctx)
	if err != nil {
		ic.setLoggedIn(false)
		ic.UserLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateTransientDisconnect,
			Error:      "inline-sidecar-unreachable",
			Message:    err.Error(),
			UserAction: status.UserActionRestart,
		})
		return
	}
	if current.Status == sidecar.StatusDisconnected {
		current, err = ic.Sidecar.Resume(ctx)
		if err != nil {
			ic.setLoggedIn(false)
			ic.UserLogin.BridgeState.Send(status.BridgeState{
				StateEvent: status.StateTransientDisconnect,
				Error:      "inline-sidecar-resume-failed",
				Message:    err.Error(),
				UserAction: status.UserActionRestart,
			})
			return
		}
	}

	switch current.Status {
	case sidecar.StatusConnected:
		ic.setLoggedIn(true)
		ic.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
		ic.startSidecarLoops(ctx)
	case sidecar.StatusAuthRequired, sidecar.StatusAuthExpired, sidecar.StatusLoggedOut:
		ic.setLoggedIn(false)
		ic.markBadCredentials("inline-auth-required", "Inline session needs relogin")
	default:
		ic.setLoggedIn(false)
		ic.UserLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateTransientDisconnect,
			Error:      "inline-not-connected",
			Message:    fmt.Sprintf("Inline sidecar status is %s", current.Status),
			UserAction: status.UserActionRestart,
		})
	}
}

func (ic *InlineClient) Disconnect() {
	ic.mu.Lock()
	cancel := ic.runCancel
	ic.runCancel = nil
	ic.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	ic.wg.Wait()
}

func (ic *InlineClient) IsLoggedIn() bool {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	return ic.loggedIn
}

func (ic *InlineClient) LogoutRemote(ctx context.Context) {
	ic.Disconnect()
	_ = ic.Sidecar.Logout(ctx)
	ic.setLoggedIn(false)
	if meta, ok := ic.UserLogin.Metadata.(*UserLoginMetadata); ok {
		meta.SessionInvalidated = true
		if err := ic.UserLogin.Save(ctx); err != nil {
			ic.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to save Inline logout state")
		}
	}
}

func (ic *InlineClient) IsThisUser(ctx context.Context, userID networkid.UserID) bool {
	return string(userID) == ic.AccountID || string(userID) == string(ic.UserLogin.ID)
}

func (ic *InlineClient) GetUserID() networkid.UserID {
	return makeUserID(ic.AccountID)
}

func (ic *InlineClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	chatID, err := chatIDFromPortal(portal)
	if err != nil {
		return nil, err
	}
	return ic.chatInfoForChat(ctx, chatID, optionalString(string(portal.ID)))
}

func (ic *InlineClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	if userID, err := strconv.ParseInt(string(ghost.ID), 10, 64); err == nil {
		if user, ok := ic.cachedUser(userID); ok {
			return userInfoFromRecord(user), nil
		}
	}
	name := string(ghost.ID)
	return &bridgev2.UserInfo{Name: &name}, nil
}

func (ic *InlineClient) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{
		MaxTextLength: 5000,
		File: event.FileFeatureMap{
			event.MsgImage: {
				MimeTypes: map[string]event.CapabilitySupportLevel{
					"image/*": event.CapLevelFullySupported,
				},
				Caption:          event.CapLevelFullySupported,
				MaxCaptionLength: 5000,
				MaxSize:          int64(maxInlineMediaDownload),
			},
			event.MsgVideo: {
				MimeTypes: map[string]event.CapabilitySupportLevel{
					"video/*": event.CapLevelFullySupported,
				},
				Caption:          event.CapLevelFullySupported,
				MaxCaptionLength: 5000,
				MaxSize:          int64(maxInlineMediaDownload),
			},
			event.MsgAudio: {
				MimeTypes: map[string]event.CapabilitySupportLevel{
					"audio/*": event.CapLevelFullySupported,
				},
				Caption:          event.CapLevelPartialSupport,
				MaxCaptionLength: 5000,
				MaxSize:          int64(maxInlineMediaDownload),
			},
			event.MsgFile: {
				MimeTypes: map[string]event.CapabilitySupportLevel{
					"*/*": event.CapLevelFullySupported,
				},
				Caption:          event.CapLevelFullySupported,
				MaxCaptionLength: 5000,
				MaxSize:          int64(maxInlineMediaDownload),
			},
		},
		Reply:                event.CapLevelPartialSupport,
		Edit:                 event.CapLevelFullySupported,
		Delete:               event.CapLevelFullySupported,
		Reaction:             event.CapLevelFullySupported,
		ReactionCount:        1,
		CustomEmojiReactions: true,
		ReadReceipts:         true,
		TypingNotifications:  true,
	}
}

func (ic *InlineClient) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	userID, err := parseInlineUserID(identifier)
	if err != nil {
		return nil, err
	}
	resp := &bridgev2.ResolveIdentifierResponse{
		UserID:   makeUserID(strconv.FormatInt(userID, 10)),
		UserInfo: ic.userInfoForID(userID),
	}
	if createChat {
		chat, err := ic.createDMWithUserID(ctx, userID)
		if err != nil {
			return nil, err
		}
		resp.Chat = chat
	}
	return resp, nil
}

func (ic *InlineClient) CreateChatWithGhost(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.CreateChatResponse, error) {
	if ghost == nil {
		return nil, fmt.Errorf("missing Inline ghost")
	}
	userID, err := parseInlineUserID(string(ghost.ID))
	if err != nil {
		return nil, err
	}
	return ic.createDMWithUserID(ctx, userID)
}

func (ic *InlineClient) CreateGroup(ctx context.Context, params *bridgev2.GroupCreateParams) (*bridgev2.CreateChatResponse, error) {
	if params == nil {
		return nil, fmt.Errorf("missing Inline group create params")
	}
	var title *string
	if params.Name != nil {
		title = optionalString(params.Name.Name)
	}
	var description *string
	if params.Topic != nil {
		description = optionalString(params.Topic.Topic)
	}
	participants := make([]sidecar.ChatCreateParticipant, 0, len(params.Participants))
	participantIDs := make([]networkid.UserID, 0, len(params.Participants)+1)
	for _, participant := range params.Participants {
		userID, err := parseInlineUserID(string(participant))
		if err != nil {
			return nil, fmt.Errorf("invalid Inline participant %q: %w", participant, err)
		}
		participants = append(participants, sidecar.ChatCreateParticipant{UserID: userID})
		participantIDs = append(participantIDs, makeUserID(strconv.FormatInt(userID, 10)))
	}
	chat, err := ic.Sidecar.CreateThread(ctx, sidecar.CreateThreadRequest{
		Title:        title,
		Description:  description,
		IsPublic:     false,
		Participants: participants,
	})
	if err != nil {
		return nil, fmt.Errorf("Inline sidecar create thread failed: %w", err)
	}
	return ic.createChatResponse(ctx, chat, ic.chatInfoForCreatedChat(ctx, chat, participantIDs)), nil
}

func (ic *InlineClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	chatID, err := strconv.ParseInt(string(msg.Portal.ID), 10, 64)
	if err != nil {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("Inline portal ID %q is not a numeric chat ID yet", msg.Portal.ID)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}

	switch effectiveMatrixMessageType(msg.Content) {
	case event.MsgText:
		return ic.handleMatrixTextMessage(ctx, msg, chatID)
	case event.MsgImage, event.MsgVideo, event.MsgAudio, event.MsgFile:
		return ic.handleMatrixMediaMessage(ctx, msg, chatID)
	default:
		return nil, bridgev2.ErrUnsupportedMessageType
	}
}

func (ic *InlineClient) handleMatrixTextMessage(ctx context.Context, msg *bridgev2.MatrixMessage, chatID int64) (*bridgev2.MatrixMessageResponse, error) {
	request := sidecar.SendTextRequest{
		Peer:       sidecar.ChatPeer(chatID),
		Text:       msg.Content.Body,
		ExternalID: matrixExternalID(msg),
		RandomID:   matrixRandomID(msg),
	}
	if msg.ReplyTo != nil {
		_, replyToMessageID, err := parseMessageID(msg.ReplyTo.ID)
		if err == nil {
			request.ReplyToMessageID = &replyToMessageID
		}
	}

	mutation, err := ic.Sidecar.SendText(ctx, request)
	if err != nil {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("Inline sidecar send failed: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusGenericError)
	}
	return ic.matrixSendResponse(msg, chatID, mutation)
}

func (ic *InlineClient) handleMatrixMediaMessage(ctx context.Context, msg *bridgev2.MatrixMessage, chatID int64) (*bridgev2.MatrixMessageResponse, error) {
	data, err := ic.UserLogin.Bridge.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)
	if err != nil {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("failed to download Matrix media: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}
	if len(data) == 0 {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("Matrix media download returned no bytes")).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}
	if len(data) > maxInlineMediaDownload {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("Matrix media is too large for current Inline bridge upload limit")).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}

	request := sidecar.UploadRequest{
		Peer:       sidecar.ChatPeer(chatID),
		Kind:       sidecarUploadKindForMatrix(msg.Content),
		FileName:   optionalString(sanitizeMatrixFileName(msg.Content.GetFileName())),
		MimeType:   optionalString(matrixMediaMimeType(msg.Content)),
		SizeBytes:  uint64Ptr(uint64(len(data))),
		Caption:    optionalString(msg.Content.GetCaption()),
		ExternalID: matrixExternalID(msg),
		RandomID:   matrixRandomID(msg),
	}
	if msg.Content.Info != nil {
		request.Width = uint32PtrFromInt(msg.Content.Info.Width)
		request.Height = uint32PtrFromInt(msg.Content.Info.Height)
		request.DurationMS = uint64PtrFromInt(msg.Content.Info.Duration)
	}
	if msg.ReplyTo != nil {
		_, replyToMessageID, err := parseMessageID(msg.ReplyTo.ID)
		if err == nil {
			request.ReplyToMessageID = &replyToMessageID
		}
	}

	mutation, err := ic.Sidecar.Upload(ctx, request, data)
	if err != nil {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("Inline sidecar media send failed: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusGenericError)
	}
	return ic.matrixSendResponse(msg, chatID, mutation)
}

func (ic *InlineClient) matrixSendResponse(msg *bridgev2.MatrixMessage, chatID int64, mutation *sidecar.MessageMutation) (*bridgev2.MatrixMessageResponse, error) {
	if mutation.MessageID == nil {
		txnID := networkid.TransactionID(mutation.Transaction.TransactionID)
		if txnID == "" {
			return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("Inline sidecar send returned neither message ID nor transaction ID")).
				WithErrorAsMessage().
				WithIsCertain(true).
				WithSendNotice(true).
				WithErrorReason(event.MessageStatusGenericError)
		}
		msg.AddPendingToSave(&database.Message{
			ID:        networkid.MessageID("pending/" + string(txnID)),
			SenderID:  ic.GetUserID(),
			Timestamp: time.Now(),
		}, txnID, nil)
		return &bridgev2.MatrixMessageResponse{Pending: true}, nil
	}

	txnID := networkid.TransactionID(mutation.Transaction.TransactionID)
	if txnID != "" {
		msg.AddPendingToIgnore(txnID)
	}

	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:        makeMessageID(chatID, *mutation.MessageID),
			SenderID:  ic.GetUserID(),
			Timestamp: time.Now(),
		},
		RemovePending: txnID,
	}, nil
}

func (ic *InlineClient) HandleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	if msg.Content.MsgType != event.MsgText {
		return bridgev2.ErrUnsupportedMessageType
	}
	chatID, messageID, err := parseMessageID(msg.EditTarget.ID)
	if err != nil {
		return bridgev2.WrapErrorInStatus(fmt.Errorf("Inline edit target is invalid: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}
	if err := ic.Sidecar.EditMessage(ctx, sidecar.EditMessageRequest{
		ChatID:     chatID,
		MessageID:  messageID,
		Text:       msg.Content.Body,
		ExternalID: matrixEventExternalID(msg.Event),
	}); err != nil {
		return bridgev2.WrapErrorInStatus(fmt.Errorf("Inline sidecar edit failed: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusGenericError)
	}
	msg.EditTarget.EditCount++
	return nil
}

func (ic *InlineClient) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	chatID, messageID, err := parseMessageID(msg.TargetMessage.ID)
	if err != nil {
		return bridgev2.WrapErrorInStatus(fmt.Errorf("Inline redaction target is invalid: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}
	if err := ic.Sidecar.DeleteMessage(ctx, sidecar.DeleteMessageRequest{
		ChatID:     chatID,
		MessageID:  messageID,
		ExternalID: matrixEventExternalID(msg.Event),
	}); err != nil {
		return bridgev2.WrapErrorInStatus(fmt.Errorf("Inline sidecar delete failed: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusGenericError)
	}
	return nil
}

func (ic *InlineClient) PreHandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	reaction := strings.TrimSpace(msg.Content.RelatesTo.Key)
	if reaction == "" {
		return bridgev2.MatrixReactionPreResponse{}, bridgev2.WrapErrorInStatus(fmt.Errorf("Inline reactions require a non-empty emoji")).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}
	return bridgev2.MatrixReactionPreResponse{
		SenderID:     ic.GetUserID(),
		EmojiID:      networkid.EmojiID(reaction),
		Emoji:        reaction,
		MaxReactions: 1,
	}, nil
}

func (ic *InlineClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	chatID, messageID, err := parseMessageID(msg.TargetMessage.ID)
	if err != nil {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("Inline reaction target is invalid: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}
	reaction := strings.TrimSpace(msg.Content.RelatesTo.Key)
	if err := ic.Sidecar.React(ctx, sidecar.ReactRequest{
		ChatID:     chatID,
		MessageID:  messageID,
		Reaction:   reaction,
		Remove:     false,
		ExternalID: matrixEventExternalID(msg.Event),
	}); err != nil {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("Inline sidecar reaction failed: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusGenericError)
	}
	return &database.Reaction{}, nil
}

func (ic *InlineClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	chatID, messageID, err := parseMessageID(msg.TargetReaction.MessageID)
	if err != nil {
		return bridgev2.WrapErrorInStatus(fmt.Errorf("Inline reaction remove target is invalid: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}
	reaction := strings.TrimSpace(string(msg.TargetReaction.EmojiID))
	if reaction == "" {
		reaction = strings.TrimSpace(msg.TargetReaction.Emoji)
	}
	if reaction == "" {
		return bridgev2.WrapErrorInStatus(fmt.Errorf("Inline reaction remove is missing emoji metadata")).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}
	if err := ic.Sidecar.React(ctx, sidecar.ReactRequest{
		ChatID:     chatID,
		MessageID:  messageID,
		Reaction:   reaction,
		Remove:     true,
		ExternalID: matrixEventExternalID(msg.Event),
	}); err != nil {
		return bridgev2.WrapErrorInStatus(fmt.Errorf("Inline sidecar reaction remove failed: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusGenericError)
	}
	return nil
}

func (ic *InlineClient) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error {
	chatID, err := strconv.ParseInt(string(msg.Portal.ID), 10, 64)
	if err != nil {
		return fmt.Errorf("Inline portal ID %q is not a numeric chat ID yet: %w", msg.Portal.ID, err)
	}
	var maxMessageID *int64
	if msg.ExactMessage != nil {
		_, parsedMessageID, err := parseMessageID(msg.ExactMessage.ID)
		if err == nil {
			maxMessageID = &parsedMessageID
		}
	}
	if err := ic.Sidecar.Read(ctx, sidecar.ReadRequest{
		ChatID:       chatID,
		MaxMessageID: maxMessageID,
	}); err != nil {
		return fmt.Errorf("Inline sidecar read failed: %w", err)
	}
	return nil
}

func (ic *InlineClient) HandleMatrixViewingChat(ctx context.Context, msg *bridgev2.MatrixViewingChat) error {
	if msg.Portal == nil {
		return nil
	}
	chatID, err := chatIDFromPortal(msg.Portal)
	if err != nil {
		return err
	}
	if err := ic.Sidecar.Read(ctx, sidecar.ReadRequest{ChatID: chatID}); err != nil {
		return fmt.Errorf("Inline sidecar read failed: %w", err)
	}
	return nil
}

func (ic *InlineClient) HandleMatrixTyping(ctx context.Context, msg *bridgev2.MatrixTyping) error {
	if msg.Portal == nil || msg.Type != bridgev2.TypingTypeText {
		return nil
	}
	chatID, err := strconv.ParseInt(string(msg.Portal.ID), 10, 64)
	if err != nil {
		return fmt.Errorf("Inline portal ID %q is not a numeric chat ID yet: %w", msg.Portal.ID, err)
	}
	if err := ic.Sidecar.Typing(ctx, sidecar.TypingRequest{
		ChatID:   chatID,
		IsTyping: msg.IsTyping,
	}); err != nil {
		return fmt.Errorf("Inline sidecar typing failed: %w", err)
	}
	return nil
}

func (ic *InlineClient) startSidecarLoops(ctx context.Context) {
	ic.mu.Lock()
	if ic.runCancel != nil {
		ic.mu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	ic.runCancel = cancel
	ic.wg.Add(2)
	ic.mu.Unlock()

	go ic.syncStartup(runCtx)
	go ic.consumeSidecarEvents(runCtx)
}

func (ic *InlineClient) syncStartup(ctx context.Context) {
	defer ic.wg.Done()

	if err := ic.syncDialogs(ctx); err != nil && !errors.Is(err, context.Canceled) {
		ic.UserLogin.Bridge.Log.Warn().Err(err).Msg("Inline startup sync failed")
	}
}

func (ic *InlineClient) syncDialogs(ctx context.Context) error {
	limit := uint32(startupDialogPageLimit)
	cursor := ""
	seenCursors := make(map[string]struct{})

	for {
		page, err := ic.Sidecar.Dialogs(ctx, sidecar.DialogsRequest{
			Limit:  &limit,
			Cursor: cursor,
		})
		if err != nil {
			return err
		}
		ic.cacheUsers(page.Users)

		for _, dialog := range page.Dialogs {
			if err := ctx.Err(); err != nil {
				return err
			}
			ic.queueDialogResync(ctx, dialog)
			ic.syncRecentHistory(ctx, dialog)
		}

		if page.NextCursor == "" {
			return nil
		}
		if _, ok := seenCursors[page.NextCursor]; ok {
			return nil
		}
		seenCursors[page.NextCursor] = struct{}{}
		cursor = page.NextCursor
	}
}

func (ic *InlineClient) syncRecentHistory(ctx context.Context, dialog sidecar.DialogRecord) {
	if !dialogNeedsStartupHistory(dialog) {
		return
	}
	limit := uint32(startupHistoryLimit)
	request := sidecar.HistoryRequest{
		ChatID: dialog.ChatID,
		Limit:  &limit,
	}
	if dialog.SyncedThroughMessageID != nil {
		request.AfterMessageID = dialog.SyncedThroughMessageID
	}
	page, err := ic.Sidecar.History(ctx, request)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			ic.UserLogin.Bridge.Log.Warn().Err(err).Int64("chat_id", dialog.ChatID).Msg("Failed to fetch Inline startup history")
		}
		return
	}
	ic.cacheUsers(page.Users)

	sort.SliceStable(page.Messages, func(i, j int) bool {
		left, right := page.Messages[i], page.Messages[j]
		if left.Timestamp != right.Timestamp {
			return left.Timestamp < right.Timestamp
		}
		return left.MessageID < right.MessageID
	})

	for _, msg := range page.Messages {
		if err := ctx.Err(); err != nil {
			return
		}
		ic.queueInlineMessage(msg)
	}
}

func dialogNeedsStartupHistory(dialog sidecar.DialogRecord) bool {
	if dialog.LastMessageID == nil {
		return false
	}
	if dialog.SyncedThroughMessageID == nil {
		return true
	}
	return *dialog.LastMessageID > *dialog.SyncedThroughMessageID
}

func (ic *InlineClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if params.ThreadRoot != "" {
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}

	request, err := historyRequestForBackfill(params)
	if err != nil {
		return nil, err
	}
	page, err := ic.Sidecar.History(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch Inline history: %w", err)
	}
	ic.cacheUsers(page.Users)

	messages, cursor := ic.backfillMessagesFromHistory(ctx, params.Portal, page.Messages)
	hasMore := page.HasMore && cursor != ""
	return &bridgev2.FetchMessagesResponse{
		Messages: messages,
		Cursor:   cursor,
		HasMore:  hasMore,
		Forward:  params.Forward,
		MarkRead: true,
	}, nil
}

func historyRequestForBackfill(params bridgev2.FetchMessagesParams) (sidecar.HistoryRequest, error) {
	chatID, err := chatIDFromPortal(params.Portal)
	if err != nil {
		return sidecar.HistoryRequest{}, err
	}
	limit := backfillLimit(params.Count)
	request := sidecar.HistoryRequest{
		ChatID: chatID,
		Limit:  limit,
	}
	if params.Forward {
		if params.AnchorMessage != nil && params.AnchorMessage.ID != "" {
			_, messageID, err := parseBackfillAnchor(chatID, params.AnchorMessage.ID)
			if err != nil {
				return sidecar.HistoryRequest{}, err
			}
			request.AfterMessageID = &messageID
		}
		return request, nil
	}

	if params.Cursor != "" {
		_, messageID, err := parseBackfillAnchor(chatID, networkid.MessageID(params.Cursor))
		if err != nil {
			return sidecar.HistoryRequest{}, err
		}
		request.BeforeMessageID = &messageID
		return request, nil
	}
	if params.AnchorMessage != nil && params.AnchorMessage.ID != "" {
		_, messageID, err := parseBackfillAnchor(chatID, params.AnchorMessage.ID)
		if err != nil {
			return sidecar.HistoryRequest{}, err
		}
		request.BeforeMessageID = &messageID
	}
	return request, nil
}

func parseBackfillAnchor(chatID int64, id networkid.MessageID) (int64, int64, error) {
	anchorChatID, messageID, err := parseMessageID(id)
	if err != nil {
		return 0, 0, err
	}
	if anchorChatID != chatID {
		return 0, 0, fmt.Errorf("backfill anchor chat ID %d does not match portal chat ID %d", anchorChatID, chatID)
	}
	return anchorChatID, messageID, nil
}

func backfillLimit(count int) *uint32 {
	limit := uint32(50)
	if count > 0 {
		limit = uint32(count)
	}
	return &limit
}

func (ic *InlineClient) backfillMessagesFromHistory(ctx context.Context, portal *bridgev2.Portal, records []sidecar.MessageRecord) ([]*bridgev2.BackfillMessage, networkid.PaginationCursor) {
	sort.SliceStable(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if left.Timestamp != right.Timestamp {
			return left.Timestamp < right.Timestamp
		}
		return left.MessageID < right.MessageID
	})

	backfillMessages := make([]*bridgev2.BackfillMessage, 0, len(records))
	var cursor networkid.PaginationCursor
	for _, record := range records {
		messageID := makeMessageID(record.ChatID, record.MessageID)
		if cursor == "" {
			cursor = networkid.PaginationCursor(messageID)
		}

		sender := bridgev2.EventSender{
			Sender:   makeUserID(strconv.FormatInt(record.SenderID, 10)),
			IsFromMe: record.IsOutgoing,
		}
		var intent bridgev2.MatrixAPI
		if portal != nil && portal.Bridge != nil {
			var ok bool
			intent, ok = portal.GetIntentFor(ctx, sender, ic.UserLogin, bridgev2.RemoteEventMessage)
			if !ok {
				continue
			}
		}
		converted, err := convertInlineMessage(ctx, portal, intent, record)
		if err != nil {
			ic.logBackfillConversionError(err, record)
			continue
		}
		if converted == nil {
			continue
		}
		backfillMessages = append(backfillMessages, &bridgev2.BackfillMessage{
			ConvertedMessage: converted,
			Sender:           sender,
			ID:               messageID,
			TxnID:            transactionIDForMessage(record),
			Timestamp:        inlineTimestamp(record.Timestamp),
		})
	}
	return backfillMessages, cursor
}

func (ic *InlineClient) logBackfillConversionError(err error, record sidecar.MessageRecord) {
	if ic == nil || ic.UserLogin == nil || ic.UserLogin.Bridge == nil {
		return
	}
	ic.UserLogin.Bridge.Log.Warn().Err(err).Int64("chat_id", record.ChatID).Int64("message_id", record.MessageID).Msg("Failed to convert Inline message for backfill")
}

func (ic *InlineClient) consumeSidecarEvents(ctx context.Context) {
	defer ic.wg.Done()

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		stream, err := ic.Sidecar.Events(ctx)
		if err != nil {
			if err := ic.waitBeforeEventReconnect(ctx, err); err != nil {
				return
			}
			continue
		}

		if err := ic.refreshSidecarStatus(ctx); err != nil {
			_ = stream.Close(websocket.StatusNormalClosure, "status check failed")
			if err := ic.waitBeforeEventReconnect(ctx, err); err != nil {
				return
			}
			break
		}
		for {
			envelope, err := stream.Recv(ctx)
			if err != nil {
				_ = stream.Close(websocket.StatusNormalClosure, "reconnect")
				if err := ic.waitBeforeEventReconnect(ctx, err); err != nil {
					return
				}
				break
			}
			ic.handleSidecarEvent(envelope)
		}
	}
}

func (ic *InlineClient) refreshSidecarStatus(ctx context.Context) error {
	current, err := ic.Sidecar.Status(ctx)
	if err != nil {
		return err
	}
	ic.handleStatusChanged(&sidecar.StatusChangedEvent{
		Status:  current.Status,
		Failure: current.Failure,
	})
	return nil
}

func (ic *InlineClient) waitBeforeEventReconnect(ctx context.Context, err error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ic.UserLogin.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateTransientDisconnect,
		Error:      "inline-sidecar-events-disconnected",
		Message:    err.Error(),
		UserAction: status.UserActionRestart,
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(sidecarEventReconnectLag):
		return nil
	}
}

func (ic *InlineClient) handleSidecarEvent(envelope *sidecar.EventEnvelope) {
	if envelope == nil {
		return
	}
	switch {
	case envelope.Event.StatusChanged != nil:
		ic.handleStatusChanged(envelope.Event.StatusChanged)
	case envelope.Event.MessageStored != nil:
		ic.queueInlineMessage(envelope.Event.MessageStored.Message)
	case envelope.Event.MessageDeleted != nil:
		ic.queueInlineMessageDelete(envelope.Event.MessageDeleted.ChatID, envelope.Event.MessageDeleted.MessageID)
	case envelope.Event.ReactionChanged != nil:
		ic.queueInlineReaction(envelope.Event.ReactionChanged)
	case envelope.Event.Typing != nil:
		ic.queueInlineTyping(envelope.Event.Typing)
	}
}

func (ic *InlineClient) handleStatusChanged(evt *sidecar.StatusChangedEvent) {
	switch evt.Status {
	case sidecar.StatusConnected:
		ic.setLoggedIn(true)
		ic.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
	case sidecar.StatusAuthRequired, sidecar.StatusAuthExpired, sidecar.StatusLoggedOut:
		ic.setLoggedIn(false)
		ic.markBadCredentials("inline-auth-required", "Inline session needs relogin")
	default:
		ic.setLoggedIn(false)
		bridgeState := status.BridgeState{
			StateEvent: status.StateTransientDisconnect,
			Error:      "inline-not-connected",
			Message:    fmt.Sprintf("Inline sidecar status is %s", evt.Status),
			UserAction: status.UserActionRestart,
		}
		if evt.Failure != nil {
			bridgeState.Message = evt.Failure.Message
		}
		ic.UserLogin.BridgeState.Send(bridgeState)
	}
}

func (ic *InlineClient) queueDialogResync(ctx context.Context, dialog sidecar.DialogRecord) {
	chatName := strconv.FormatInt(dialog.ChatID, 10)
	if dialog.Title != nil && *dialog.Title != "" {
		chatName = *dialog.Title
	}
	chatInfo, err := ic.chatInfoForChat(ctx, dialog.ChatID, &chatName)
	if err != nil {
		ic.UserLogin.Bridge.Log.Warn().Err(err).Int64("chat_id", dialog.ChatID).Msg("Failed to fetch Inline members for chat resync")
		chatInfo = &bridgev2.ChatInfo{
			Name:        &chatName,
			CanBackfill: true,
		}
	}
	ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatResync,
			PortalKey:    ic.portalKeyForChat(dialog.ChatID),
			Timestamp:    time.Now(),
			CreatePortal: true,
		},
		ChatInfo: chatInfo,
	})
}

func (ic *InlineClient) cacheUsers(users []sidecar.UserRecord) {
	if len(users) == 0 {
		return
	}
	ic.mu.Lock()
	defer ic.mu.Unlock()
	if ic.users == nil {
		ic.users = make(map[int64]sidecar.UserRecord, len(users))
	}
	for _, user := range users {
		if user.UserID <= 0 {
			continue
		}
		ic.users[user.UserID] = user
	}
}

func (ic *InlineClient) cachedUser(userID int64) (sidecar.UserRecord, bool) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	user, ok := ic.users[userID]
	return user, ok
}

func (ic *InlineClient) chatInfoForChat(ctx context.Context, chatID int64, name *string) (*bridgev2.ChatInfo, error) {
	page, err := ic.Sidecar.ChatParticipants(ctx, sidecar.ChatParticipantsRequest{ChatID: chatID})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch Inline chat participants: %w", err)
	}
	ic.cacheUsers(page.Users)
	return &bridgev2.ChatInfo{
		Name:        name,
		Members:     ic.chatMemberListFromParticipants(ctx, page),
		CanBackfill: true,
	}, nil
}

func (ic *InlineClient) chatMemberListFromParticipants(ctx context.Context, page *sidecar.ChatParticipantsPage) *bridgev2.ChatMemberList {
	if page == nil {
		return nil
	}
	members := make(bridgev2.ChatMemberMap, len(page.Participants))
	for _, participant := range page.Participants {
		if participant.UserID <= 0 {
			continue
		}
		userID := makeUserID(strconv.FormatInt(participant.UserID, 10))
		member := bridgev2.ChatMember{
			EventSender: bridgev2.EventSender{
				Sender:   userID,
				IsFromMe: ic.IsThisUser(ctx, userID),
			},
			Membership: event.MembershipJoin,
		}
		if user, ok := ic.cachedUser(participant.UserID); ok {
			member.UserInfo = userInfoFromRecord(user)
		} else {
			name := strconv.FormatInt(participant.UserID, 10)
			member.UserInfo = &bridgev2.UserInfo{Name: &name}
		}
		members[userID] = member
	}

	memberList := &bridgev2.ChatMemberList{
		IsFull:                     true,
		CheckAllLogins:             true,
		ExcludeChangesFromTimeline: true,
		TotalMemberCount:           len(members),
		MemberMap:                  members,
	}
	if len(members) == 2 {
		for userID := range members {
			if !ic.IsThisUser(ctx, userID) {
				memberList.OtherUserID = userID
				break
			}
		}
	}
	return memberList
}

func (ic *InlineClient) createDMWithUserID(ctx context.Context, userID int64) (*bridgev2.CreateChatResponse, error) {
	chat, err := ic.Sidecar.CreateDM(ctx, sidecar.CreateDMRequest{UserID: userID})
	if err != nil {
		return nil, fmt.Errorf("Inline sidecar create DM failed: %w", err)
	}
	participantIDs := []networkid.UserID{makeUserID(strconv.FormatInt(userID, 10))}
	return ic.createChatResponse(ctx, chat, ic.chatInfoForCreatedChat(ctx, chat, participantIDs)), nil
}

func (ic *InlineClient) createChatResponse(ctx context.Context, chat *sidecar.CreatedChat, info *bridgev2.ChatInfo) *bridgev2.CreateChatResponse {
	if chat == nil {
		return &bridgev2.CreateChatResponse{}
	}
	return &bridgev2.CreateChatResponse{
		PortalKey:  ic.portalKeyForChat(chat.ChatID),
		PortalInfo: info,
	}
}

func (ic *InlineClient) chatInfoForCreatedChat(ctx context.Context, chat *sidecar.CreatedChat, participantIDs []networkid.UserID) *bridgev2.ChatInfo {
	var name *string
	if chat != nil && chat.Title != nil {
		name = optionalString(*chat.Title)
	}
	members := make(bridgev2.ChatMemberMap, len(participantIDs)+1)
	if selfID := ic.GetUserID(); selfID != "" {
		members[selfID] = ic.chatMemberForUserID(ctx, selfID)
	}
	for _, userID := range participantIDs {
		if userID == "" {
			continue
		}
		members[userID] = ic.chatMemberForUserID(ctx, userID)
		if name == nil && !ic.IsThisUser(ctx, userID) {
			if parsed, err := strconv.ParseInt(string(userID), 10, 64); err == nil {
				if user, ok := ic.cachedUser(parsed); ok {
					displayName := inlineUserDisplayName(user)
					name = &displayName
				}
			}
		}
	}
	memberList := &bridgev2.ChatMemberList{
		IsFull:                     true,
		CheckAllLogins:             true,
		ExcludeChangesFromTimeline: true,
		TotalMemberCount:           len(members),
		MemberMap:                  members,
	}
	if len(members) == 2 {
		for userID := range members {
			if !ic.IsThisUser(ctx, userID) {
				memberList.OtherUserID = userID
				break
			}
		}
	}
	return &bridgev2.ChatInfo{
		Name:        name,
		Members:     memberList,
		CanBackfill: true,
	}
}

func (ic *InlineClient) chatMemberForUserID(ctx context.Context, userID networkid.UserID) bridgev2.ChatMember {
	member := bridgev2.ChatMember{
		EventSender: bridgev2.EventSender{
			Sender:   userID,
			IsFromMe: ic.IsThisUser(ctx, userID),
		},
		Membership: event.MembershipJoin,
	}
	if parsed, err := strconv.ParseInt(string(userID), 10, 64); err == nil {
		member.UserInfo = ic.userInfoForID(parsed)
	}
	return member
}

func (ic *InlineClient) userInfoForID(userID int64) *bridgev2.UserInfo {
	if user, ok := ic.cachedUser(userID); ok {
		return userInfoFromRecord(user)
	}
	name := strconv.FormatInt(userID, 10)
	return &bridgev2.UserInfo{Name: &name}
}

func (ic *InlineClient) queueInlineMessage(msg sidecar.MessageRecord) {
	ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, ic.remoteMessageEvent(msg))
}

func (ic *InlineClient) queueInlineMessageDelete(chatID, messageID int64) {
	ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.MessageRemove{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventMessageRemove,
			PortalKey: ic.portalKeyForChat(chatID),
			Timestamp: time.Now(),
		},
		TargetMessage: makeMessageID(chatID, messageID),
	})
}

func (ic *InlineClient) queueInlineReaction(evt *sidecar.ReactionChangedEvent) {
	if evt == nil || evt.UserID <= 0 || strings.TrimSpace(evt.Reaction) == "" {
		return
	}
	eventType := bridgev2.RemoteEventReaction
	if evt.Removed {
		eventType = bridgev2.RemoteEventReactionRemove
	}
	userID := strconv.FormatInt(evt.UserID, 10)
	ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.Reaction{
		EventMeta: simplevent.EventMeta{
			Type:      eventType,
			PortalKey: ic.portalKeyForChat(evt.ChatID),
			Sender: bridgev2.EventSender{
				Sender:   makeUserID(userID),
				IsFromMe: userID == ic.AccountID,
			},
			Timestamp: time.Now(),
		},
		TargetMessage: makeMessageID(evt.ChatID, evt.MessageID),
		EmojiID:       networkid.EmojiID(evt.Reaction),
		Emoji:         evt.Reaction,
	})
}

func (ic *InlineClient) queueInlineTyping(evt *sidecar.TypingEvent) {
	if evt == nil {
		return
	}
	timeout := 0 * time.Second
	if evt.IsTyping {
		timeout = 5 * time.Second
	}
	ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.Typing{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventTyping,
			PortalKey: ic.portalKeyForChat(evt.ChatID),
			Sender: bridgev2.EventSender{
				Sender: makeUserID(strconv.FormatInt(evt.UserID, 10)),
			},
			Timestamp: time.Now(),
		},
		Timeout: timeout,
		Type:    bridgev2.TypingTypeText,
	})
}

func (ic *InlineClient) remoteMessageEvent(msg sidecar.MessageRecord) *simplevent.Message[sidecar.MessageRecord] {
	return &simplevent.Message[sidecar.MessageRecord]{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventMessageUpsert,
			PortalKey:    ic.portalKeyForChat(msg.ChatID),
			CreatePortal: true,
			Sender: bridgev2.EventSender{
				Sender:   makeUserID(strconv.FormatInt(msg.SenderID, 10)),
				IsFromMe: msg.IsOutgoing,
			},
			Timestamp: inlineTimestamp(msg.Timestamp),
		},
		Data:               msg,
		ID:                 makeMessageID(msg.ChatID, msg.MessageID),
		TransactionID:      transactionIDForMessage(msg),
		ConvertMessageFunc: convertInlineMessage,
		HandleExistingFunc: upsertExistingInlineMessage,
		ConvertEditFunc:    convertInlineMessageEdit,
		TargetMessage:      makeMessageID(msg.ChatID, msg.MessageID),
	}
}

func upsertExistingInlineMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data sidecar.MessageRecord) (bridgev2.UpsertResult, error) {
	if len(existing) == 0 {
		return bridgev2.UpsertResult{ContinueMessageHandling: true}, nil
	}
	return bridgev2.UpsertResult{
		SubEvents: []bridgev2.RemoteEvent{&simplevent.Message[sidecar.MessageRecord]{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventEdit,
				PortalKey: portal.PortalKey,
				Sender: bridgev2.EventSender{
					Sender:   makeUserID(strconv.FormatInt(data.SenderID, 10)),
					IsFromMe: data.IsOutgoing,
				},
				Timestamp: inlineTimestamp(data.Timestamp),
			},
			Data:            data,
			TargetMessage:   makeMessageID(data.ChatID, data.MessageID),
			ConvertEditFunc: convertInlineMessageEdit,
		}},
	}, nil
}

func convertInlineMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data sidecar.MessageRecord) (*bridgev2.ConvertedMessage, error) {
	converted, err := convertedContentFromInline(ctx, portal, intent, data.Content)
	if err != nil {
		return nil, err
	}
	if data.ReplyToMessageID != nil {
		converted.ReplyTo = &networkid.MessageOptionalPartID{
			MessageID: makeMessageID(data.ChatID, *data.ReplyToMessageID),
		}
	}
	return converted, nil
}

func convertInlineMessageEdit(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data sidecar.MessageRecord) (*bridgev2.ConvertedEdit, error) {
	if len(existing) == 0 {
		return nil, bridgev2.ErrTargetMessageNotFound
	}
	converted, err := convertedContentFromInline(ctx, portal, intent, data.Content)
	if err != nil {
		return nil, err
	}
	if len(converted.Parts) == 0 {
		return &bridgev2.ConvertedEdit{}, nil
	}
	return &bridgev2.ConvertedEdit{
		ModifiedParts: []*bridgev2.ConvertedEditPart{
			converted.Parts[0].ToEditPart(existing[0]),
		},
	}, nil
}

func convertedContentFromInline(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, content sidecar.MessageContent) (*bridgev2.ConvertedMessage, error) {
	msgType := event.MsgText
	body := content.Text

	switch content.Type {
	case "", "text":
		if body == "" {
			body = " "
		}
	case "media":
		return convertedInlineMedia(ctx, portal, intent, content)
	default:
		msgType = event.MsgNotice
		body = unsupportedInlineContentNotice(content)
	}

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: msgType,
				Body:    body,
			},
		}},
	}, nil
}

func convertedInlineMedia(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, content sidecar.MessageContent) (*bridgev2.ConvertedMessage, error) {
	if strings.TrimSpace(content.URL) == "" || portal == nil || intent == nil {
		return convertedInlineMediaUnavailable(content), nil
	}

	data, err := downloadInlineMedia(ctx, content.URL)
	if err != nil {
		return convertedInlineMediaUnavailable(content), nil
	}

	fileName := inlineMediaFileName(content)
	mimeType := inlineMediaMimeType(content)
	mxc, file, err := intent.UploadMedia(ctx, portal.MXID, data, fileName, mimeType)
	if err != nil {
		return nil, fmt.Errorf("failed to upload Inline media to Matrix: %w", err)
	}

	info := &event.FileInfo{
		MimeType: mimeType,
		Size:     len(data),
	}
	if width := uint32PtrToInt(content.Width); width > 0 {
		info.Width = width
	}
	if height := uint32PtrToInt(content.Height); height > 0 {
		info.Height = height
	}
	if duration := uint64PtrToInt(content.DurationMS); duration > 0 {
		info.Duration = duration
	}

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType:  inlineMediaMessageType(content),
				Body:     inlineMediaBody(content, fileName),
				URL:      mxc,
				File:     file,
				FileName: fileName,
				Info:     info,
			},
		}},
	}, nil
}

func downloadInlineMedia(ctx context.Context, rawURL string) ([]byte, error) {
	return downloadInlineURL(ctx, rawURL, maxInlineMediaDownload, "media")
}

func downloadInlineAvatar(ctx context.Context, rawURL string) ([]byte, error) {
	return downloadInlineURL(ctx, rawURL, maxInlineAvatarDownload, "avatar")
}

func downloadInlineURL(ctx context.Context, rawURL string, maxBytes int64, label string) ([]byte, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse %s URL: %w", label, err)
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return nil, fmt.Errorf("unsupported %s URL scheme", label)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build %s request: %w", label, err)
	}
	resp, err := inlineMediaHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", label, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("download %s returned HTTP %d", label, resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", label, err)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%s exceeds %d byte bridge download limit", label, maxBytes)
	}
	return data, nil
}

func convertedInlineMediaUnavailable(content sidecar.MessageContent) *bridgev2.ConvertedMessage {
	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: event.MsgNotice,
				Body:    inlineMediaUnavailableNotice(content),
			},
		}},
	}
}

func inlineMediaMessageType(content sidecar.MessageContent) event.MessageType {
	switch content.Kind {
	case "photo":
		return event.MsgImage
	case "video":
		return event.MsgVideo
	case "voice":
		return event.MsgAudio
	default:
		return event.MsgFile
	}
}

func inlineMediaMimeType(content sidecar.MessageContent) string {
	if content.MimeType != nil && strings.TrimSpace(*content.MimeType) != "" {
		return strings.TrimSpace(*content.MimeType)
	}
	switch content.Kind {
	case "photo":
		return "image/jpeg"
	case "video":
		return "video/mp4"
	case "voice":
		return "audio/ogg"
	default:
		return "application/octet-stream"
	}
}

func inlineMediaFileName(content sidecar.MessageContent) string {
	if content.FileName != nil {
		if fileName := sanitizeMatrixFileName(*content.FileName); fileName != "" {
			return fileName
		}
	}

	id := strings.TrimSpace(content.FileID)
	if id == "" {
		id = "media"
	}
	switch content.Kind {
	case "photo":
		return "inline-photo-" + id + extensionForMime(inlineMediaMimeType(content), ".jpg")
	case "video":
		return "inline-video-" + id + extensionForMime(inlineMediaMimeType(content), ".mp4")
	case "voice":
		return "inline-voice-" + id + extensionForMime(inlineMediaMimeType(content), ".ogg")
	default:
		return "inline-file-" + id
	}
}

func inlineMediaBody(content sidecar.MessageContent, fileName string) string {
	if content.Caption != nil && strings.TrimSpace(*content.Caption) != "" {
		return strings.TrimSpace(*content.Caption)
	}
	return fileName
}

func inlineMediaUnavailableNotice(content sidecar.MessageContent) string {
	displayName := inlineMediaDisplayName(content)
	notice := "[Inline media unavailable]"
	if displayName != "" {
		notice = fmt.Sprintf("[Inline media unavailable: %s]", displayName)
	} else if content.Kind != "" {
		notice = fmt.Sprintf("[Inline %s unavailable]", content.Kind)
	}
	if content.Caption != nil && strings.TrimSpace(*content.Caption) != "" {
		notice += "\n" + strings.TrimSpace(*content.Caption)
	}
	return notice
}

func inlineMediaDisplayName(content sidecar.MessageContent) string {
	if content.FileName != nil {
		if fileName := sanitizeMatrixFileName(*content.FileName); fileName != "" {
			return fileName
		}
	}
	return strings.TrimSpace(content.FileID)
}

func sanitizeMatrixFileName(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "/", "_")
	value = strings.ReplaceAll(value, "\\", "_")
	value = strings.ReplaceAll(value, "\x00", "")
	return value
}

func extensionForMime(mimeType, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "video/webm":
		return ".webm"
	case "audio/mpeg":
		return ".mp3"
	case "audio/mp4", "audio/aac":
		return ".m4a"
	default:
		return fallback
	}
}

func uint32PtrToInt(value *uint32) int {
	if value == nil || *value == 0 {
		return 0
	}
	if uint64(*value) > uint64(maxInt()) {
		return 0
	}
	return int(*value)
}

func uint64PtrToInt(value *uint64) int {
	if value == nil || *value == 0 {
		return 0
	}
	if *value > uint64(maxInt()) {
		return 0
	}
	return int(*value)
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func effectiveMatrixMessageType(content *event.MessageEventContent) event.MessageType {
	if content == nil {
		return ""
	}
	msgType := content.MsgType
	if msgType == event.MsgFile && content.Info != nil {
		mimeType := strings.ToLower(strings.TrimSpace(content.Info.MimeType))
		switch {
		case strings.HasPrefix(mimeType, "image/"):
			return event.MsgImage
		case strings.HasPrefix(mimeType, "video/"):
			return event.MsgVideo
		case strings.HasPrefix(mimeType, "audio/"):
			return event.MsgAudio
		}
	}
	return msgType
}

func sidecarUploadKindForMatrix(content *event.MessageEventContent) string {
	switch effectiveMatrixMessageType(content) {
	case event.MsgImage:
		return "photo"
	case event.MsgVideo:
		return "video"
	case event.MsgAudio:
		return "voice"
	default:
		return "document"
	}
}

func matrixMediaMimeType(content *event.MessageEventContent) string {
	if content != nil && content.Info != nil && strings.TrimSpace(content.Info.MimeType) != "" {
		return strings.TrimSpace(content.Info.MimeType)
	}
	switch effectiveMatrixMessageType(content) {
	case event.MsgImage:
		return "image/jpeg"
	case event.MsgVideo:
		return "video/mp4"
	case event.MsgAudio:
		return "audio/ogg"
	default:
		return "application/octet-stream"
	}
}

func inlineUserDisplayName(user sidecar.UserRecord) string {
	if value := optionalStringValue(user.DisplayName); value != "" {
		return value
	}
	first := optionalStringValue(user.FirstName)
	last := optionalStringValue(user.LastName)
	if first != "" && last != "" {
		return first + " " + last
	}
	if first != "" {
		return first
	}
	if last != "" {
		return last
	}
	if value := optionalStringValue(user.Username); value != "" {
		return value
	}
	return strconv.FormatInt(user.UserID, 10)
}

func inlineUserAvatar(user sidecar.UserRecord) *bridgev2.Avatar {
	rawURL := optionalStringValue(user.AvatarURL)
	if rawURL == "" {
		return nil
	}
	hash := sha256.Sum256([]byte(rawURL))
	avatarID := networkid.AvatarID(fmt.Sprintf("inline-user-%d-%x", user.UserID, hash[:8]))
	return &bridgev2.Avatar{
		ID: avatarID,
		Get: func(ctx context.Context) ([]byte, error) {
			return downloadInlineAvatar(ctx, rawURL)
		},
	}
}

func userInfoFromRecord(user sidecar.UserRecord) *bridgev2.UserInfo {
	name := inlineUserDisplayName(user)
	return &bridgev2.UserInfo{
		Name:   &name,
		Avatar: inlineUserAvatar(user),
		IsBot:  user.IsBot,
	}
}

func optionalStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func uint64Ptr(value uint64) *uint64 {
	if value == 0 {
		return nil
	}
	return &value
}

func uint32PtrFromInt(value int) *uint32 {
	if value <= 0 {
		return nil
	}
	converted := uint32(value)
	if int(converted) != value {
		return nil
	}
	return &converted
}

func uint64PtrFromInt(value int) *uint64 {
	if value <= 0 {
		return nil
	}
	converted := uint64(value)
	return &converted
}

func unsupportedInlineContentNotice(content sidecar.MessageContent) string {
	switch {
	case content.Caption != nil && *content.Caption != "":
		return *content.Caption
	case content.FileName != nil && *content.FileName != "":
		return fmt.Sprintf("[Unsupported Inline %s: %s]", content.Type, *content.FileName)
	case content.Type != "":
		return fmt.Sprintf("[Unsupported Inline %s]", content.Type)
	default:
		return "[Unsupported Inline message]"
	}
}

func (ic *InlineClient) portalKeyForChat(chatID int64) networkid.PortalKey {
	return networkid.PortalKey{
		ID:       makePortalID(strconv.FormatInt(chatID, 10)),
		Receiver: ic.UserLogin.ID,
	}
}

func chatIDFromPortal(portal *bridgev2.Portal) (int64, error) {
	if portal == nil {
		return 0, errors.New("Inline backfill requires a portal")
	}
	chatID, err := strconv.ParseInt(string(portal.PortalKey.ID), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Inline portal ID %q is not a numeric chat ID yet: %w", portal.PortalKey.ID, err)
	}
	return chatID, nil
}

func parseInlineUserID(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("Inline user ID must not be empty")
	}
	userID, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Inline user ID must be numeric: %w", err)
	}
	if userID <= 0 {
		return 0, fmt.Errorf("Inline user ID must be positive")
	}
	return userID, nil
}

func makeMessageID(chatID, messageID int64) networkid.MessageID {
	return networkid.MessageID(strconv.FormatInt(chatID, 10) + "/" + strconv.FormatInt(messageID, 10))
}

func parseMessageID(messageID networkid.MessageID) (chatID, inlineMessageID int64, err error) {
	parts := strings.Split(string(messageID), "/")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected chat/message Inline message ID, got %q", messageID)
	}
	chatID, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse chat ID: %w", err)
	}
	inlineMessageID, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse message ID: %w", err)
	}
	return chatID, inlineMessageID, nil
}

func transactionIDForMessage(msg sidecar.MessageRecord) networkid.TransactionID {
	if msg.Transaction == nil {
		return ""
	}
	return networkid.TransactionID(msg.Transaction.TransactionID)
}

func inlineTimestamp(ts int64) time.Time {
	if ts <= 0 {
		return time.Now()
	}
	return time.Unix(ts, 0)
}

func (ic *InlineClient) setLoggedIn(loggedIn bool) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	ic.loggedIn = loggedIn
}

func (ic *InlineClient) markBadCredentials(code status.BridgeStateErrorCode, message string) {
	ic.UserLogin.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateBadCredentials,
		Error:      code,
		Message:    message,
		UserAction: status.UserActionRelogin,
	})
}

func matrixExternalID(msg *bridgev2.MatrixMessage) *sidecar.ExternalID {
	if msg == nil {
		return nil
	}
	return matrixEventExternalID(msg.Event)
}

func matrixEventExternalID(evt *event.Event) *sidecar.ExternalID {
	if evt == nil || evt.ID == "" {
		return nil
	}
	return &sidecar.ExternalID{
		Source: "matrix-event",
		ID:     string(evt.ID),
	}
}

func matrixRandomID(msg *bridgev2.MatrixMessage) *int64 {
	if msg.Event == nil || msg.Event.ID == "" {
		return nil
	}
	hash := sha256.Sum256([]byte(string(msg.Event.ID) + "\x00" + msg.Content.Body))
	value := int64(binary.BigEndian.Uint64(hash[:8]) & 0x7fff_ffff_ffff_ffff)
	return &value
}
