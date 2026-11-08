package connector

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
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

	"github.com/inline-chat/matrix-inline/pkg/msgconv"
	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

const (
	startupDialogPageLimit    = 100
	startupHistoryLimit       = 20
	dialogSyncRPCBudget       = 45
	dialogPollInterval        = 30 * time.Second
	dialogLiveRefreshInterval = 15 * time.Minute
	dialogRateLimitCooldown   = 2 * time.Minute
	dialogRateLimitMaxDelay   = 15 * time.Minute
	startupRPCThrottle        = 1200 * time.Millisecond
	sidecarEventReconnectLag  = 3 * time.Second
	reconciliationPageLimit   = 500
	reconciliationMaxPages    = 10_000
	bridgeRecoveryRPCBudget   = 24
	bridgeRecoveryPassDelay   = 3 * time.Second
	maxInlineMediaDownload    = 100 << 20
	maxInlineAvatarDownload   = 8 << 20
)

var errInlineRateLimited = errors.New("inline rate limited")
var errBridgeRecoveryIncomplete = errors.New("Inline bridge recovery pass incomplete")

type permanentMediaError struct {
	err error
}

func (err *permanentMediaError) Error() string { return err.err.Error() }
func (err *permanentMediaError) Unwrap() error { return err.err }

func permanentMediaFailure(err error) error {
	if err == nil {
		return nil
	}
	return &permanentMediaError{err: err}
}

func isPermanentMediaFailure(err error) bool {
	var permanent *permanentMediaError
	return errors.As(err, &permanent)
}

var inlineMediaHTTPClient = &http.Client{Timeout: 2 * time.Minute}

type InlineClient struct {
	UserLogin *bridgev2.UserLogin
	AccountID string
	Sidecar   *sidecar.Client

	mu                         sync.RWMutex
	loggedIn                   bool
	users                      map[int64]sidecar.UserRecord
	dialogs                    map[int64]sidecar.DialogRecord
	spaces                     map[int64]sidecar.SpaceRecord
	spaceAccess                map[int64]string
	spaceAccessComplete        bool
	workspaceRevision          uint64
	workspaceProjectedRevision uint64
	details                    map[int64]struct{}
	history                    map[int64]int64
	historyLoaded              map[int64]struct{}
	storeNamespace             string
	lastSidecarSequence        uint64
	sidecarEventGeneration     string
	bridgeStateVersion         uint32
	bridgeRecoveryCursor       int64
	dialogSyncCursor           string
	topicRetryAfter            map[int64]time.Time
	hiddenDialogsDefault       hiddenDialogsPolicy
	hiddenDialogsOverride      hiddenDialogsPolicy
	channelMuteDefault         string
	channelMuteOverride        string
	metadataSaveMu             sync.Mutex

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
var _ bridgev2.MarkedUnreadHandlingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.MuteHandlingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.ChatViewingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.TypingHandlingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.RoomNameHandlingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.MembershipHandlingNetworkAPI = (*InlineClient)(nil)
var _ bridgev2.DeleteChatHandlingNetworkAPI = (*InlineClient)(nil)
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
		ic.ensureRemoteProfile(ctx)
		ic.startSidecarLoops(ctx)
	case sidecar.StatusReconnecting:
		ic.setLoggedIn(true)
		ic.UserLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateTransientDisconnect,
			Error:      "inline-sidecar-reconnecting",
			Message:    "Inline sidecar is reconnecting",
		})
		ic.ensureRemoteProfile(ctx)
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
	if err := ic.Sidecar.Logout(ctx); err != nil {
		ic.UserLogin.Bridge.Log.Warn().Err(err).Msg("Failed to revoke and clear Inline sidecar session during logout")
	}
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
	if strings.TrimSpace(ic.AccountID) != "" {
		return makeUserID(ic.AccountID)
	}
	if ic.UserLogin != nil && ic.UserLogin.ID != "" {
		return networkid.UserID(ic.UserLogin.ID)
	}
	return ""
}

func (ic *InlineClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	if err := ic.guardMigrationPresentation(ctx, portal); err != nil {
		return nil, err
	}
	if strings.HasPrefix(string(portal.ID), "space:") {
		n, err := strconv.ParseInt(strings.TrimPrefix(string(portal.ID), "space:"), 10, 64)
		if err != nil || n <= 0 {
			return nil, errors.New("invalid workspace portal")
		}
		ic.mu.RLock()
		space, ok := ic.spaces[n]
		access := ic.spaceAccess[n]
		ic.mu.RUnlock()
		if !ok || access != "accessible" {
			return nil, errors.New("workspace access unavailable")
		}
		return ic.workspaceChatInfo(space), nil
	}
	chatID, err := chatIDFromPortal(portal)
	if err != nil {
		return nil, err
	}
	name := optionalString(string(portal.ID))
	if dialog, ok := ic.cachedDialog(chatID); ok {
		if displayName := dialogDisplayName(dialog); displayName != "" {
			name = optionalString(displayName)
		}
		if isDMDialog(dialog) {
			return ic.chatInfoForDialog(ctx, dialog, name), nil
		}
	}
	info, err := ic.chatInfoForChat(ctx, chatID, name)
	if err != nil {
		if dialog, ok := ic.cachedDialog(chatID); ok {
			return ic.chatInfoForDialog(ctx, dialog, name), nil
		}
		return nil, err
	}
	return info, nil
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
	features := &event.RoomFeatures{
		ID: "inline-rich-v1",
		Formatting: event.FormattingFeatureMap{
			event.FmtBold:       event.CapLevelFullySupported,
			event.FmtItalic:     event.CapLevelFullySupported,
			event.FmtInlineCode: event.CapLevelFullySupported,
			event.FmtCodeBlock:  event.CapLevelFullySupported,
			event.FmtInlineLink: event.CapLevelFullySupported,
			event.FmtUserLink:   event.CapLevelPartialSupport,
		},
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
		Edit:                 event.CapLevelPartialSupport,
		Delete:               event.CapLevelFullySupported,
		Reaction:             event.CapLevelFullySupported,
		ReactionCount:        1,
		CustomEmojiReactions: false,
		ReadReceipts:         true,
		TypingNotifications:  true,
		MarkAsUnread:         true,
	}
	return features
}

func (ic *InlineClient) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	directory, err := ic.loadDirectory(ctx)
	if err != nil {
		return nil, err
	}
	user, err := directory.resolve(identifier)
	if err != nil {
		return nil, err
	}
	userID := user.Record.UserID
	ic.cacheUsers([]sidecar.UserRecord{user.Record})
	resp := &bridgev2.ResolveIdentifierResponse{
		UserID:   makeUserID(strconv.FormatInt(userID, 10)),
		UserInfo: userInfoFromRecord(user.Record),
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
	resolved, err := ic.ResolveIdentifier(ctx, string(ghost.ID), true)
	if err != nil {
		if _, valid := directoryPositiveID(string(ghost.ID)); valid && (errors.Is(err, errDirectoryNotFound) || errors.Is(err, errDirectoryIncomplete)) {
			return nil, fmt.Errorf("%w: %w", bridgev2.ErrResolveIdentifierTryNext, err)
		}
		return nil, err
	}
	return resolved.Chat, nil
}

func (ic *InlineClient) CreateGroup(ctx context.Context, params *bridgev2.GroupCreateParams) (*bridgev2.CreateChatResponse, error) {
	directory, err := ic.loadDirectory(ctx)
	if err != nil {
		return nil, err
	}
	request, participantIDs, err := validateThreadCreation(params, ic.UserLogin.ID, directory)
	if err != nil {
		return nil, err
	}
	if ic.Sidecar.SessionNamespace != directory.namespace {
		return nil, fmt.Errorf("Inline login changed during creation")
	}
	chat, err := ic.Sidecar.CreateThread(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("Inline sidecar create thread failed: %w", err)
	}
	for _, participant := range request.Participants {
		ic.cacheUsers([]sidecar.UserRecord{directory.users[participant.UserID].Record})
	}
	info := ic.chatInfoForCreatedChat(ctx, chat, participantIDs)
	kind := database.RoomTypeDefault
	info.Type = &kind
	info.Members.OtherUserID = ""
	info.Members.IsFull = false
	if chat != nil {
		ic.rememberDialog(sidecar.DialogRecord{ChatID: chat.ChatID, Title: chat.Title, SpaceID: request.SpaceID, Structure: &sidecar.DialogStructure{Kind: "channel", SpaceID: request.SpaceID}})
	}
	return ic.createChatResponse(ctx, chat, info), nil
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
	if err := validateInlineReplyTarget(msg, chatID); err != nil {
		return nil, bridgev2.WrapErrorInStatus(err).WithErrorAsMessage().WithIsCertain(true).WithSendNotice(true).WithErrorReason(event.MessageStatusUnsupported)
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
	text, entities := msgconv.FromMatrix(msg.Content, ic.outgoingMentionResolver(msg.Content))
	request := sidecar.SendTextRequest{
		Peer:       sidecar.ChatPeer(chatID),
		Text:       text,
		Entities:   entities,
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
			WithIsCertain(sidecarErrorIsCertain(err)).
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

	captionContent := *msg.Content
	captionContent.Body = msg.Content.GetCaption()
	if captionContent.Body == "" {
		captionContent.Format, captionContent.FormattedBody = "", ""
	}
	caption, entities := msgconv.FromMatrix(&captionContent, ic.outgoingMentionResolver(msg.Content))
	request := sidecar.UploadRequest{
		Peer:       sidecar.ChatPeer(chatID),
		Kind:       sidecarUploadKindForMatrix(msg.Content),
		FileName:   optionalString(sanitizeMatrixFileName(msg.Content.GetFileName())),
		MimeType:   optionalString(matrixMediaMimeType(msg.Content)),
		SizeBytes:  uint64Ptr(uint64(len(data))),
		Caption:    optionalString(caption),
		Entities:   entities,
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
			WithIsCertain(sidecarErrorIsCertain(err)).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusGenericError)
	}
	return ic.matrixSendResponse(msg, chatID, mutation)
}

func (ic *InlineClient) matrixSendResponse(msg *bridgev2.MatrixMessage, chatID int64, mutation *sidecar.MessageMutation) (*bridgev2.MatrixMessageResponse, error) {
	if mutation == nil {
		return nil, bridgev2.WrapErrorInStatus(errors.New("Inline sidecar send returned an empty mutation")).
			WithErrorAsMessage().
			WithIsCertain(false).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusGenericError)
	}
	switch mutation.State {
	case sidecar.TransactionFailed, sidecar.TransactionCancelled:
		message := "Inline rejected the message"
		if mutation.Failure != nil && strings.TrimSpace(mutation.Failure.Message) != "" {
			message = mutation.Failure.Message
		}
		return nil, bridgev2.WrapErrorInStatus(errors.New(message)).
			WithErrorAsMessage().
			WithStatus(event.MessageStatusFail).
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusGenericError)
	case sidecar.TransactionQueued, sidecar.TransactionSent, sidecar.TransactionAcked:
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf(
			"Inline send %s is not final yet; retrying with the same transaction",
			mutation.Transaction.TransactionID,
		)).
			WithErrorAsMessage().
			WithIsCertain(false).
			WithSendNotice(false).
			WithErrorReason(event.MessageStatusGenericError)
	case sidecar.TransactionCompleted:
		if mutation.MessageID == nil {
			return nil, bridgev2.WrapErrorInStatus(errors.New("completed Inline send did not include a message ID")).
				WithErrorAsMessage().
				WithIsCertain(false).
				WithSendNotice(true).
				WithErrorReason(event.MessageStatusGenericError)
		}
	default:
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("Inline sidecar send returned unknown transaction state %q", mutation.State)).
			WithErrorAsMessage().
			WithIsCertain(false).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusGenericError)
	}

	txnID := networkid.TransactionID(mutation.Transaction.TransactionID)
	if txnID != "" {
		msg.AddPendingToIgnore(txnID)
	}

	var metadata *MessageMetadata
	if msg.Content != nil && msg.Content.MsgType.IsMedia() {
		media := *msg.Content
		metadata = &MessageMetadata{InlineMedia: &media, MediaHandled: true}
	}
	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:        makeMessageID(chatID, *mutation.MessageID),
			SenderID:  ic.GetUserID(),
			Timestamp: time.Now(),
			Metadata:  metadata,
		},
		RemovePending: txnID,
	}, nil
}

func (ic *InlineClient) HandleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	if msg == nil || msg.Content == nil || msg.EditTarget == nil || msg.Portal == nil || msg.Portal.Portal == nil || msg.EditTarget.Room != msg.Portal.PortalKey {
		return fmt.Errorf("Inline edit target must belong to this conversation and login: %w", bridgev2.ErrUnsupportedMessageType)
	}
	if msg.Content.MsgType != event.MsgText && !msg.Content.MsgType.IsMedia() {
		return bridgev2.ErrUnsupportedMessageType
	}
	if msg.Content.MsgType.IsMedia() && !matrixCaptionEditAllowed(msg.EditTarget, msg.Content) {
		return fmt.Errorf("Inline media replacement or unverified legacy caption edit is unsupported: %w", bridgev2.ErrUnsupportedMessageType)
	}
	chatID, messageID, err := parseMessageID(msg.EditTarget.ID)
	if err != nil {
		return bridgev2.WrapErrorInStatus(fmt.Errorf("Inline edit target is invalid: %w", err)).
			WithErrorAsMessage().
			WithIsCertain(true).
			WithSendNotice(true).
			WithErrorReason(event.MessageStatusUnsupported)
	}
	portalChat, portalErr := chatIDFromPortal(msg.Portal)
	if portalErr != nil || chatID != portalChat || messageID <= 0 {
		return fmt.Errorf("Inline edit target must be a positive message in this conversation: %w", bridgev2.ErrUnsupportedMessageType)
	}
	content := *msg.Content
	if content.MsgType.IsMedia() {
		content.Body = content.GetCaption()
		if content.Body == "" {
			content.Format, content.FormattedBody = "", ""
		}
	}
	text, entities := msgconv.FromMatrix(&content, ic.outgoingMentionResolver(msg.Content))
	if err := ic.Sidecar.EditMessage(ctx, sidecar.EditMessageRequest{
		ChatID:     chatID,
		MessageID:  messageID,
		Text:       text,
		Entities:   entities,
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
	if reaction == "" || strings.Contains(reaction, "://") {
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
	if msg.ExactMessage == nil {
		return errors.New("Inline read receipt has no exact source target")
	}
	targetChat, targetID, err := parseMessageID(msg.ExactMessage.ID)
	if err != nil || targetChat != chatID || targetID <= 0 {
		return errors.New("Inline read receipt target does not match this chat")
	}
	maxMessageID := &targetID
	if err := ic.Sidecar.Read(ctx, sidecar.ReadRequest{
		ChatID:       chatID,
		MaxMessageID: maxMessageID,
	}); err != nil {
		return fmt.Errorf("Inline sidecar read failed: %w", err)
	}
	return nil
}

func (ic *InlineClient) HandleMarkedUnread(ctx context.Context, msg *bridgev2.MatrixMarkedUnread) error {
	chatID, err := chatIDFromPortal(msg.Portal)
	if err != nil {
		return err
	}
	if err := ic.Sidecar.SetMarkedUnread(ctx, sidecar.SetMarkedUnreadRequest{
		ChatID: chatID,
		Unread: msg.Content.Unread,
	}); err != nil {
		return fmt.Errorf("Inline sidecar marked-unread update failed: %w", err)
	}
	return nil
}

func (ic *InlineClient) HandleMute(ctx context.Context, msg *bridgev2.MatrixMute) error {
	chatID, err := chatIDFromPortal(msg.Portal)
	if err != nil {
		return err
	}
	duration := msg.Content.GetMuteDuration()
	if duration > 0 {
		return fmt.Errorf("Inline does not support expiring per-chat mutes")
	}
	var mode *sidecar.DialogNotificationMode
	if duration < 0 {
		muted := sidecar.DialogNotificationNone
		mode = &muted
	}
	if err := ic.Sidecar.UpdateDialogNotifications(ctx, sidecar.UpdateDialogNotificationsRequest{
		ChatID: chatID,
		Mode:   mode,
	}); err != nil {
		return fmt.Errorf("Inline sidecar notification update failed: %w", err)
	}
	return nil
}

func (ic *InlineClient) HandleMatrixRoomName(ctx context.Context, msg *bridgev2.MatrixRoomName) (bool, error) {
	if msg.Portal == nil || msg.Portal.OtherUserID != "" {
		return false, bridgev2.ErrRoomMetadataNotSupported
	}
	title := strings.TrimSpace(msg.Content.Name)
	if title == "" {
		return false, fmt.Errorf("Inline thread names cannot be empty")
	}
	chatID, err := chatIDFromPortal(msg.Portal)
	if err != nil {
		return false, err
	}
	if err := ic.Sidecar.UpdateChatInfo(ctx, sidecar.UpdateChatInfoRequest{
		ChatID: chatID,
		Title:  &title,
	}); err != nil {
		return false, fmt.Errorf("Inline sidecar chat name update failed: %w", err)
	}
	msg.Portal.Name = title
	msg.Portal.NameSet = true
	return true, nil
}

func (ic *InlineClient) HandleMatrixMembership(ctx context.Context, msg *bridgev2.MatrixMembershipChange) (*bridgev2.MatrixMembershipResult, error) {
	if msg.Portal == nil || msg.Portal.OtherUserID != "" {
		return nil, bridgev2.ErrMembershipNotSupported
	}
	ghost, ok := msg.Target.(*bridgev2.Ghost)
	if !ok {
		return nil, bridgev2.ErrMembershipNotSupported
	}
	userID, err := parseInlineUserID(string(ghost.ID))
	if err != nil {
		return nil, fmt.Errorf("invalid Inline membership target %q: %w", ghost.ID, err)
	}
	chatID, err := chatIDFromPortal(msg.Portal)
	if err != nil {
		return nil, err
	}
	switch msg.Type {
	case bridgev2.Invite:
		err = ic.Sidecar.AddChatParticipant(ctx, sidecar.AddChatParticipantRequest{
			ChatID: chatID,
			UserID: userID,
		})
	case bridgev2.Kick, bridgev2.RevokeInvite:
		// Removing a direct participant does not prove group or public-space
		// access was removed. Do not report a Matrix kick as successful until
		// the source can establish the target's effective access afterward.
		return nil, fmt.Errorf("%w: Inline cannot verify that all access was removed", bridgev2.ErrMembershipNotSupported)
	case bridgev2.AcceptInvite, bridgev2.ProfileChange:
		return &bridgev2.MatrixMembershipResult{}, nil
	default:
		return nil, fmt.Errorf("%w: %s to %s", bridgev2.ErrMembershipNotSupported, msg.Type.From, msg.Type.To)
	}
	if err != nil {
		return nil, fmt.Errorf("Inline sidecar membership update failed: %w", err)
	}
	return &bridgev2.MatrixMembershipResult{}, nil
}

func (ic *InlineClient) HandleMatrixDeleteChat(ctx context.Context, msg *bridgev2.MatrixDeleteChat) error {
	if msg.Portal == nil || msg.Portal.OtherUserID != "" {
		return bridgev2.ErrDeleteChatNotSupported
	}
	chatID, err := chatIDFromPortal(msg.Portal)
	if err != nil {
		return err
	}
	if err := ic.Sidecar.DeleteChat(ctx, sidecar.DeleteChatRequest{ChatID: chatID}); err != nil {
		return fmt.Errorf("Inline sidecar chat delete failed: %w", err)
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

	go ic.syncDialogLoop(runCtx)
	go ic.consumeSidecarEvents(runCtx)
}

func (ic *InlineClient) syncDialogLoop(ctx context.Context) {
	defer ic.wg.Done()
	var lastLiveRefresh time.Time

	for {
		recoveringUpgrade := ic.needsBridgeStateRecovery()
		refreshLive := recoveringUpgrade || lastLiveRefresh.IsZero() || time.Since(lastLiveRefresh) >= dialogLiveRefreshInterval
		var err error
		if recoveringUpgrade {
			err = ic.recoverBridgeState(ctx)
		} else {
			err = ic.syncDialogs(ctx, refreshLive)
		}
		if err == nil && refreshLive {
			lastLiveRefresh = time.Now()
		}
		rateLimited := isRateLimitedError(err)
		delay := dialogPollInterval
		if errors.Is(err, errBridgeRecoveryIncomplete) {
			delay = bridgeRecoveryPassDelay
		} else if rateLimited {
			delay = rateLimitRetryDelay(err, dialogRateLimitCooldown)
		}
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errBridgeRecoveryIncomplete) {
			if rateLimited {
				ic.UserLogin.Bridge.Log.Warn().Err(err).Dur("retry_after", delay).Msg("Inline dialog sync rate limited; backing off")
			} else {
				ic.UserLogin.Bridge.Log.Warn().Err(err).Msg("Inline dialog sync failed")
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (ic *InlineClient) needsBridgeStateRecovery() bool {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	return ic.bridgeStateVersion < currentBridgeStateVersion
}

func (ic *InlineClient) recoverBridgeState(ctx context.Context) error {
	ic.mu.RLock()
	fromVersion := ic.bridgeStateVersion
	recoveryCursor := ic.bridgeRecoveryCursor
	ic.mu.RUnlock()
	ic.UserLogin.Bridge.Log.Info().
		Uint32("from_version", fromVersion).
		Uint32("to_version", currentBridgeStateVersion).
		Msg("Starting one-time Inline bridge state recovery")
	complete, err := ic.reconcileBridgeStateSlice(ctx, recoveryCursor, bridgeRecoveryRPCBudget)
	if err != nil {
		return fmt.Errorf("reconcile Inline bridge state during upgrade: %w", err)
	}
	if !complete {
		return errBridgeRecoveryIncomplete
	}
	if err := ic.persistBridgeStateVersion(ctx, currentBridgeStateVersion); err != nil {
		return err
	}
	ic.UserLogin.Bridge.Log.Info().
		Uint32("version", currentBridgeStateVersion).
		Msg("Completed one-time Inline bridge state recovery")
	return nil
}

func (ic *InlineClient) persistBridgeStateVersion(ctx context.Context, version uint32) error {
	ic.metadataSaveMu.Lock()
	defer ic.metadataSaveMu.Unlock()
	ic.mu.Lock()
	previous := ic.bridgeStateVersion
	previousCursor := ic.bridgeRecoveryCursor
	if version <= previous {
		ic.mu.Unlock()
		return nil
	}
	ic.bridgeStateVersion = version
	ic.bridgeRecoveryCursor = 0
	meta, ok := ic.UserLogin.Metadata.(*UserLoginMetadata)
	if ok {
		meta.BridgeStateVersion = version
		meta.BridgeRecoveryCursor = 0
	}
	ic.mu.Unlock()

	if !ok {
		return errors.New("Inline user login metadata is unavailable")
	}
	if err := ic.UserLogin.Save(ctx); err != nil {
		ic.mu.Lock()
		ic.bridgeStateVersion = previous
		ic.bridgeRecoveryCursor = previousCursor
		meta.BridgeStateVersion = previous
		meta.BridgeRecoveryCursor = previousCursor
		ic.mu.Unlock()
		return fmt.Errorf("save Inline bridge state recovery version: %w", err)
	}
	return nil
}

func (ic *InlineClient) persistBridgeRecoveryCursor(ctx context.Context, cursor int64) error {
	if cursor <= 0 {
		return nil
	}
	ic.metadataSaveMu.Lock()
	defer ic.metadataSaveMu.Unlock()
	ic.mu.Lock()
	previous := ic.bridgeRecoveryCursor
	if cursor <= previous {
		ic.mu.Unlock()
		return nil
	}
	ic.bridgeRecoveryCursor = cursor
	meta, ok := ic.UserLogin.Metadata.(*UserLoginMetadata)
	if ok {
		meta.BridgeRecoveryCursor = cursor
	}
	ic.mu.Unlock()
	if !ok {
		return errors.New("Inline user login metadata is unavailable")
	}
	if err := ic.UserLogin.Save(ctx); err != nil {
		ic.mu.Lock()
		ic.bridgeRecoveryCursor = previous
		meta.BridgeRecoveryCursor = previous
		ic.mu.Unlock()
		return fmt.Errorf("save Inline bridge recovery cursor: %w", err)
	}
	return nil
}

func (ic *InlineClient) persistDialogSyncCursor(ctx context.Context, cursor string) error {
	if ic.UserLogin == nil {
		ic.mu.Lock()
		ic.dialogSyncCursor = cursor
		ic.mu.Unlock()
		return nil
	}
	ic.metadataSaveMu.Lock()
	defer ic.metadataSaveMu.Unlock()
	ic.mu.Lock()
	previous := ic.dialogSyncCursor
	if cursor == previous {
		ic.mu.Unlock()
		return nil
	}
	ic.dialogSyncCursor = cursor
	meta, ok := ic.UserLogin.Metadata.(*UserLoginMetadata)
	if ok {
		meta.DialogSyncCursor = cursor
	}
	ic.mu.Unlock()
	if !ok {
		return errors.New("Inline user login metadata is unavailable")
	}
	if err := ic.UserLogin.Save(ctx); err != nil {
		ic.mu.Lock()
		ic.dialogSyncCursor = previous
		meta.DialogSyncCursor = previous
		ic.mu.Unlock()
		return fmt.Errorf("save Inline dialog sync cursor: %w", err)
	}
	return nil
}

func (ic *InlineClient) effectiveHiddenDialogsPolicy() hiddenDialogsPolicy {
	_, _, effective := ic.hiddenDialogsSettings()
	return effective
}

func (ic *InlineClient) hiddenDialogsSettings() (defaults, override, effective hiddenDialogsPolicy) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	defaults = ic.hiddenDialogsDefault
	if defaults == "" {
		defaults = hiddenDialogsExclude
	}
	override = ic.hiddenDialogsOverride
	if override != "" {
		effective = override
	} else {
		effective = defaults
	}
	return
}

func (ic *InlineClient) shouldProjectDialog(dialog sidecar.DialogRecord) bool {
	if ic.effectiveHiddenDialogsPolicy() == hiddenDialogsInclude {
		return true
	}
	return dialog.ChatListHidden == nil || !*dialog.ChatListHidden
}

func (ic *InlineClient) shouldProjectChat(ctx context.Context, chatID int64, refresh bool) (bool, error) {
	if ic.effectiveHiddenDialogsPolicy() == hiddenDialogsInclude {
		return true, nil
	}
	if !refresh {
		if dialog, ok := ic.cachedDialog(chatID); ok {
			return ic.shouldProjectDialog(dialog), nil
		}
	}
	state, err := ic.Sidecar.ChatState(ctx, sidecar.ChatStateRequest{ChatID: chatID})
	if err != nil {
		return false, fmt.Errorf("fetch Inline chat %d visibility: %w", chatID, err)
	}
	if state.Dialog == nil {
		// Unknown/deleted chats may still need deletion or reconciliation events
		// delivered to a portal that already exists.
		return true, nil
	}
	ic.rememberDialog(*state.Dialog)
	return ic.shouldProjectDialog(*state.Dialog), nil
}

func (ic *InlineClient) skipHiddenChatEvent(ctx context.Context, chatID int64, refresh bool, eventType string) (bool, error) {
	project, err := ic.shouldProjectChat(ctx, chatID, refresh)
	if err != nil {
		return false, err
	}
	if project {
		return false, nil
	}
	if ic.UserLogin != nil && ic.UserLogin.Bridge != nil {
		ic.UserLogin.Bridge.Log.Debug().
			Int64("chat_id", chatID).
			Str("inline_event_type", eventType).
			Msg("Skipped Matrix projection for hidden Inline dialog")
	}
	return true, nil
}

func (ic *InlineClient) persistHiddenDialogsOverride(ctx context.Context, policy hiddenDialogsPolicy) error {
	if policy != "" && policy != hiddenDialogsExclude && policy != hiddenDialogsInclude {
		return fmt.Errorf("unsupported hidden dialog policy %q", policy)
	}
	if ic.UserLogin == nil {
		ic.mu.Lock()
		ic.hiddenDialogsOverride = policy
		ic.dialogSyncCursor = ""
		ic.dialogs = make(map[int64]sidecar.DialogRecord)
		ic.mu.Unlock()
		return nil
	}

	ic.metadataSaveMu.Lock()
	defer ic.metadataSaveMu.Unlock()
	meta, ok := ic.UserLogin.Metadata.(*UserLoginMetadata)
	if !ok || meta == nil {
		return errors.New("Inline user login metadata is unavailable")
	}
	previousPolicy := meta.HiddenDialogs
	previousCursor := meta.DialogSyncCursor
	meta.HiddenDialogs = string(policy)
	meta.DialogSyncCursor = ""
	if err := ic.UserLogin.Save(ctx); err != nil {
		meta.HiddenDialogs = previousPolicy
		meta.DialogSyncCursor = previousCursor
		return fmt.Errorf("save Inline hidden dialog setting: %w", err)
	}

	ic.mu.Lock()
	ic.hiddenDialogsOverride = policy
	ic.dialogSyncCursor = ""
	// Force the next stable scan to treat every included dialog as newly seen.
	ic.dialogs = make(map[int64]sidecar.DialogRecord)
	ic.mu.Unlock()
	return nil
}

func (ic *InlineClient) syncDialogs(ctx context.Context, refreshLive bool) error {
	if err := ic.refreshWorkspaceSnapshot(ctx); err != nil {
		// A failed observation is not proof that source access was removed.
		// Keep the complete prior snapshot and its presentation revision.
		if ic.UserLogin != nil && ic.UserLogin.Bridge != nil {
			ic.UserLogin.Bridge.Log.Warn().Err(err).Msg("Workspace refresh failed; retaining last known snapshot")
		}
	}

	limit := uint32(startupDialogPageLimit)
	ic.mu.RLock()
	workspaceRevision := ic.workspaceRevision
	workspaceChanged := workspaceRevision != ic.workspaceProjectedRevision
	cursor := ic.dialogSyncCursor
	ic.mu.RUnlock()
	seenCursors := make(map[string]struct{})
	remainingRPCs := dialogSyncRPCBudget
	deferredWork := make([]sidecar.DialogRecord, 0)
	pages := 0
	dialogsSeen := 0
	resyncsQueued := 0

	for {
		if remainingRPCs <= 0 {
			ic.UserLogin.Bridge.Log.Debug().Msg("Inline dialog sync RPC budget exhausted; continuing on next pass")
			break
		}
		request := sidecar.DialogsRequest{Limit: &limit, Cursor: cursor, Order: sidecar.DialogsOrderStableChatID}
		var page *sidecar.DialogsPage
		var err error
		if refreshLive && cursor == "" {
			page, err = ic.Sidecar.Dialogs(ctx, request)
		} else {
			page, err = ic.Sidecar.CachedDialogs(ctx, request)
		}
		if err != nil {
			return err
		}
		remainingRPCs--
		pages++
		ic.cacheUsers(page.Users)

		for _, dialog := range page.Dialogs {
			dialogsSeen++
			if err := ctx.Err(); err != nil {
				return err
			}
			changed := ic.rememberDialog(dialog)
			if !ic.shouldProjectDialog(dialog) {
				continue
			}
			if changed || workspaceChanged {
				ic.queueDialogResync(ctx, dialog)
				resyncsQueued++
			}
			if _, err := ic.loadHistoryCheckpoint(ctx, dialog.ChatID); err != nil {
				ic.UserLogin.Bridge.Log.Warn().Err(err).Int64("chat_id", dialog.ChatID).Msg("Failed to load Inline history checkpoint from bridge database")
			}
			if (!isDMDialog(dialog) && ic.needsDialogDetailsSync(dialog.ChatID)) || ic.needsHistoryDelivery(dialog) {
				deferredWork = append(deferredWork, dialog)
			}
		}

		if page.NextCursor == "" {
			ic.mu.Lock()
			ic.workspaceProjectedRevision = workspaceRevision
			ic.mu.Unlock()
			if err := ic.persistDialogSyncCursor(ctx, ""); err != nil {
				return err
			}
			break
		}
		if page.NextCursor == cursor {
			return fmt.Errorf("Inline stable dialog sync cursor did not advance: %q", cursor)
		}
		if _, ok := seenCursors[page.NextCursor]; ok {
			return fmt.Errorf("Inline stable dialog sync cursor repeated: %q", page.NextCursor)
		}
		seenCursors[page.NextCursor] = struct{}{}
		cursor = page.NextCursor
		if err := ic.persistDialogSyncCursor(ctx, cursor); err != nil {
			return err
		}
	}
	err := ic.syncDeferredDialogWork(ctx, deferredWork, remainingRPCs)
	if ic.UserLogin != nil && ic.UserLogin.Bridge != nil {
		ic.UserLogin.Bridge.Log.Debug().
			Bool("live_refresh", refreshLive).
			Int("pages", pages).
			Int("dialogs", dialogsSeen).
			Int("resyncs", resyncsQueued).
			Int("deferred", len(deferredWork)).
			Int("rpc_budget_remaining_before_deferred", remainingRPCs).
			Err(err).
			Msg("Inline dialog sync pass finished")
	}
	return err
}

func (ic *InlineClient) syncDeferredDialogWork(ctx context.Context, dialogs []sidecar.DialogRecord, remainingRPCs int) error {
	for _, dialog := range ic.prioritizedHistoryDialogs(dialogs) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if remainingRPCs <= 0 {
			ic.UserLogin.Bridge.Log.Debug().Msg("Inline dialog sync RPC budget exhausted; continuing on next pass")
			return nil
		}
		if err := ic.syncRecentHistory(ctx, dialog); err != nil {
			if isRateLimitedError(err) {
				return err
			}
			ic.UserLogin.Bridge.Log.Warn().Err(err).Int64("chat_id", dialog.ChatID).Msg("Failed to fetch Inline startup history")
		}
		remainingRPCs--
		if err := sleepWithContext(ctx, startupRPCThrottle); err != nil {
			return err
		}
	}

	for _, dialog := range dialogs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !isDMDialog(dialog) && ic.needsDialogDetailsSync(dialog.ChatID) {
			if remainingRPCs <= 0 {
				ic.UserLogin.Bridge.Log.Debug().Msg("Inline dialog sync RPC budget exhausted; continuing on next pass")
				return nil
			}
			if err := ic.syncDialogDetails(ctx, dialog); err != nil {
				if isRateLimitedError(err) {
					return err
				}
				ic.UserLogin.Bridge.Log.Warn().Err(err).Int64("chat_id", dialog.ChatID).Msg("Failed to fetch Inline chat details")
			} else {
				ic.markDialogDetailsSynced(dialog.ChatID)
			}
			remainingRPCs--
			if err := sleepWithContext(ctx, startupRPCThrottle); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ic *InlineClient) prioritizedHistoryDialogs(dialogs []sidecar.DialogRecord) []sidecar.DialogRecord {
	work := make([]sidecar.DialogRecord, 0, len(dialogs))
	for _, dialog := range dialogs {
		if ic.needsHistoryDelivery(dialog) {
			work = append(work, dialog)
		}
	}
	sort.SliceStable(work, func(i, j int) bool {
		left, right := work[i], work[j]
		leftCheckpointed := ic.historyCheckpoint(left.ChatID) > 0
		rightCheckpointed := ic.historyCheckpoint(right.ChatID) > 0
		if leftCheckpointed != rightCheckpointed {
			return leftCheckpointed
		}
		if isDMDialog(left) != isDMDialog(right) {
			return isDMDialog(left)
		}
		leftLast, rightLast := lastMessageIDValue(left), lastMessageIDValue(right)
		if leftLast != rightLast {
			return leftLast > rightLast
		}
		return left.ChatID < right.ChatID
	})
	return work
}

func (ic *InlineClient) syncDialogDetails(ctx context.Context, dialog sidecar.DialogRecord) error {
	if !ic.shouldProjectDialog(dialog) {
		return nil
	}
	name := dialogName(dialog)
	info, err := ic.chatInfoForChat(ctx, dialog.ChatID, name)
	if err != nil {
		return err
	}
	ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:           bridgev2.RemoteEventChatResync,
			PostHandleFunc: ic.projectionPostHandler,
			PortalKey:      ic.portalKeyForChat(dialog.ChatID),
			Timestamp:      time.Now(),
			CreatePortal:   true,
		},
		GetChatInfoFunc: ic.migrationGatedInfoProvider(info),
	})
	return nil
}

func (ic *InlineClient) syncRecentHistory(ctx context.Context, dialog sidecar.DialogRecord) error {
	if !ic.shouldProjectDialog(dialog) || !ic.needsHistoryDelivery(dialog) {
		return nil
	}
	checkpoint := ic.historyCheckpoint(dialog.ChatID)
	request := historyRequestForDelivery(dialog, checkpoint, startupHistoryLimit)
	page, err := ic.Sidecar.History(ctx, request)
	if err != nil {
		return err
	}
	ic.cacheUsers(page.Users)

	return ic.queueHistoricalPage(ctx, dialog.ChatID, page.Messages, &checkpoint)
}

func historyRequestForDelivery(dialog sidecar.DialogRecord, checkpoint int64, limit uint32) sidecar.HistoryRequest {
	request := sidecar.HistoryRequest{
		ChatID: dialog.ChatID,
		Limit:  &limit,
	}
	// An explicit zero cursor selects the oldest source IDs, rather than a
	// recent tail page that cannot establish coverage from an empty frontier.
	request.AfterMessageID = &checkpoint
	return request
}

func (ic *InlineClient) needsHistoryDelivery(dialog sidecar.DialogRecord) bool {
	if dialog.LastMessageID == nil {
		return false
	}
	return *dialog.LastMessageID > ic.historyCheckpoint(dialog.ChatID)
}

func lastMessageIDValue(dialog sidecar.DialogRecord) int64 {
	if dialog.LastMessageID == nil {
		return 0
	}
	return *dialog.LastMessageID
}

func (ic *InlineClient) historyCheckpoint(chatID int64) int64 {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	if ic.history == nil {
		return 0
	}
	return ic.history[chatID]
}

func (ic *InlineClient) rememberHistoryDelivered(chatID, messageID int64) {
	if messageID <= 0 {
		return
	}
	ic.mu.Lock()
	defer ic.mu.Unlock()
	if ic.history == nil {
		ic.history = make(map[int64]int64)
	}
	if ic.historyLoaded == nil {
		ic.historyLoaded = make(map[int64]struct{})
	}
	ic.historyLoaded[chatID] = struct{}{}
	if messageID > ic.history[chatID] {
		ic.history[chatID] = messageID
	}
}

func (ic *InlineClient) loadHistoryCheckpoint(ctx context.Context, chatID int64) (int64, error) {
	ic.mu.RLock()
	_, loaded := ic.historyLoaded[chatID]
	checkpoint := ic.history[chatID]
	ic.mu.RUnlock()
	if loaded {
		return checkpoint, nil
	}
	portal, err := ic.UserLogin.Bridge.GetPortalByKey(ctx, ic.portalKeyForChat(chatID))
	if err != nil {
		return 0, fmt.Errorf("load Inline portal history checkpoint: %w", err)
	}
	if metadata, ok := portal.Metadata.(*PortalMetadata); ok && metadata != nil {
		checkpoint = metadata.HistoricalCheckpoint
	}
	// A mapped live maximum does not prove coverage of lower historical IDs.
	// Legacy rows restart from zero and deduplicate through persisted mappings.

	ic.mu.Lock()
	if ic.history == nil {
		ic.history = make(map[int64]int64)
	}
	if ic.historyLoaded == nil {
		ic.historyLoaded = make(map[int64]struct{})
	}
	if checkpoint > ic.history[chatID] {
		ic.history[chatID] = checkpoint
	}
	ic.historyLoaded[chatID] = struct{}{}
	checkpoint = ic.history[chatID]
	ic.mu.Unlock()
	return checkpoint, nil
}

func maxInlineMessageIDForChat(messages []*database.Message, chatID int64) int64 {
	var checkpoint int64
	for _, message := range messages {
		if message == nil {
			continue
		}
		parsedChatID, messageID, err := parseMessageID(message.ID)
		if err == nil && parsedChatID == chatID && messageID > checkpoint {
			checkpoint = messageID
		}
	}
	return checkpoint
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}

func isRateLimitedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errInlineRateLimited) {
		return true
	}
	var sidecarErr *sidecar.Error
	if errors.As(err, &sidecarErr) {
		category := strings.ToLower(sidecarErr.Category)
		message := strings.ToLower(sidecarErr.Message)
		if strings.Contains(category, "flood") ||
			strings.Contains(category, "rate") ||
			strings.Contains(message, "420") ||
			strings.Contains(message, "flood") ||
			strings.Contains(message, "rate limit") {
			return true
		}
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "http error: 420") ||
		strings.Contains(message, "http 420") ||
		strings.Contains(message, "returned http 420") ||
		strings.Contains(message, "flood") ||
		strings.Contains(message, "rate limit")
}

func sidecarErrorIsCertain(err error) bool {
	var sidecarErr *sidecar.Error
	if !errors.As(err, &sidecarErr) {
		return false
	}
	switch sidecarErr.Category {
	case "InvalidInput", "ProtocolMismatch", "NotFound", "PermissionDenied", "Unsupported", "Conflict":
		return true
	default:
		return false
	}
}

func rateLimitRetryDelay(err error, fallback time.Duration) time.Duration {
	var sidecarErr *sidecar.Error
	if !errors.As(err, &sidecarErr) || sidecarErr.RetryAfterSeconds == nil || *sidecarErr.RetryAfterSeconds == 0 {
		return fallback
	}
	seconds := *sidecarErr.RetryAfterSeconds
	maxSeconds := uint64(dialogRateLimitMaxDelay / time.Second)
	if seconds > maxSeconds {
		seconds = maxSeconds
	}
	return time.Duration(seconds) * time.Second
}

func (ic *InlineClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if params.ThreadRoot != "" {
		return nil, fmt.Errorf("Inline Matrix thread history is unsupported; use the separate child conversation: %w", bridgev2.ErrUnsupportedMessageType)
	}

	chat, err := chatIDFromPortal(params.Portal)
	if err != nil {
		return nil, err
	}
	if err = ic.readDesireBeforeDelivery(ctx, chat); err != nil {
		return nil, err
	}
	var page *sidecar.HistoryPage
	if bundled, ok := params.BundledData.(historicalPage); ok {
		if bundled.ChatID != chat {
			return nil, errors.New("historical bundle belongs to another chat")
		}
		page = &sidecar.HistoryPage{Messages: bundled.Records}
	} else {
		request, err := historyRequestForBackfill(params)
		if err != nil {
			return nil, err
		}
		page, err = ic.Sidecar.History(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch Inline history: %w", err)
		}
	}
	ic.cacheUsers(page.Users)
	for _, record := range page.Messages {
		if record.ChatID != chat || record.MessageID <= 0 {
			return nil, errors.New("history record belongs to another chat")
		}
	}

	messages, cursor := ic.backfillMessagesFromHistory(ctx, params.Portal, page.Messages)
	hasMore := page.HasMore && cursor != ""
	return &bridgev2.FetchMessagesResponse{
		Messages: messages,
		Cursor:   cursor,
		HasMore:  hasMore,
		Forward:  params.Forward,
		MarkRead: false,
		CompleteCallback: func() {
			_ = ic.persistHistoricalMappings(ctx, params.Portal, page.Messages)
			ic.reconcileProjection(ctx, params.Portal)
		},
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
	var cursor networkid.PaginationCursor
	var cursorChatID, minimumMessageID int64
	for _, record := range records {
		if record.ChatID <= 0 || record.MessageID <= 0 {
			continue
		}
		if minimumMessageID == 0 || record.MessageID < minimumMessageID {
			cursorChatID = record.ChatID
			minimumMessageID = record.MessageID
		}
	}
	if minimumMessageID > 0 {
		cursor = networkid.PaginationCursor(makeMessageID(cursorChatID, minimumMessageID))
	}

	sort.SliceStable(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if left.Timestamp != right.Timestamp {
			return left.Timestamp < right.Timestamp
		}
		return left.MessageID < right.MessageID
	})

	backfillMessages := make([]*bridgev2.BackfillMessage, 0, len(records))
	for _, record := range records {
		if record.ChatID <= 0 || record.MessageID <= 0 {
			ic.logBackfillConversionError(errors.New("invalid Inline message identity"), record)
			continue
		}
		messageID := makeMessageID(record.ChatID, record.MessageID)

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
		converted, err := ic.convertInlineMessage(ctx, portal, intent, record)
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
			StreamOrder:      record.MessageID,
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

		health, err := ic.Sidecar.Health(ctx)
		if err != nil {
			if err := ic.waitBeforeEventReconnect(ctx, err); err != nil {
				return
			}
			continue
		}
		namespace, generation, afterSequence := ic.sidecarEventCursor()
		if generation == "" {
			if err := ic.persistSidecarEventCursor(ctx, health.EventGeneration, afterSequence); err != nil {
				if err := ic.waitBeforeEventReconnect(ctx, err); err != nil {
					return
				}
				continue
			}
			generation = health.EventGeneration
		} else if health.EventGeneration != generation {
			if err := ic.recoverSidecarGenerationChange(ctx, health.EventGeneration); err != nil {
				if err := ic.waitBeforeEventReconnect(ctx, err); err != nil {
					return
				}
				continue
			}
			continue
		}
		stream, err := ic.Sidecar.EventsAfterGeneration(ctx, namespace, generation, afterSequence)
		if err != nil {
			if errors.Is(err, sidecar.ErrEventReplayUnavailable) {
				if recoveryErr := ic.recoverSidecarReplayGap(ctx, err); recoveryErr == nil {
					continue
				} else {
					err = recoveryErr
				}
			}
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
			continue
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
			if err := ic.handleSequencedSidecarEvent(ctx, envelope); err != nil {
				_ = stream.Close(websocket.StatusNormalClosure, "sequence recovery")
				if err := ic.waitBeforeEventReconnect(ctx, err); err != nil {
					return
				}
				break
			}
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
	bridgeError := status.BridgeStateErrorCode("inline-sidecar-events-disconnected")
	message := err.Error()
	if errors.Is(err, sidecar.ErrEventReplayUnavailable) {
		bridgeError = status.BridgeStateErrorCode("inline-sidecar-replay-unavailable")
		message = "Inline sidecar event history is no longer retained; bridge reconciliation is required"
	}
	ic.UserLogin.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateTransientDisconnect,
		Error:      bridgeError,
		Message:    message,
	})
	delay := sidecarEventReconnectLag
	if isRateLimitedError(err) {
		delay = rateLimitRetryDelay(err, dialogRateLimitCooldown)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}

func (ic *InlineClient) recoverSidecarReplayGap(ctx context.Context, eventErr error) error {
	var replayErr *sidecar.EventReplayUnavailableError
	if !errors.As(eventErr, &replayErr) || replayErr.LatestSequence == nil || replayErr.EventGeneration == "" {
		return fmt.Errorf("Inline sidecar replay gap did not include a recovery cursor: %w", eventErr)
	}
	latest := *replayErr.LatestSequence
	ic.UserLogin.Bridge.Log.Warn().
		Uint64("latest_sequence", latest).
		Msg("Reconciling durable Inline state after sidecar replay gap")
	if err := ic.reconcileSidecarState(ctx); err != nil {
		return fmt.Errorf("reconcile Inline state after sidecar replay gap: %w", err)
	}
	if err := ic.persistSidecarEventCursor(ctx, replayErr.EventGeneration, latest); err != nil {
		return err
	}
	if err := ic.Sidecar.AckEventsGeneration(ctx, ic.storeNamespace, replayErr.EventGeneration, latest); err != nil {
		return fmt.Errorf("ack reconciled Inline sidecar cursor: %w", err)
	}
	ic.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
	return nil
}

func (ic *InlineClient) recoverSidecarGenerationChange(ctx context.Context, generation string) error {
	if strings.TrimSpace(generation) == "" {
		return errors.New("Inline sidecar health omitted its event generation")
	}
	ic.UserLogin.Bridge.Log.Warn().Str("event_generation", generation).Msg("Reconciling after Inline sidecar event-log generation changed")
	if err := ic.reconcileSidecarState(ctx); err != nil {
		return fmt.Errorf("reconcile Inline state after event-log generation change: %w", err)
	}
	return ic.persistSidecarEventCursor(ctx, generation, 0)
}

func (ic *InlineClient) reconcileBridgeStateSlice(ctx context.Context, afterChatID int64, rpcBudget int) (bool, error) {
	if rpcBudget <= 0 {
		return false, nil
	}
	limit := uint32(reconciliationPageLimit)
	cursor := ""
	if afterChatID > 0 {
		cursor = strconv.FormatInt(afterChatID, 10)
	}
	page, err := ic.Sidecar.Dialogs(ctx, sidecar.DialogsRequest{
		Limit:  &limit,
		Cursor: cursor,
		Order:  sidecar.DialogsOrderStableChatID,
	})
	if err != nil {
		return false, fmt.Errorf("fetch Inline stable dialog recovery page: %w", err)
	}
	ic.cacheUsers(page.Users)
	if err := ic.reconcileAccountDeletes(ctx); err != nil {
		return false, err
	}

	// The first reconciliation page is a live dialogs RPC. Later pages, account
	// state, chat state and cached history are served from the sidecar store.
	remainingRPCs := rpcBudget - 1
	for _, dialog := range page.Dialogs {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		ic.rememberDialog(dialog)
		if !ic.shouldProjectDialog(dialog) {
			if err := ic.persistBridgeRecoveryCursor(ctx, dialog.ChatID); err != nil {
				return false, err
			}
			continue
		}
		if _, err := ic.loadHistoryCheckpoint(ctx, dialog.ChatID); err != nil {
			return false, fmt.Errorf("load Inline history checkpoint for chat %d: %w", dialog.ChatID, err)
		}
		for ic.needsHistoryDelivery(dialog) {
			if remainingRPCs <= 0 {
				return false, nil
			}
			before := ic.historyCheckpoint(dialog.ChatID)
			if err := ic.syncRecentHistory(ctx, dialog); err != nil {
				return false, fmt.Errorf("recover Inline history for chat %d: %w", dialog.ChatID, err)
			}
			remainingRPCs--
			if ic.historyCheckpoint(dialog.ChatID) <= before {
				return false, fmt.Errorf("Inline history recovery for chat %d did not advance", dialog.ChatID)
			}
			if err := sleepWithContext(ctx, startupRPCThrottle); err != nil {
				return false, err
			}
		}
		if !isDMDialog(dialog) {
			if remainingRPCs <= 0 {
				return false, nil
			}
			remainingRPCs--
		}
		if err := ic.reconcileDialogState(ctx, dialog); err != nil {
			return false, err
		}
		if !isDMDialog(dialog) {
			if err := sleepWithContext(ctx, startupRPCThrottle); err != nil {
				return false, err
			}
		}
		if err := ic.persistBridgeRecoveryCursor(ctx, dialog.ChatID); err != nil {
			return false, err
		}
	}
	return page.NextCursor == "", nil
}

func (ic *InlineClient) reconcileAccountDeletes(ctx context.Context) error {
	accountState, err := ic.Sidecar.AccountState(ctx)
	if err != nil {
		return fmt.Errorf("fetch Inline account reconciliation state: %w", err)
	}
	ic.cacheWorkspaceSnapshot(accountState)
	for _, chatID := range accountState.DeletedChatIDs {
		if err := eventHandlingResultError("reconciled chat delete", ic.queueChatDelete(chatID)); err != nil {
			return err
		}
	}
	return nil
}

func (ic *InlineClient) reconcileSidecarState(ctx context.Context) error {
	dialogs, err := ic.fetchAllDialogsForReconciliation(ctx, 0)
	if err != nil {
		return err
	}
	if err := ic.reconcileAccountDeletes(ctx); err != nil {
		return err
	}

	for _, dialog := range dialogs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ic.reconcileDialogState(ctx, dialog); err != nil {
			return err
		}
		if !isDMDialog(dialog) {
			if err := sleepWithContext(ctx, startupRPCThrottle); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ic *InlineClient) reconcileDialogState(ctx context.Context, dialog sidecar.DialogRecord) error {
	ic.rememberDialog(dialog)
	if !ic.shouldProjectDialog(dialog) {
		return nil
	}
	info, err := ic.chatInfoForChat(ctx, dialog.ChatID, dialogName(dialog))
	if err != nil {
		return fmt.Errorf("fetch Inline chat %d reconciliation info: %w", dialog.ChatID, err)
	}
	result := ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:           bridgev2.RemoteEventChatResync,
			PostHandleFunc: ic.projectionPostHandler,
			PortalKey:      ic.portalKeyForChat(dialog.ChatID),
			Timestamp:      time.Now(),
			CreatePortal:   true,
		},
		GetChatInfoFunc: ic.migrationGatedInfoProvider(info),
	})
	if err := eventHandlingResultError("reconciled chat resync", result); err != nil {
		return err
	}

	state, err := ic.Sidecar.ChatState(ctx, sidecar.ChatStateRequest{ChatID: dialog.ChatID})
	if err != nil {
		return fmt.Errorf("fetch Inline chat %d durable state: %w", dialog.ChatID, err)
	}
	if state.Deleted {
		return eventHandlingResultError("reconciled chat delete", ic.queueChatDelete(dialog.ChatID))
	}
	for _, messageID := range state.DeletedMessageIDs {
		if err := eventHandlingResultError(
			"reconciled message delete",
			ic.queueInlineMessageDelete(dialog.ChatID, messageID),
		); err != nil {
			return err
		}
	}
	if err := ic.reconcileCachedHistory(ctx, dialog.ChatID); err != nil {
		return err
	}
	if err := ic.reconcileReactions(state); err != nil {
		return err
	}
	return ic.queueReadState(state.ReadState)
}

func (ic *InlineClient) fetchAllDialogsForReconciliation(ctx context.Context, afterChatID int64) ([]sidecar.DialogRecord, error) {
	limit := uint32(reconciliationPageLimit)
	cursor := ""
	if afterChatID > 0 {
		cursor = strconv.FormatInt(afterChatID, 10)
	}
	seen := make(map[string]struct{})
	dialogs := make([]sidecar.DialogRecord, 0)
	for pageIndex := 0; pageIndex < reconciliationMaxPages; pageIndex++ {
		request := sidecar.DialogsRequest{Limit: &limit, Cursor: cursor, Order: sidecar.DialogsOrderStableChatID}
		var page *sidecar.DialogsPage
		var err error
		if pageIndex == 0 {
			page, err = ic.Sidecar.Dialogs(ctx, request)
		} else {
			page, err = ic.Sidecar.CachedDialogs(ctx, request)
		}
		if err != nil {
			return nil, fmt.Errorf("fetch Inline dialogs for reconciliation: %w", err)
		}
		ic.cacheUsers(page.Users)
		dialogs = append(dialogs, page.Dialogs...)
		if page.NextCursor == "" {
			return dialogs, nil
		}
		if _, duplicate := seen[page.NextCursor]; duplicate {
			return nil, fmt.Errorf("Inline dialog reconciliation cursor repeated: %q", page.NextCursor)
		}
		seen[page.NextCursor] = struct{}{}
		cursor = page.NextCursor
	}
	return nil, fmt.Errorf("Inline dialog reconciliation exceeded %d pages", reconciliationMaxPages)
}

func (ic *InlineClient) reconcileCachedHistory(ctx context.Context, chatID int64) error {
	limit := uint32(reconciliationPageLimit)
	after := int64(0)
	for pageIndex := 0; pageIndex < reconciliationMaxPages; pageIndex++ {
		page, err := ic.Sidecar.CachedHistory(ctx, sidecar.HistoryRequest{
			ChatID:         chatID,
			Limit:          &limit,
			AfterMessageID: &after,
		})
		if err != nil {
			return fmt.Errorf("fetch cached Inline history for chat %d: %w", chatID, err)
		}
		ic.cacheUsers(page.Users)
		if len(page.Messages) == 0 {
			if page.HasMore {
				return fmt.Errorf("cached Inline history for chat %d did not advance", chatID)
			}
			return nil
		}
		pageMax, err := cachedHistoryPageMax(page.Messages, after)
		if err != nil {
			return fmt.Errorf("validate cached Inline history for chat %d: %w", chatID, err)
		}
		if err := ic.queueHistoricalPage(ctx, chatID, page.Messages, nil); err != nil {
			return err
		}
		after = pageMax
		if !page.HasMore {
			return nil
		}
	}
	return fmt.Errorf("cached Inline history reconciliation for chat %d exceeded %d pages", chatID, reconciliationMaxPages)
}

func cachedHistoryPageMax(messages []sidecar.MessageRecord, previousAfter int64) (int64, error) {
	pageMax := previousAfter
	seen := make(map[int64]struct{}, len(messages))
	for _, message := range messages {
		if message.MessageID <= previousAfter {
			return previousAfter, fmt.Errorf(
				"message ID %d did not advance beyond page boundary %d",
				message.MessageID,
				previousAfter,
			)
		}
		if _, duplicate := seen[message.MessageID]; duplicate {
			return previousAfter, fmt.Errorf("duplicate message ID %d in cached history page", message.MessageID)
		}
		seen[message.MessageID] = struct{}{}
		if message.MessageID > pageMax {
			pageMax = message.MessageID
		}
	}
	if len(messages) > 0 && pageMax <= previousAfter {
		return previousAfter, errors.New("cached history page did not make progress")
	}
	return pageMax, nil
}

func (ic *InlineClient) reconcileReactions(state *sidecar.ChatStateSnapshot) error {
	if state == nil {
		return nil
	}
	byMessage := make(map[int64]map[networkid.UserID]*bridgev2.ReactionSyncUser)
	for _, reaction := range state.Reactions {
		users := byMessage[reaction.MessageID]
		if users == nil {
			users = make(map[networkid.UserID]*bridgev2.ReactionSyncUser)
			byMessage[reaction.MessageID] = users
		}
		userID := makeUserID(strconv.FormatInt(reaction.UserID, 10))
		user := users[userID]
		if user == nil {
			user = &bridgev2.ReactionSyncUser{HasAllReactions: true}
			users[userID] = user
		}
		user.Reactions = append(user.Reactions, &bridgev2.BackfillReaction{
			Sender: bridgev2.EventSender{
				Sender:   userID,
				IsFromMe: strconv.FormatInt(reaction.UserID, 10) == ic.AccountID,
			},
			EmojiID: networkid.EmojiID(reaction.Reaction),
			Emoji:   reaction.Reaction,
		})
	}
	for _, messageID := range state.ReactionSnapshotMessageIDs {
		users := byMessage[messageID]
		if users == nil {
			users = make(map[networkid.UserID]*bridgev2.ReactionSyncUser)
		}
		result := ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ReactionSync{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventReactionSync,
				PortalKey: ic.portalKeyForChat(state.ChatID),
				Timestamp: time.Now(),
			},
			TargetMessage: makeMessageID(state.ChatID, messageID),
			Reactions: &bridgev2.ReactionSyncData{
				Users:       users,
				HasAllUsers: true,
			},
		})
		if err := eventHandlingResultError("reconciled reaction snapshot", result); err != nil {
			return err
		}
	}
	return nil
}

func (ic *InlineClient) syncReadState(ctx context.Context, chatID int64) error {
	state, err := ic.Sidecar.ChatState(ctx, sidecar.ChatStateRequest{ChatID: chatID})
	if err != nil {
		return fmt.Errorf("fetch Inline read state for chat %d: %w", chatID, err)
	}
	return ic.queueReadState(state.ReadState)
}

func (ic *InlineClient) queueReadState(state *sidecar.ReadStateRecord) error {
	if state == nil {
		return nil
	}
	ctx := context.Background()
	if err := ic.storeReadDesire(ctx, state); err != nil {
		return err
	}
	// A ChatResync is a serialized wakeup, never an ordinary framework receipt:
	// MarkRead/MarkUnread can clear manual unread with unmarked account data.
	result := ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatResync{
		EventMeta:       simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: ic.portalKeyForChat(state.ChatID), PostHandleFunc: ic.projectionPostHandler},
		GetChatInfoFunc: ic.migrationGatedInfoProvider(&bridgev2.ChatInfo{}),
	})
	return eventHandlingResultError("pending read projection", result)
}

func (ic *InlineClient) handleSequencedSidecarEvent(ctx context.Context, envelope *sidecar.EventEnvelope) error {
	if envelope == nil {
		return nil
	}
	if envelope.Reliability == sidecar.EventBestEffort {
		if err := ic.handleSidecarEvent(ctx, envelope); err != nil {
			ic.UserLogin.Bridge.Log.Debug().Err(err).Msg("Dropping failed best-effort Inline sidecar event")
		}
		return nil
	}
	if envelope.Sequence == nil {
		return errors.New("lossless Inline sidecar event is missing a sequence")
	}
	namespace, generation, current := ic.sidecarEventCursor()
	if envelope.SessionNamespace != namespace {
		return fmt.Errorf("Inline sidecar event namespace %q does not match login namespace %q", envelope.SessionNamespace, namespace)
	}
	if envelope.Generation != generation {
		return fmt.Errorf("Inline sidecar event generation %q does not match login generation %q", envelope.Generation, generation)
	}
	sequence := *envelope.Sequence
	if sequence <= current {
		return ic.Sidecar.AckEventsGeneration(ctx, namespace, generation, current)
	}
	if sequence != current+1 {
		return fmt.Errorf("Inline sidecar event gap: got %d after %d", sequence, current)
	}

	if err := ic.handleSidecarEvent(ctx, envelope); err != nil {
		return err
	}
	if err := ic.persistSidecarSequence(ctx, sequence); err != nil {
		return err
	}
	return ic.Sidecar.AckEventsGeneration(ctx, namespace, generation, sequence)
}

func (ic *InlineClient) sidecarEventCursor() (string, string, uint64) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	return ic.storeNamespace, ic.sidecarEventGeneration, ic.lastSidecarSequence
}

func (ic *InlineClient) persistSidecarSequence(ctx context.Context, sequence uint64) error {
	ic.mu.RLock()
	generation := ic.sidecarEventGeneration
	ic.mu.RUnlock()
	return ic.persistSidecarEventCursor(ctx, generation, sequence)
}

func (ic *InlineClient) persistSidecarEventCursor(ctx context.Context, generation string, sequence uint64) error {
	ic.metadataSaveMu.Lock()
	defer ic.metadataSaveMu.Unlock()
	ic.mu.Lock()
	previousSequence := ic.lastSidecarSequence
	previousGeneration := ic.sidecarEventGeneration
	if generation == previousGeneration && sequence <= previousSequence {
		ic.mu.Unlock()
		return nil
	}
	ic.sidecarEventGeneration = generation
	ic.lastSidecarSequence = sequence
	meta, ok := ic.UserLogin.Metadata.(*UserLoginMetadata)
	if ok {
		meta.SidecarEventGeneration = generation
		meta.LastSidecarSequence = sequence
	}
	ic.mu.Unlock()

	if !ok {
		return errors.New("Inline user login metadata is unavailable")
	}
	if err := ic.UserLogin.Save(ctx); err != nil {
		ic.mu.Lock()
		ic.sidecarEventGeneration = previousGeneration
		ic.lastSidecarSequence = previousSequence
		meta.SidecarEventGeneration = previousGeneration
		meta.LastSidecarSequence = previousSequence
		ic.mu.Unlock()
		return fmt.Errorf("save Inline sidecar event sequence: %w", err)
	}
	ic.UserLogin.Bridge.Log.Debug().Uint64("sequence", sequence).Msg("Persisted Inline sidecar event cursor")
	return nil
}

func (ic *InlineClient) handleSidecarEvent(ctx context.Context, envelope *sidecar.EventEnvelope) error {
	if envelope == nil {
		return nil
	}
	switch envelope.Event.Type {
	case "StatusChanged":
		if envelope.Event.StatusChanged == nil {
			return errors.New("malformed StatusChanged sidecar event")
		}
		ic.handleStatusChanged(envelope.Event.StatusChanged)
		return nil
	case "TransactionChanged":
		if envelope.Event.TransactionChanged == nil {
			return errors.New("malformed TransactionChanged sidecar event")
		}
		switch sidecar.TransactionState(envelope.Event.TransactionChanged.State) {
		case sidecar.TransactionQueued, sidecar.TransactionSent, sidecar.TransactionAcked,
			sidecar.TransactionCompleted, sidecar.TransactionFailed, sidecar.TransactionCancelled:
			ic.UserLogin.Bridge.Log.Debug().
				Str("transaction_state", envelope.Event.TransactionChanged.State).
				Msg("Handled Inline transaction state event")
			return nil
		default:
			return fmt.Errorf("unsupported Inline transaction state %q", envelope.Event.TransactionChanged.State)
		}
	case "ChatUpserted":
		if envelope.Event.ChatUpserted == nil {
			return errors.New("malformed ChatUpserted sidecar event")
		}
		if skip, err := ic.skipHiddenChatEvent(ctx, envelope.Event.ChatUpserted.ChatID, true, envelope.Event.Type); err != nil {
			return err
		} else if skip {
			return nil
		}
		ic.markDialogDetailsStale(envelope.Event.ChatUpserted.ChatID)
		return eventHandlingResultError("chat resync", ic.queueChatResyncByID(ctx, envelope.Event.ChatUpserted.ChatID))
	case "ChatDeleted":
		if envelope.Event.ChatDeleted == nil {
			return errors.New("malformed ChatDeleted sidecar event")
		}
		return eventHandlingResultError("chat delete", ic.queueChatDelete(envelope.Event.ChatDeleted.ChatID))
	case "ChatParticipantsChanged":
		if envelope.Event.ChatParticipantsChanged == nil {
			return errors.New("malformed ChatParticipantsChanged sidecar event")
		}
		if skip, err := ic.skipHiddenChatEvent(ctx, envelope.Event.ChatParticipantsChanged.ChatID, false, envelope.Event.Type); err != nil {
			return err
		} else if skip {
			return nil
		}
		ic.markDialogDetailsStale(envelope.Event.ChatParticipantsChanged.ChatID)
		return eventHandlingResultError("participant resync", ic.queueChatResyncByID(ctx, envelope.Event.ChatParticipantsChanged.ChatID))
	case "UserUpserted":
		if envelope.Event.UserUpserted == nil {
			return errors.New("malformed UserUpserted sidecar event")
		}
		return ic.refreshInlineUserProfile(ctx, envelope.Event.UserUpserted.UserID)
	case "SpaceUpserted":
		if envelope.Event.SpaceUpserted == nil {
			return errors.New("malformed SpaceUpserted sidecar event")
		}
		return ic.queueSpaceDialogResync(ctx, envelope.Event.SpaceUpserted.SpaceID)
	case "SpaceMemberChanged":
		if envelope.Event.SpaceMemberChanged == nil {
			return errors.New("malformed SpaceMemberChanged sidecar event")
		}
		return ic.queueSpaceDialogResync(ctx, envelope.Event.SpaceMemberChanged.SpaceID)
	case "UserSettingsChanged":
		if envelope.Event.UserSettingsChanged == nil {
			return errors.New("malformed UserSettingsChanged sidecar event")
		}
		ic.UserLogin.Bridge.Log.Debug().Msg("Handled Inline user settings event with no Matrix projection")
		return nil
	case "MessageActionInvoked":
		if envelope.Event.MessageActionInvoked == nil {
			return errors.New("malformed MessageActionInvoked sidecar event")
		}
		ic.UserLogin.Bridge.Log.Debug().
			Int64("chat_id", envelope.Event.MessageActionInvoked.ChatID).
			Int64("message_id", envelope.Event.MessageActionInvoked.MessageID).
			Msg("Handled Inline message action event with no Matrix projection")
		return nil
	case "MessageActionAnswered":
		if envelope.Event.MessageActionAnswered == nil {
			return errors.New("malformed MessageActionAnswered sidecar event")
		}
		ic.UserLogin.Bridge.Log.Debug().Msg("Handled Inline message action response with no Matrix projection")
		return nil
	case "MessageUpserted":
		if envelope.Event.MessageUpserted == nil {
			return errors.New("malformed MessageUpserted sidecar event")
		}
		if skip, err := ic.skipHiddenChatEvent(ctx, envelope.Event.MessageUpserted.ChatID, false, envelope.Event.Type); err != nil {
			return err
		} else if skip {
			return nil
		}
		return ic.syncMessageUpsert(ctx, envelope.Event.MessageUpserted.ChatID, envelope.Event.MessageUpserted.MessageID)
	case "MessageStored":
		if envelope.Event.MessageStored == nil {
			return errors.New("malformed MessageStored sidecar event")
		}
		if skip, err := ic.skipHiddenChatEvent(ctx, envelope.Event.MessageStored.Message.ChatID, false, envelope.Event.Type); err != nil {
			return err
		} else if skip {
			return nil
		}
		return eventHandlingResultError("message upsert", ic.queueInlineMessage(ctx, envelope.Event.MessageStored.Message))
	case "MessageDeleted":
		if envelope.Event.MessageDeleted == nil {
			return errors.New("malformed MessageDeleted sidecar event")
		}
		if skip, err := ic.skipHiddenChatEvent(ctx, envelope.Event.MessageDeleted.ChatID, false, envelope.Event.Type); err != nil {
			return err
		} else if skip {
			return nil
		}
		return eventHandlingResultError("message delete", ic.queueInlineMessageDelete(envelope.Event.MessageDeleted.ChatID, envelope.Event.MessageDeleted.MessageID))
	case "ChatHistoryCleared":
		if envelope.Event.ChatHistoryCleared == nil {
			return errors.New("malformed ChatHistoryCleared sidecar event")
		}
		if skip, err := ic.skipHiddenChatEvent(ctx, envelope.Event.ChatHistoryCleared.ChatID, false, envelope.Event.Type); err != nil {
			return err
		} else if skip {
			return nil
		}
		return ic.queueChatHistoryClear(ctx, envelope.Event.ChatHistoryCleared)
	case "ReactionChanged":
		if envelope.Event.ReactionChanged == nil {
			return errors.New("malformed ReactionChanged sidecar event")
		}
		if skip, err := ic.skipHiddenChatEvent(ctx, envelope.Event.ReactionChanged.ChatID, false, envelope.Event.Type); err != nil {
			return err
		} else if skip {
			return nil
		}
		return eventHandlingResultError("reaction", ic.queueInlineReaction(envelope.Event.ReactionChanged))
	case "ReadStateChanged":
		if envelope.Event.ReadStateChanged == nil {
			return errors.New("malformed ReadStateChanged sidecar event")
		}
		if skip, err := ic.skipHiddenChatEvent(ctx, envelope.Event.ReadStateChanged.ChatID, false, envelope.Event.Type); err != nil {
			return err
		} else if skip {
			return nil
		}
		return ic.syncReadState(ctx, envelope.Event.ReadStateChanged.ChatID)
	case "Typing":
		if envelope.Event.Typing == nil {
			return errors.New("malformed Typing sidecar event")
		}
		if skip, err := ic.skipHiddenChatEvent(ctx, envelope.Event.Typing.ChatID, false, envelope.Event.Type); err != nil {
			return err
		} else if skip {
			return nil
		}
		return eventHandlingResultError("typing", ic.queueInlineTyping(envelope.Event.Typing))
	case "UserStatusChanged":
		if envelope.Event.UserStatusChanged == nil {
			return errors.New("malformed UserStatusChanged sidecar event")
		}
		return nil
	case "BotPresenceChanged":
		if envelope.Event.BotPresenceChanged == nil {
			return errors.New("malformed BotPresenceChanged sidecar event")
		}
		return nil
	case "NewMessageNotification":
		if envelope.Event.NewMessageNotification == nil {
			return errors.New("malformed NewMessageNotification sidecar event")
		}
		if skip, err := ic.skipHiddenChatEvent(ctx, envelope.Event.NewMessageNotification.Message.ChatID, false, envelope.Event.Type); err != nil {
			return err
		} else if skip {
			return nil
		}
		return eventHandlingResultError("message notification", ic.queueInlineMessage(ctx, envelope.Event.NewMessageNotification.Message))
	default:
		return fmt.Errorf("unsupported Inline sidecar event variant %q", envelope.Event.Type)
	}
}

func (ic *InlineClient) refreshInlineUserProfile(ctx context.Context, userID int64) error {
	limit := uint32(1)
	page, err := ic.Sidecar.CachedDialogs(ctx, sidecar.DialogsRequest{Limit: &limit})
	if err != nil {
		return fmt.Errorf("fetch cached Inline users: %w", err)
	}
	ic.cacheUsers(page.Users)
	user, ok := ic.cachedUser(userID)
	if !ok {
		return fmt.Errorf("updated Inline user %d was not found in durable state", userID)
	}
	ghost, err := ic.UserLogin.Bridge.GetGhostByID(ctx, makeUserID(strconv.FormatInt(userID, 10)))
	if err != nil {
		return fmt.Errorf("load Matrix ghost for Inline user %d: %w", userID, err)
	}
	ghost.UpdateInfo(ctx, userInfoFromRecord(user))
	return nil
}

func (ic *InlineClient) syncMessageUpsert(ctx context.Context, chatID, messageID int64) error {
	limit := uint32(1)
	after := messageID - 1
	page, err := ic.Sidecar.History(ctx, sidecar.HistoryRequest{
		ChatID:         chatID,
		Limit:          &limit,
		AfterMessageID: &after,
	})
	if err != nil {
		return fmt.Errorf("fetch Inline message upsert: %w", err)
	}
	ic.cacheUsers(page.Users)
	for _, msg := range page.Messages {
		if msg.MessageID == messageID {
			return eventHandlingResultError("message upsert", ic.queueInlineMessage(ctx, msg))
		}
	}
	return fmt.Errorf("Inline message upsert %d/%d was not found in client history", chatID, messageID)
}

func (ic *InlineClient) handleStatusChanged(evt *sidecar.StatusChangedEvent) {
	switch evt.Status {
	case sidecar.StatusConnected:
		ic.setLoggedIn(true)
		ic.UserLogin.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
	case sidecar.StatusReconnecting:
		ic.setLoggedIn(true)
		bridgeState := status.BridgeState{
			StateEvent: status.StateTransientDisconnect,
			Error:      "inline-sidecar-reconnecting",
			Message:    "Inline sidecar is reconnecting",
		}
		if evt.Failure != nil {
			bridgeState.Message = evt.Failure.Message
		}
		ic.UserLogin.BridgeState.Send(bridgeState)
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

func (ic *InlineClient) queueDialogResync(ctx context.Context, dialog sidecar.DialogRecord) bridgev2.EventHandlingResult {
	if !ic.shouldProjectDialog(dialog) {
		return bridgev2.EventHandlingResultIgnored
	}
	if err := ic.ensureWorkspace(ctx, dialog); err != nil {
		return bridgev2.EventHandlingResult{Error: err}
	}
	chatInfo := ic.chatInfoForDialog(ctx, dialog, dialogName(dialog))
	return ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:           bridgev2.RemoteEventChatResync,
			PostHandleFunc: ic.projectionPostHandler,
			PortalKey:      ic.portalKeyForChat(dialog.ChatID),
			Timestamp:      time.Now(),
			CreatePortal:   true,
		},
		GetChatInfoFunc: ic.migrationGatedInfoProvider(chatInfo),
	})
}

func dialogName(dialog sidecar.DialogRecord) *string {
	if displayName := dialogDisplayName(dialog); displayName != "" {
		return optionalString(displayName)
	}
	return optionalString(strconv.FormatInt(dialog.ChatID, 10))
}

func dialogDisplayName(dialog sidecar.DialogRecord) string {
	if dialog.Title == nil {
		return ""
	}
	title := strings.TrimSpace(*dialog.Title)
	if title == "" {
		return ""
	}
	if dialog.Emoji != nil {
		if emoji := strings.TrimSpace(*dialog.Emoji); emoji != "" {
			return emoji + " " + title
		}
	}
	return title
}

func (ic *InlineClient) queueChatResyncByID(ctx context.Context, chatID int64) bridgev2.EventHandlingResult {
	if dialog, ok := ic.cachedDialog(chatID); ok {
		return ic.queueDialogResync(ctx, dialog)
	}
	return ic.queueDialogResync(ctx, sidecar.DialogRecord{ChatID: chatID})
}

func (ic *InlineClient) queueSpaceDialogResync(ctx context.Context, spaceID int64) error {
	if err := ic.refreshWorkspaceSnapshot(ctx); err != nil {
		return err
	}
	ic.mu.RLock()
	dialogs := make([]sidecar.DialogRecord, 0)
	for _, dialog := range ic.dialogs {
		if dialog.SpaceID != nil && *dialog.SpaceID == spaceID {
			dialogs = append(dialogs, dialog)
		}
	}
	ic.mu.RUnlock()
	for _, dialog := range dialogs {
		if !ic.shouldProjectDialog(dialog) {
			continue
		}
		ic.markDialogDetailsStale(dialog.ChatID)
		if err := eventHandlingResultError("space dialog resync", ic.queueDialogResync(ctx, dialog)); err != nil {
			return err
		}
	}
	return nil
}

func (ic *InlineClient) queueChatDelete(chatID int64) bridgev2.EventHandlingResult {
	return ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatDelete{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatDelete,
			PortalKey: ic.portalKeyForChat(chatID),
			Timestamp: time.Now(),
		},
		OnlyForMe: true,
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

func (ic *InlineClient) rememberDialog(dialog sidecar.DialogRecord) bool {
	if dialog.ChatID <= 0 {
		return false
	}
	ic.mu.Lock()
	defer ic.mu.Unlock()
	if ic.dialogs == nil {
		ic.dialogs = make(map[int64]sidecar.DialogRecord)
	}
	previous, ok := ic.dialogs[dialog.ChatID]
	// A partial observation cannot erase a previously complete structure. A
	// conflicting complete observation remains unknown and must be enriched.
	if dialog.Structure == nil && ok && previous.Structure != nil {
		dialog.Structure = previous.Structure
		dialog.PeerUserID = previous.PeerUserID
		dialog.SpaceID = previous.SpaceID
		dialog.ParentChatID = previous.ParentChatID
		dialog.ParentMessageID = previous.ParentMessageID
	}
	ic.dialogs[dialog.ChatID] = dialog
	changed := !ok || !sameDialogRecord(previous, dialog)
	if !ok || !int64PtrEqual(previous.PeerUserID, dialog.PeerUserID) {
		delete(ic.details, dialog.ChatID)
	}
	return changed
}

func (ic *InlineClient) cachedDialog(chatID int64) (sidecar.DialogRecord, bool) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	dialog, ok := ic.dialogs[chatID]
	return dialog, ok
}

func boolPtrEqual(left, right *bool) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func (ic *InlineClient) needsDialogDetailsSync(chatID int64) bool {
	if chatID <= 0 {
		return false
	}
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	_, ok := ic.details[chatID]
	return !ok
}

func (ic *InlineClient) markDialogDetailsSynced(chatID int64) {
	if chatID <= 0 {
		return
	}
	ic.mu.Lock()
	defer ic.mu.Unlock()
	if ic.details == nil {
		ic.details = make(map[int64]struct{})
	}
	ic.details[chatID] = struct{}{}
}

func (ic *InlineClient) markDialogDetailsStale(chatID int64) {
	if chatID <= 0 {
		return
	}
	ic.mu.Lock()
	defer ic.mu.Unlock()
	delete(ic.details, chatID)
}

func (ic *InlineClient) cachedUser(userID int64) (sidecar.UserRecord, bool) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	user, ok := ic.users[userID]
	return user, ok
}

func (ic *InlineClient) chatInfoForChat(ctx context.Context, chatID int64, name *string) (*bridgev2.ChatInfo, error) {
	if d, ok := ic.cachedDialog(chatID); !ok || classifyDialog(d) == roomUnknown {
		bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
		state, err := ic.Sidecar.ChatState(bounded, sidecar.ChatStateRequest{ChatID: chatID})
		cancel()
		if err == nil && state.Dialog != nil {
			ic.rememberDialog(*state.Dialog)
		}
	}
	if dialog, ok := ic.cachedDialog(chatID); ok && isDMDialog(dialog) {
		return ic.chatInfoForDialog(ctx, dialog, name), nil
	}
	page, err := ic.Sidecar.ChatParticipants(ctx, sidecar.ChatParticipantsRequest{ChatID: chatID})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch Inline chat participants: %w", err)
	}
	ic.cacheUsers(page.Users)
	dialog, _ := ic.cachedDialog(chatID)
	info := ic.chatInfoForDialog(ctx, dialog, name)
	info.Members = ic.chatMemberListFromParticipants(ctx, page)
	if info.Members != nil {
		info.Members.OtherUserID = ""
	}
	return info, nil
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
	if len(members) == 0 {
		return nil
	}
	if selfID := ic.GetUserID(); selfID != "" {
		if _, ok := members[selfID]; !ok {
			members[selfID] = ic.chatMemberForUserID(ctx, selfID)
		}
	}

	memberList := &bridgev2.ChatMemberList{
		IsFull:                     true,
		CheckAllLogins:             true,
		ExcludeChangesFromTimeline: true,
		TotalMemberCount:           len(members),
		MemberMap:                  members,
	}
	return memberList
}

func (ic *InlineClient) chatMemberListForDM(ctx context.Context, peerUserID int64) *bridgev2.ChatMemberList {
	members := make(bridgev2.ChatMemberMap, 2)
	if selfID := ic.GetUserID(); selfID != "" {
		members[selfID] = ic.chatMemberForUserID(ctx, selfID)
	}
	otherUserID := makeUserID(strconv.FormatInt(peerUserID, 10))
	members[otherUserID] = ic.chatMemberForUserID(ctx, otherUserID)
	return &bridgev2.ChatMemberList{
		IsFull:                     true,
		CheckAllLogins:             true,
		ExcludeChangesFromTimeline: true,
		TotalMemberCount:           len(members),
		OtherUserID:                otherUserID,
		MemberMap:                  members,
	}
}

func (ic *InlineClient) chatMemberListForKnownSelf(ctx context.Context) *bridgev2.ChatMemberList {
	selfID := ic.GetUserID()
	if selfID == "" {
		return nil
	}
	return &bridgev2.ChatMemberList{
		IsFull:                     false,
		CheckAllLogins:             true,
		ExcludeChangesFromTimeline: true,
		MemberMap: bridgev2.ChatMemberMap{
			selfID: ic.chatMemberForUserID(ctx, selfID),
		},
	}
}

func (ic *InlineClient) createDMWithUserID(ctx context.Context, userID int64) (*bridgev2.CreateChatResponse, error) {
	chat, err := ic.Sidecar.CreateDM(ctx, sidecar.CreateDMRequest{UserID: userID})
	if err != nil {
		return nil, fmt.Errorf("Inline sidecar create DM failed: %w", err)
	}
	if chat != nil {
		ic.rememberDialog(sidecar.DialogRecord{
			ChatID:     chat.ChatID,
			PeerUserID: &userID,
			Title:      chat.Title,
		})
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

func eventHandlingResultError(kind string, result bridgev2.EventHandlingResult) error {
	if result.Success {
		return nil
	}
	if result.Error != nil {
		return fmt.Errorf("mautrix rejected Inline %s event: %w", kind, result.Error)
	}
	return fmt.Errorf("mautrix rejected Inline %s event", kind)
}

func (ic *InlineClient) queueInlineMessage(ctx context.Context, msg sidecar.MessageRecord) bridgev2.EventHandlingResult {
	result := ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, ic.remoteMessageEvent(msg))
	if historyDeliveryAccepted(result) {
		row, err := ic.UserLogin.Bridge.DB.Message.GetFirstPartByID(ctx, ic.portalKeyForChat(msg.ChatID).Receiver, makeMessageID(msg.ChatID, msg.MessageID))
		if err != nil || row == nil || row.MXID == "" || row.HasFakeMXID() || row.Room != ic.portalKeyForChat(msg.ChatID) {
			return bridgev2.EventHandlingResultFailed.WithError(errors.New("live delivery awaits persisted Matrix mapping"))
		}
		if err := ic.persistDeliveryCheckpoint(ctx, msg.ChatID, msg.MessageID, false); err != nil {
			result.Success = false
			result.Error = err
			return result
		}
	}
	return result
}

func (ic *InlineClient) persistHistoryCheckpoint(ctx context.Context, chatID, messageID int64) error {
	return ic.persistDeliveryCheckpoint(ctx, chatID, messageID, true)
}
func (ic *InlineClient) persistDeliveryCheckpoint(ctx context.Context, chatID, messageID int64, historical bool) error {
	if chatID <= 0 || messageID <= 0 {
		return errors.New("invalid Inline history checkpoint")
	}
	portal, err := ic.UserLogin.Bridge.GetPortalByKey(ctx, ic.portalKeyForChat(chatID))
	if err != nil {
		return fmt.Errorf("load portal for Inline history checkpoint: %w", err)
	}
	metadata, ok := portal.Metadata.(*PortalMetadata)
	if !ok || metadata == nil {
		return errors.New("Inline portal metadata is unavailable")
	}
	previous := *metadata
	if messageID > metadata.HistoryCheckpoint {
		metadata.HistoryCheckpoint = messageID
	}
	if historical && messageID > metadata.HistoricalCheckpoint {
		metadata.HistoricalCheckpoint = messageID
	}
	if *metadata == previous {
		return nil
	}
	if err := portal.Save(ctx); err != nil {
		*metadata = previous
		return fmt.Errorf("save Inline history checkpoint: %w", err)
	}
	return nil
}

func historyDeliveryAccepted(result bridgev2.EventHandlingResult) bool {
	return result.Success
}

func (ic *InlineClient) queueInlineMessageDelete(chatID, messageID int64) bridgev2.EventHandlingResult {
	return ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.MessageRemove{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventMessageRemove,
			PortalKey: ic.portalKeyForChat(chatID),
			Timestamp: time.Now(),
		},
		TargetMessage: makeMessageID(chatID, messageID),
	})
}

func (ic *InlineClient) queueChatHistoryClear(ctx context.Context, evt *sidecar.ChatHistoryClearedEvent) error {
	if evt == nil {
		return nil
	}
	end := time.Unix(253402300799, 0)
	if evt.BeforeDate != nil {
		end = time.Unix(*evt.BeforeDate, 0).Add(-time.Nanosecond)
	}
	messages, err := ic.UserLogin.Bridge.DB.Message.GetMessagesBetweenTimeQuery(
		ctx,
		ic.portalKeyForChat(evt.ChatID),
		time.Unix(0, 0).Add(-time.Nanosecond),
		end,
	)
	if err != nil {
		return fmt.Errorf("query cleared Inline history from bridge database: %w", err)
	}
	seen := make(map[networkid.MessageID]struct{}, len(messages))
	for _, message := range messages {
		if message == nil {
			continue
		}
		chatID, messageID, err := parseMessageID(message.ID)
		if err != nil || chatID != evt.ChatID {
			continue
		}
		if _, duplicate := seen[message.ID]; duplicate {
			continue
		}
		seen[message.ID] = struct{}{}
		if err := eventHandlingResultError("cleared message delete", ic.queueInlineMessageDelete(chatID, messageID)); err != nil {
			return err
		}
	}
	return nil
}

func (ic *InlineClient) queueInlineReaction(evt *sidecar.ReactionChangedEvent) bridgev2.EventHandlingResult {
	if evt == nil || evt.UserID <= 0 || strings.TrimSpace(evt.Reaction) == "" {
		return bridgev2.EventHandlingResultIgnored
	}
	eventType := bridgev2.RemoteEventReaction
	if evt.Removed {
		eventType = bridgev2.RemoteEventReactionRemove
	}
	userID := strconv.FormatInt(evt.UserID, 10)
	return ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.Reaction{
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

func (ic *InlineClient) queueInlineTyping(evt *sidecar.TypingEvent) bridgev2.EventHandlingResult {
	if evt == nil {
		return bridgev2.EventHandlingResultIgnored
	}
	timeout := 0 * time.Second
	if evt.IsTyping {
		timeout = 5 * time.Second
	}
	return ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.Typing{
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
			Timestamp:      inlineTimestamp(msg.Timestamp),
			StreamOrder:    msg.MessageID,
			PostHandleFunc: ic.projectionPostHandler,
		},
		Data:               msg,
		ID:                 makeMessageID(msg.ChatID, msg.MessageID),
		TransactionID:      transactionIDForMessage(msg),
		ConvertMessageFunc: ic.convertInlineMessage,
		HandleExistingFunc: ic.upsertExistingInlineMessage,
		ConvertEditFunc:    ic.convertInlineMessageEdit,
		TargetMessage:      makeMessageID(msg.ChatID, msg.MessageID),
	}
}

func upsertExistingInlineMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data sidecar.MessageRecord) (bridgev2.UpsertResult, error) {
	if len(existing) == 0 {
		return bridgev2.UpsertResult{ContinueMessageHandling: true}, nil
	}
	if staleInlineRevision(existing[0], data) {
		return bridgev2.UpsertResult{}, nil
	}
	fingerprint := inlineMessageFingerprint(data)
	if metadata, ok := existing[0].Metadata.(*MessageMetadata); ok &&
		metadata != nil &&
		(data.Content.Type != "media" || metadata.MediaHandled) {
		if metadata.InlineFingerprint == fingerprint ||
			(metadata.InlineFingerprint != "" && emptyInlineMessageMetadata(data.Metadata) &&
				metadata.InlineFingerprint == legacyInlineMessageFingerprint(data)) {
			return bridgev2.UpsertResult{}, nil
		}
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
	return convertInlineMessageWithResolver(ctx, portal, intent, data, nil)
}

func convertInlineMessageWithResolver(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data sidecar.MessageRecord, resolve msgconv.InlineToMatrix) (*bridgev2.ConvertedMessage, error) {
	converted, err := convertedContentFromInline(ctx, portal, intent, data.Content)
	if err != nil {
		return nil, err
	}
	applyInlineRichContent(converted, data, resolve)
	if data.ReplyToMessageID != nil {
		converted.ReplyTo = &networkid.MessageOptionalPartID{
			MessageID: makeMessageID(data.ChatID, *data.ReplyToMessageID),
		}
	}
	setInlineMessageFingerprint(converted, data)
	return converted, nil
}

func convertInlineMessageEdit(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data sidecar.MessageRecord) (*bridgev2.ConvertedEdit, error) {
	return convertInlineMessageEditWithResolver(ctx, portal, intent, existing, data, nil)
}

func convertInlineMessageEditWithResolver(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data sidecar.MessageRecord, resolve msgconv.InlineToMatrix) (*bridgev2.ConvertedEdit, error) {
	if len(existing) == 0 {
		return nil, bridgev2.ErrTargetMessageNotFound
	}
	if staleInlineRevision(existing[0], data) {
		return &bridgev2.ConvertedEdit{}, nil
	}
	converted, err := convertedInlineEditContent(ctx, portal, intent, existing[0], data)
	if err != nil {
		return nil, err
	}
	if len(converted.Parts) == 0 {
		return &bridgev2.ConvertedEdit{}, nil
	}
	applyInlineRichContent(converted, data, resolve)
	setInlineMessageFingerprint(converted, data)
	return &bridgev2.ConvertedEdit{
		ModifiedParts: []*bridgev2.ConvertedEditPart{
			converted.Parts[0].ToEditPart(existing[0]),
		},
	}, nil
}

func setInlineMessageFingerprint(converted *bridgev2.ConvertedMessage, data sidecar.MessageRecord) {
	if converted == nil {
		return
	}
	metadata := &MessageMetadata{
		InlineFingerprint: inlineMessageFingerprint(data),
		MediaHandled:      data.Content.Type == "media",
		InlineRevision:    data.Metadata.Revision,
		InlineEditTime:    data.Metadata.EditTimestamp,
	}
	for _, part := range converted.Parts {
		if part != nil {
			if data.Content.Type == "media" && part.Content != nil && part.Content.MsgType.IsMedia() {
				media := *part.Content
				metadata.InlineMedia = &media
				metadata.InlineMediaID = inlineMediaIdentity(data.Content)
			}
			part.DBMetadata = metadata
		}
	}
}

func inlineMessageFingerprint(data sidecar.MessageRecord) string {
	payload, err := json.Marshal(struct {
		SenderID         int64                   `json:"sender_id"`
		IsOutgoing       bool                    `json:"is_outgoing"`
		Content          sidecar.MessageContent  `json:"content"`
		ReplyToMessageID *int64                  `json:"reply_to_message_id,omitempty"`
		Metadata         sidecar.MessageMetadata `json:"metadata"`
	}{
		SenderID:         data.SenderID,
		IsOutgoing:       data.IsOutgoing,
		Content:          data.Content,
		ReplyToMessageID: data.ReplyToMessageID,
		Metadata:         data.Metadata,
	})
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("%x", digest[:])
}

// Legacy hashes omitted all metadata. Only semantically empty metadata can use
// that equality proof: a first rich correction or revisionless edit must survive.
func emptyInlineMessageMetadata(m sidecar.MessageMetadata) bool {
	return m.Mentioned == nil && m.EditTimestamp == nil && m.Revision == nil &&
		m.SenderIsBot == nil && !m.IsService && len(m.Entities) == 0 &&
		len(m.Attachments) == 0 && len(m.Actions) == 0 && m.AgentSession == nil
}

func legacyInlineMessageFingerprint(data sidecar.MessageRecord) string {
	payload, err := json.Marshal(struct {
		SenderID         int64                  `json:"sender_id"`
		IsOutgoing       bool                   `json:"is_outgoing"`
		Content          sidecar.MessageContent `json:"content"`
		ReplyToMessageID *int64                 `json:"reply_to_message_id,omitempty"`
	}{data.SenderID, data.IsOutgoing, data.Content, data.ReplyToMessageID})
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("%x", digest[:])
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
		if isPermanentMediaFailure(err) {
			return convertedInlineMediaUnavailable(content), nil
		}
		return nil, fmt.Errorf("failed to download Inline media: %w", err)
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
		return nil, permanentMediaFailure(fmt.Errorf("parse %s URL: %w", label, err))
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return nil, permanentMediaFailure(fmt.Errorf("unsupported %s URL scheme", label))
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
		err := fmt.Errorf("download %s returned HTTP %d", label, resp.StatusCode)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 &&
			resp.StatusCode != http.StatusRequestTimeout &&
			resp.StatusCode != http.StatusTooEarly &&
			resp.StatusCode != http.StatusTooManyRequests {
			return nil, permanentMediaFailure(err)
		}
		return nil, err
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", label, err)
	}
	if int64(len(data)) > maxBytes {
		return nil, permanentMediaFailure(fmt.Errorf("%s exceeds %d byte bridge download limit", label, maxBytes))
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

func stringPtrEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func int64PtrEqual(left, right *int64) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func uint32PtrEqual(left, right *uint32) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
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
