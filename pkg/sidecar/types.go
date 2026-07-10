package sidecar

import (
	"encoding/json"
	"fmt"
)

const (
	// ProtocolVersion is the sidecar command/event protocol version expected by
	// this bridge.
	ProtocolVersion = 4

	// DefaultBaseURL is the loopback sidecar endpoint.
	DefaultBaseURL = "http://127.0.0.1:29342"
)

type Health struct {
	OK              bool         `json:"ok"`
	Protocol        ProtocolInfo `json:"protocol"`
	Status          ClientStatus `json:"status"`
	EventGeneration string       `json:"event_generation"`
}

type ProtocolInfo struct {
	ProtocolVersion        int    `json:"protocol_version"`
	ClientVersion          string `json:"client_version"`
	CoreSyncSchemaRevision uint32 `json:"core_sync_schema_revision"`
}

type ClientStatus string

const (
	StatusDisconnected ClientStatus = "Disconnected"
	StatusConnecting   ClientStatus = "Connecting"
	StatusConnected    ClientStatus = "Connected"
	StatusReconnecting ClientStatus = "Reconnecting"
	StatusAuthRequired ClientStatus = "AuthRequired"
	StatusAuthExpired  ClientStatus = "AuthExpired"
	StatusLoggedOut    ClientStatus = "LoggedOut"
	StatusShuttingDown ClientStatus = "ShuttingDown"
)

type AuthCredential struct {
	Type  string `json:"type"`
	Token string `json:"token,omitempty"`
}

func AccessToken(token string) AuthCredential {
	return AuthCredential{Type: "access_token", Token: token}
}

type ConnectRequest struct {
	Auth             AuthCredential `json:"auth"`
	AccountNamespace string         `json:"account_namespace,omitempty"`
}

type AuthContactKind string

const (
	AuthContactEmail AuthContactKind = "email"
	AuthContactPhone AuthContactKind = "phone"
)

type AuthStartRequest struct {
	Contact    string          `json:"contact"`
	Kind       AuthContactKind `json:"kind"`
	DeviceName string          `json:"device_name,omitempty"`
}

type AuthStartResult struct {
	ExistingUser    bool   `json:"existing_user"`
	NeedsInviteCode bool   `json:"needs_invite_code"`
	ChallengeToken  string `json:"challenge_token,omitempty"`
}

type AuthVerifyRequest struct {
	Contact          string          `json:"contact"`
	Kind             AuthContactKind `json:"kind"`
	Code             string          `json:"code"`
	ChallengeToken   string          `json:"challenge_token,omitempty"`
	DeviceName       string          `json:"device_name,omitempty"`
	AccountNamespace string          `json:"account_namespace,omitempty"`
}

type AuthVerifyResult struct {
	UserID           int64  `json:"user_id"`
	AccountNamespace string `json:"account_namespace"`
	Status           Status `json:"status"`
}

type Status struct {
	Protocol ProtocolInfo `json:"protocol"`
	Status   ClientStatus `json:"status"`
	Failure  *Failure     `json:"failure,omitempty"`
}

type Failure struct {
	Category string `json:"category"`
	Message  string `json:"message"`
}

type Error struct {
	Category          string  `json:"category"`
	Message           string  `json:"message"`
	RetryAfterSeconds *uint64 `json:"retry_after_seconds,omitempty"`
}

func (err *Error) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.Category == "" {
		return err.Message
	}
	return err.Category + ": " + err.Message
}

type Response struct {
	ProtocolVersion int             `json:"protocol_version"`
	ID              string          `json:"id"`
	Outcome         ResponseOutcome `json:"outcome"`
}

type ResponseOutcome struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
}

type Result struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type DialogsRequest struct {
	Limit  *uint32 `json:"limit,omitempty"`
	Cursor string  `json:"cursor,omitempty"`
	Order  string  `json:"order,omitempty"`
}

const DialogsOrderStableChatID = "stable_chat_id"

type DialogRecord struct {
	ChatID                 int64   `json:"chat_id"`
	PeerUserID             *int64  `json:"peer_user_id,omitempty"`
	Title                  *string `json:"title,omitempty"`
	Emoji                  *string `json:"emoji,omitempty"`
	LastMessageID          *int64  `json:"last_message_id,omitempty"`
	SyncedThroughMessageID *int64  `json:"synced_through_message_id,omitempty"`
	UnreadCount            *uint32 `json:"unread_count,omitempty"`
	SpaceID                *int64  `json:"space_id,omitempty"`
	IsPublic               *bool   `json:"is_public,omitempty"`
	Archived               *bool   `json:"archived,omitempty"`
	Pinned                 *bool   `json:"pinned,omitempty"`
	Open                   *bool   `json:"open,omitempty"`
	ChatListHidden         *bool   `json:"chat_list_hidden,omitempty"`
	Order                  *string `json:"order,omitempty"`
	PinnedOrder            *string `json:"pinned_order,omitempty"`
	NotificationMode       *string `json:"notification_mode,omitempty"`
	FollowMode             *string `json:"follow_mode,omitempty"`
	PinnedMessageIDs       []int64 `json:"pinned_message_ids,omitempty"`
}

type DialogsPage struct {
	Dialogs    []DialogRecord `json:"dialogs"`
	Users      []UserRecord   `json:"users,omitempty"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

type AccountStateSnapshot struct {
	DeletedChatIDs []int64 `json:"deleted_chat_ids"`
}

type ChatStateRequest struct {
	ChatID int64 `json:"chat_id"`
}

type ReactionRecord struct {
	ChatID    int64  `json:"chat_id"`
	MessageID int64  `json:"message_id"`
	UserID    int64  `json:"user_id"`
	Reaction  string `json:"reaction"`
}

type ReadStateRecord struct {
	ChatID       int64   `json:"chat_id"`
	ReadMaxID    *int64  `json:"read_max_id,omitempty"`
	UnreadCount  *uint32 `json:"unread_count,omitempty"`
	MarkedUnread bool    `json:"marked_unread"`
}

type ChatStateSnapshot struct {
	ChatID                     int64                   `json:"chat_id"`
	Dialog                     *DialogRecord           `json:"dialog,omitempty"`
	Deleted                    bool                    `json:"deleted"`
	DeletedMessageIDs          []int64                 `json:"deleted_message_ids"`
	Reactions                  []ReactionRecord        `json:"reactions"`
	ReactionSnapshotMessageIDs []int64                 `json:"reaction_snapshot_message_ids"`
	ReadState                  *ReadStateRecord        `json:"read_state,omitempty"`
	Participants               []ChatParticipantRecord `json:"participants"`
	ParticipantsComplete       bool                    `json:"participants_complete"`
}

type UserRecord struct {
	UserID      int64   `json:"user_id"`
	DisplayName *string `json:"display_name,omitempty"`
	Username    *string `json:"username,omitempty"`
	FirstName   *string `json:"first_name,omitempty"`
	LastName    *string `json:"last_name,omitempty"`
	AvatarURL   *string `json:"avatar_url,omitempty"`
	IsBot       *bool   `json:"is_bot,omitempty"`
}

type HistoryRequest struct {
	ChatID          int64   `json:"chat_id"`
	Limit           *uint32 `json:"limit,omitempty"`
	BeforeMessageID *int64  `json:"before_message_id,omitempty"`
	AfterMessageID  *int64  `json:"after_message_id,omitempty"`
}

type HistoryPage struct {
	Messages   []MessageRecord `json:"messages"`
	Users      []UserRecord    `json:"users,omitempty"`
	HasMore    bool            `json:"has_more"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type ChatParticipantsRequest struct {
	ChatID int64 `json:"chat_id"`
}

type AddChatParticipantRequest struct {
	ChatID int64 `json:"chat_id"`
	UserID int64 `json:"user_id"`
}

type RemoveChatParticipantRequest struct {
	ChatID int64 `json:"chat_id"`
	UserID int64 `json:"user_id"`
}

type UpdateChatInfoRequest struct {
	ChatID int64   `json:"chat_id"`
	Title  *string `json:"title,omitempty"`
	Emoji  *string `json:"emoji,omitempty"`
}

type DeleteChatRequest struct {
	ChatID int64 `json:"chat_id"`
}

type ChatParticipantRecord struct {
	UserID int64  `json:"user_id"`
	Date   *int64 `json:"date,omitempty"`
}

type ChatParticipantsPage struct {
	Participants []ChatParticipantRecord `json:"participants"`
	Users        []UserRecord            `json:"users,omitempty"`
}

type ChatCreateParticipant struct {
	UserID int64 `json:"user_id"`
}

type CreateDMRequest struct {
	UserID int64 `json:"user_id"`
}

type CreateThreadRequest struct {
	Title        *string                 `json:"title,omitempty"`
	SpaceID      *int64                  `json:"space_id,omitempty"`
	Description  *string                 `json:"description,omitempty"`
	Emoji        *string                 `json:"emoji,omitempty"`
	IsPublic     bool                    `json:"is_public"`
	Participants []ChatCreateParticipant `json:"participants,omitempty"`
}

type CreateReplyThreadRequest struct {
	ParentChatID    int64                   `json:"parent_chat_id"`
	ParentMessageID *int64                  `json:"parent_message_id,omitempty"`
	Title           *string                 `json:"title,omitempty"`
	Description     *string                 `json:"description,omitempty"`
	Emoji           *string                 `json:"emoji,omitempty"`
	Participants    []ChatCreateParticipant `json:"participants,omitempty"`
}

type CreatedChat struct {
	ChatID          int64   `json:"chat_id"`
	Title           *string `json:"title,omitempty"`
	ParentChatID    *int64  `json:"parent_chat_id,omitempty"`
	ParentMessageID *int64  `json:"parent_message_id,omitempty"`
}

type MessageRecord struct {
	ChatID           int64                `json:"chat_id"`
	MessageID        int64                `json:"message_id"`
	SenderID         int64                `json:"sender_id"`
	Timestamp        int64                `json:"timestamp"`
	IsOutgoing       bool                 `json:"is_outgoing"`
	Content          MessageContent       `json:"content"`
	ReplyToMessageID *int64               `json:"reply_to_message_id,omitempty"`
	Transaction      *TransactionIdentity `json:"transaction,omitempty"`
}

type MessageContent struct {
	Type       string  `json:"type"`
	Text       string  `json:"text,omitempty"`
	Kind       string  `json:"kind,omitempty"`
	FileID     string  `json:"file_id,omitempty"`
	URL        string  `json:"url,omitempty"`
	MimeType   *string `json:"mime_type,omitempty"`
	FileName   *string `json:"file_name,omitempty"`
	Caption    *string `json:"caption,omitempty"`
	SizeBytes  *uint64 `json:"size_bytes,omitempty"`
	Width      *uint32 `json:"width,omitempty"`
	Height     *uint32 `json:"height,omitempty"`
	DurationMS *uint64 `json:"duration_ms,omitempty"`
	Reason     string  `json:"reason,omitempty"`
}

type Peer struct {
	Type     string `json:"type"`
	UserID   int64  `json:"user_id,omitempty"`
	ChatID   int64  `json:"chat_id,omitempty"`
	ThreadID int64  `json:"thread_id,omitempty"`
}

func ChatPeer(chatID int64) Peer {
	return Peer{Type: "chat", ChatID: chatID}
}

func UserPeer(userID int64) Peer {
	return Peer{Type: "user", UserID: userID}
}

type ExternalID struct {
	Source string `json:"source"`
	ID     string `json:"id"`
}

type SendTextRequest struct {
	Peer             Peer        `json:"peer"`
	Text             string      `json:"text"`
	ExternalID       *ExternalID `json:"external_id,omitempty"`
	RandomID         *int64      `json:"random_id,omitempty"`
	ReplyToMessageID *int64      `json:"reply_to_message_id,omitempty"`
}

type UploadRequest struct {
	Peer             Peer        `json:"peer"`
	Kind             string      `json:"kind"`
	FileName         *string     `json:"file_name,omitempty"`
	MimeType         *string     `json:"mime_type,omitempty"`
	SizeBytes        *uint64     `json:"size_bytes,omitempty"`
	Caption          *string     `json:"caption,omitempty"`
	Width            *uint32     `json:"width,omitempty"`
	Height           *uint32     `json:"height,omitempty"`
	DurationMS       *uint64     `json:"duration_ms,omitempty"`
	ExternalID       *ExternalID `json:"external_id,omitempty"`
	RandomID         *int64      `json:"random_id,omitempty"`
	ReplyToMessageID *int64      `json:"reply_to_message_id,omitempty"`
}

type EditMessageRequest struct {
	ChatID     int64       `json:"chat_id"`
	MessageID  int64       `json:"message_id"`
	Text       string      `json:"text"`
	ExternalID *ExternalID `json:"external_id,omitempty"`
}

type DeleteMessageRequest struct {
	ChatID     int64       `json:"chat_id"`
	MessageID  int64       `json:"message_id"`
	ExternalID *ExternalID `json:"external_id,omitempty"`
}

type ReactRequest struct {
	ChatID     int64       `json:"chat_id"`
	MessageID  int64       `json:"message_id"`
	Reaction   string      `json:"reaction"`
	Remove     bool        `json:"remove"`
	ExternalID *ExternalID `json:"external_id,omitempty"`
}

type ReadRequest struct {
	ChatID       int64  `json:"chat_id"`
	MaxMessageID *int64 `json:"max_message_id,omitempty"`
}

type SetMarkedUnreadRequest struct {
	ChatID int64 `json:"chat_id"`
	Unread bool  `json:"unread"`
}

type DialogNotificationMode string

const (
	DialogNotificationAll      DialogNotificationMode = "all"
	DialogNotificationMentions DialogNotificationMode = "mentions"
	DialogNotificationNone     DialogNotificationMode = "none"
)

type UpdateDialogNotificationsRequest struct {
	ChatID int64                   `json:"chat_id"`
	Mode   *DialogNotificationMode `json:"mode,omitempty"`
}

type TypingRequest struct {
	ChatID   int64 `json:"chat_id"`
	IsTyping bool  `json:"is_typing"`
}

type MessageMutation struct {
	Transaction TransactionIdentity `json:"transaction"`
	MessageID   *int64              `json:"message_id,omitempty"`
	State       TransactionState    `json:"state"`
	Failure     *Failure            `json:"failure,omitempty"`
}

type TransactionState string

const (
	TransactionQueued    TransactionState = "Queued"
	TransactionSent      TransactionState = "Sent"
	TransactionAcked     TransactionState = "Acked"
	TransactionCompleted TransactionState = "Completed"
	TransactionFailed    TransactionState = "Failed"
	TransactionCancelled TransactionState = "Cancelled"
)

type TransactionIdentity struct {
	TransactionID      string      `json:"transaction_id"`
	ExternalID         *ExternalID `json:"external_id,omitempty"`
	RandomID           int64       `json:"random_id"`
	TemporaryMessageID *int64      `json:"temporary_message_id,omitempty"`
	FinalMessageID     *int64      `json:"final_message_id,omitempty"`
}

type EventReliability string

const (
	EventLossless   EventReliability = "Lossless"
	EventBestEffort EventReliability = "BestEffort"
)

type EventEnvelope struct {
	ProtocolVersion  int              `json:"protocol_version"`
	SessionNamespace string           `json:"session_namespace"`
	Generation       string           `json:"generation"`
	Sequence         *uint64          `json:"sequence,omitempty"`
	Reliability      EventReliability `json:"reliability"`
	Event            ClientEvent      `json:"event"`
}

type EventAckRequest struct {
	SessionNamespace string `json:"session_namespace"`
	Generation       string `json:"generation"`
	Sequence         uint64 `json:"sequence"`
}

type EventAckResponse struct {
	Generation           string `json:"generation"`
	AcknowledgedSequence uint64 `json:"acknowledged_sequence"`
}

type ClientEvent struct {
	Type string

	StatusChanged           *StatusChangedEvent
	TransactionChanged      *TransactionEvent
	ChatUpserted            *ChatIDEvent
	ChatDeleted             *ChatIDEvent
	ChatParticipantsChanged *ChatIDEvent
	UserUpserted            *UserIDEvent
	SpaceUpserted           *SpaceIDEvent
	SpaceMemberChanged      *SpaceMemberChangedEvent
	UserSettingsChanged     *struct{}
	MessageActionInvoked    *MessageActionInvokedEvent
	MessageActionAnswered   *MessageActionAnsweredEvent
	MessageUpserted         *MessageIDEvent
	MessageStored           *MessageStoredEvent
	MessageDeleted          *MessageIDEvent
	ChatHistoryCleared      *ChatHistoryClearedEvent
	ReactionChanged         *ReactionChangedEvent
	ReadStateChanged        *ChatIDEvent
	Typing                  *TypingEvent
	UserStatusChanged       *UserStatusChangedEvent
	BotPresenceChanged      *BotPresenceChangedEvent
	NewMessageNotification  *NewMessageNotificationEvent
}

func (evt *ClientEvent) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) != 1 {
		return fmt.Errorf("Inline sidecar event must contain exactly one variant, got %d", len(raw))
	}
	for typ, payload := range raw {
		evt.Type = typ
		switch typ {
		case "StatusChanged":
			var value StatusChangedEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.StatusChanged = &value
		case "TransactionChanged":
			var value TransactionEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.TransactionChanged = &value
		case "ChatUpserted":
			var value ChatIDEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.ChatUpserted = &value
		case "ChatDeleted":
			var value ChatIDEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.ChatDeleted = &value
		case "ChatParticipantsChanged":
			var value ChatIDEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.ChatParticipantsChanged = &value
		case "UserUpserted":
			var value UserIDEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.UserUpserted = &value
		case "SpaceUpserted":
			var value SpaceIDEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.SpaceUpserted = &value
		case "SpaceMemberChanged":
			var value SpaceMemberChangedEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.SpaceMemberChanged = &value
		case "UserSettingsChanged":
			evt.UserSettingsChanged = &struct{}{}
		case "MessageActionInvoked":
			var value MessageActionInvokedEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.MessageActionInvoked = &value
		case "MessageActionAnswered":
			var value MessageActionAnsweredEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.MessageActionAnswered = &value
		case "MessageUpserted":
			var value MessageIDEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.MessageUpserted = &value
		case "MessageStored":
			var value MessageStoredEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.MessageStored = &value
		case "MessageDeleted":
			var value MessageIDEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.MessageDeleted = &value
		case "ChatHistoryCleared":
			var value ChatHistoryClearedEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.ChatHistoryCleared = &value
		case "ReactionChanged":
			var value ReactionChangedEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.ReactionChanged = &value
		case "ReadStateChanged":
			var value ChatIDEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.ReadStateChanged = &value
		case "Typing":
			var value TypingEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.Typing = &value
		case "UserStatusChanged":
			var value UserStatusChangedEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.UserStatusChanged = &value
		case "BotPresenceChanged":
			var value BotPresenceChangedEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.BotPresenceChanged = &value
		case "NewMessageNotification":
			var value NewMessageNotificationEvent
			if err := json.Unmarshal(payload, &value); err != nil {
				return err
			}
			evt.NewMessageNotification = &value
		default:
			return fmt.Errorf("unsupported Inline sidecar event variant %q", typ)
		}
		return nil
	}
	return fmt.Errorf("Inline sidecar event did not contain a variant")
}

type StatusChangedEvent struct {
	Status  ClientStatus `json:"status"`
	Failure *Failure     `json:"failure,omitempty"`
}

type TransactionEvent struct {
	Identity TransactionIdentity `json:"identity"`
	State    string              `json:"state"`
	Failure  *Failure            `json:"failure,omitempty"`
}

type ChatIDEvent struct {
	ChatID int64 `json:"chat_id"`
}

type UserIDEvent struct {
	UserID int64 `json:"user_id"`
}

type SpaceIDEvent struct {
	SpaceID int64 `json:"space_id"`
}

type SpaceMemberChangedEvent struct {
	SpaceID int64 `json:"space_id"`
	UserID  int64 `json:"user_id"`
	Removed bool  `json:"removed"`
}

type MessageActionInvokedEvent struct {
	InteractionID int64  `json:"interaction_id"`
	ChatID        int64  `json:"chat_id"`
	MessageID     int64  `json:"message_id"`
	ActorUserID   int64  `json:"actor_user_id"`
	ActionID      string `json:"action_id"`
	Data          []byte `json:"data"`
}

type MessageActionAnsweredEvent struct {
	InteractionID int64   `json:"interaction_id"`
	Toast         *string `json:"toast,omitempty"`
}

type MessageIDEvent struct {
	ChatID    int64 `json:"chat_id"`
	MessageID int64 `json:"message_id"`
}

type ChatHistoryClearedEvent struct {
	ChatID     int64  `json:"chat_id"`
	BeforeDate *int64 `json:"before_date,omitempty"`
}

type MessageStoredEvent struct {
	Message MessageRecord `json:"message"`
}

type ReactionChangedEvent struct {
	ChatID    int64  `json:"chat_id"`
	MessageID int64  `json:"message_id"`
	UserID    int64  `json:"user_id"`
	Reaction  string `json:"reaction"`
	Removed   bool   `json:"removed"`
}

type TypingEvent struct {
	ChatID   int64 `json:"chat_id"`
	UserID   int64 `json:"user_id"`
	IsTyping bool  `json:"is_typing"`
}

type UserStatusChangedEvent struct {
	UserID     int64  `json:"user_id"`
	IsOnline   *bool  `json:"is_online,omitempty"`
	LastOnline *int64 `json:"last_online,omitempty"`
}

type BotPresenceChangedEvent struct {
	BotUserID     int64   `json:"bot_user_id"`
	ChatID        *int64  `json:"chat_id,omitempty"`
	Kind          string  `json:"kind"`
	Comment       *string `json:"comment,omitempty"`
	AvatarChanged bool    `json:"avatar_changed"`
}

type NewMessageNotificationEvent struct {
	Message MessageRecord `json:"message"`
	Reason  string        `json:"reason"`
}
