package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
)

type Client struct {
	BaseURL          string
	HTTP             *http.Client
	SessionNamespace string
}

// ErrEventReplayUnavailable means the adapter no longer retains the bridge's
// requested lossless event history.
var ErrEventReplayUnavailable = errors.New("Inline sidecar event replay is unavailable")

type EventReplayUnavailableError struct {
	RequestedAfterSequence *uint64 `json:"requested_after_sequence,omitempty"`
	OldestRetainedSequence *uint64 `json:"oldest_retained_sequence,omitempty"`
	LatestSequence         *uint64 `json:"latest_sequence,omitempty"`
	EventGeneration        string  `json:"event_generation,omitempty"`
	ResetRequired          bool    `json:"reset_required,omitempty"`
}

func (err *EventReplayUnavailableError) Error() string {
	if err == nil || err.LatestSequence == nil {
		return ErrEventReplayUnavailable.Error()
	}
	return fmt.Sprintf("%s (latest sequence %d)", ErrEventReplayUnavailable, *err.LatestSequence)
}

func (err *EventReplayUnavailableError) Is(target error) bool {
	return target == ErrEventReplayUnavailable
}

func NewClient(baseURL string) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{
		BaseURL: baseURL,
		HTTP:    &http.Client{Timeout: 2 * time.Minute},
	}
}

func (c *Client) WithSessionNamespace(namespace string) *Client {
	copy := *c
	copy.SessionNamespace = strings.TrimSpace(namespace)
	return &copy
}

func (c *Client) Health(ctx context.Context) (*Health, error) {
	var health Health
	if err := c.do(ctx, http.MethodGet, "/health", nil, &health); err != nil {
		return nil, err
	}
	if health.Protocol.ProtocolVersion != ProtocolVersion {
		return nil, fmt.Errorf("unsupported Inline sidecar protocol %d, expected %d", health.Protocol.ProtocolVersion, ProtocolVersion)
	}
	return &health, nil
}

func (c *Client) Status(ctx context.Context) (*Status, error) {
	var status Status
	if err := c.doRPC(ctx, http.MethodGet, "/status", nil, "status", &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (c *Client) AuthStart(ctx context.Context, request AuthStartRequest) (*AuthStartResult, error) {
	var result AuthStartResult
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/auth/start", request, "auth_start", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) AuthVerify(ctx context.Context, request AuthVerifyRequest) (*AuthVerifyResult, error) {
	var result AuthVerifyResult
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/auth/verify", request, "auth_verify", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) Resume(ctx context.Context) (*Status, error) {
	var status Status
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/resume", nil, "status", &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (c *Client) Connect(ctx context.Context, token, namespace string) (*Status, error) {
	request := ConnectRequest{
		Auth:             AccessToken(token),
		AccountNamespace: namespace,
	}
	var status Status
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/connect", request, "status", &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (c *Client) Logout(ctx context.Context) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/logout", nil, "empty", nil)
}

func (c *Client) Dialogs(ctx context.Context, request DialogsRequest) (*DialogsPage, error) {
	var page DialogsPage
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/dialogs", request, "dialogs", &page); err != nil {
		return nil, err
	}
	return &page, nil
}

func (c *Client) CachedDialogs(ctx context.Context, request DialogsRequest) (*DialogsPage, error) {
	var page DialogsPage
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/state/dialogs", request, "dialogs", &page); err != nil {
		return nil, err
	}
	return &page, nil
}

func (c *Client) AccountState(ctx context.Context) (*AccountStateSnapshot, error) {
	var snapshot AccountStateSnapshot
	if err := c.doRPC(ctx, http.MethodGet, "/rpc/state/account", nil, "account_state", &snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (c *Client) ChatState(ctx context.Context, request ChatStateRequest) (*ChatStateSnapshot, error) {
	var snapshot ChatStateSnapshot
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/state/chat", request, "chat_state", &snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (c *Client) History(ctx context.Context, request HistoryRequest) (*HistoryPage, error) {
	var page HistoryPage
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/history", request, "history", &page); err != nil {
		return nil, err
	}
	return &page, nil
}

func (c *Client) CachedHistory(ctx context.Context, request HistoryRequest) (*HistoryPage, error) {
	var page HistoryPage
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/state/chat/history", request, "history", &page); err != nil {
		return nil, err
	}
	return &page, nil
}

func (c *Client) ChatParticipants(ctx context.Context, request ChatParticipantsRequest) (*ChatParticipantsPage, error) {
	var page ChatParticipantsPage
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/chat/participants", request, "chat_participants", &page); err != nil {
		return nil, err
	}
	return &page, nil
}

func (c *Client) AddChatParticipant(ctx context.Context, request AddChatParticipantRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/chat/participants/add", request, "empty", nil)
}

func (c *Client) RemoveChatParticipant(ctx context.Context, request RemoveChatParticipantRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/chat/participants/remove", request, "empty", nil)
}

func (c *Client) UpdateChatInfo(ctx context.Context, request UpdateChatInfoRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/chat/info", request, "empty", nil)
}

func (c *Client) DeleteChat(ctx context.Context, request DeleteChatRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/chat/delete", request, "empty", nil)
}

func (c *Client) CreateDM(ctx context.Context, request CreateDMRequest) (*CreatedChat, error) {
	var chat CreatedChat
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/chat/create-dm", request, "created_chat", &chat); err != nil {
		return nil, err
	}
	return &chat, nil
}

func (c *Client) CreateThread(ctx context.Context, request CreateThreadRequest) (*CreatedChat, error) {
	var chat CreatedChat
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/chat/create-thread", request, "created_chat", &chat); err != nil {
		return nil, err
	}
	return &chat, nil
}

func (c *Client) CreateReplyThread(ctx context.Context, request CreateReplyThreadRequest) (*CreatedChat, error) {
	var chat CreatedChat
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/chat/create-reply-thread", request, "created_chat", &chat); err != nil {
		return nil, err
	}
	return &chat, nil
}

func (c *Client) SendText(ctx context.Context, request SendTextRequest) (*MessageMutation, error) {
	var mutation MessageMutation
	if err := c.doRPC(ctx, http.MethodPost, "/rpc/send", request, "message", &mutation); err != nil {
		return nil, err
	}
	return &mutation, nil
}

func (c *Client) Upload(ctx context.Context, request UploadRequest, data []byte) (*MessageMutation, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	metadata, err := writer.CreateFormField("metadata")
	if err != nil {
		return nil, fmt.Errorf("create upload metadata field: %w", err)
	}
	if err := json.NewEncoder(metadata).Encode(request); err != nil {
		return nil, fmt.Errorf("encode upload metadata: %w", err)
	}
	fileName := "upload.bin"
	if request.FileName != nil && strings.TrimSpace(*request.FileName) != "" {
		fileName = strings.TrimSpace(*request.FileName)
	}
	file, err := writer.CreateFormFile("file", fileName)
	if err != nil {
		return nil, fmt.Errorf("create upload file field: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		return nil, fmt.Errorf("write upload file field: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("finalize upload multipart body: %w", err)
	}

	var response Response
	if err := c.doRaw(ctx, http.MethodPost, "/rpc/upload", writer.FormDataContentType(), &body, &response); err != nil {
		return nil, err
	}
	var mutation MessageMutation
	if err := decodeRPCResponse(response, "message", &mutation); err != nil {
		return nil, err
	}
	return &mutation, nil
}

func (c *Client) EditMessage(ctx context.Context, request EditMessageRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/edit", request, "empty", nil)
}

func (c *Client) DeleteMessage(ctx context.Context, request DeleteMessageRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/delete", request, "empty", nil)
}

func (c *Client) React(ctx context.Context, request ReactRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/react", request, "empty", nil)
}

func (c *Client) Read(ctx context.Context, request ReadRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/read", request, "empty", nil)
}

func (c *Client) SetMarkedUnread(ctx context.Context, request SetMarkedUnreadRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/marked-unread", request, "empty", nil)
}

func (c *Client) UpdateDialogNotifications(ctx context.Context, request UpdateDialogNotificationsRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/dialog/notifications", request, "empty", nil)
}

func (c *Client) Typing(ctx context.Context, request TypingRequest) error {
	return c.doRPC(ctx, http.MethodPost, "/rpc/typing", request, "empty", nil)
}

func (c *Client) Events(ctx context.Context) (*EventStream, error) {
	return c.EventsAfterGeneration(ctx, "", "", 0)
}

// EventsAfter opens the event stream and requests durable replay after the
// last contiguous sequence persisted by the bridge.
func (c *Client) EventsAfter(ctx context.Context, namespace string, afterSequence uint64) (*EventStream, error) {
	return c.EventsAfterGeneration(ctx, namespace, "", afterSequence)
}

// EventsAfterGeneration opens the event stream within one durable adapter
// event-log generation.
func (c *Client) EventsAfterGeneration(ctx context.Context, namespace, generation string, afterSequence uint64) (*EventStream, error) {
	query := url.Values{}
	query.Set("after_sequence", strconv.FormatUint(afterSequence, 10))
	if strings.TrimSpace(namespace) != "" {
		query.Set("session_namespace", namespace)
	}
	if strings.TrimSpace(generation) != "" {
		query.Set("generation", generation)
	}
	eventsURL := websocketURL(c.BaseURL, "/ws/events?"+query.Encode())
	options := &websocket.DialOptions{}
	if strings.TrimSpace(c.SessionNamespace) != "" {
		options.HTTPHeader = http.Header{"X-Inline-Session-Namespace": []string{c.SessionNamespace}}
	}
	conn, response, err := websocket.Dial(ctx, eventsURL, options)
	if err != nil {
		if response != nil && response.StatusCode == http.StatusGone {
			defer response.Body.Close()
			var replayErr EventReplayUnavailableError
			if decodeErr := json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&replayErr); decodeErr != nil {
				return nil, ErrEventReplayUnavailable
			}
			return nil, &replayErr
		}
		return nil, err
	}
	return &EventStream{conn: conn}, nil
}

// AckEvents acknowledges durable processing through sequence.
func (c *Client) AckEvents(ctx context.Context, namespace string, sequence uint64) error {
	return c.AckEventsGeneration(ctx, namespace, "", sequence)
}

// AckEventsGeneration acknowledges durable processing within one event-log generation.
func (c *Client) AckEventsGeneration(ctx context.Context, namespace, generation string, sequence uint64) error {
	request := EventAckRequest{SessionNamespace: namespace, Generation: generation, Sequence: sequence}
	var response EventAckResponse
	return c.do(ctx, http.MethodPost, "/rpc/events/ack", request, &response)
}

type EventStream struct {
	conn *websocket.Conn
}

func (stream *EventStream) Recv(ctx context.Context) (*EventEnvelope, error) {
	messageType, reader, err := stream.conn.Reader(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText {
		_, _ = io.Copy(io.Discard, reader)
		return nil, fmt.Errorf("unexpected Inline sidecar event message type %v", messageType)
	}
	var envelope EventEnvelope
	if err := json.NewDecoder(reader).Decode(&envelope); err != nil {
		return nil, err
	}
	if envelope.ProtocolVersion != ProtocolVersion {
		return nil, fmt.Errorf("unsupported Inline sidecar protocol %d, expected %d", envelope.ProtocolVersion, ProtocolVersion)
	}
	return &envelope, nil
}

func (stream *EventStream) Close(status websocket.StatusCode, reason string) error {
	return stream.conn.Close(status, reason)
}

func (c *Client) doRPC(ctx context.Context, method, path string, body any, resultType string, out any) error {
	var response Response
	if err := c.do(ctx, method, path, body, &response); err != nil {
		return err
	}
	return decodeRPCResponse(response, resultType, out)
}

func decodeRPCResponse(response Response, resultType string, out any) error {
	if response.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported Inline sidecar protocol %d, expected %d", response.ProtocolVersion, ProtocolVersion)
	}
	switch response.Outcome.Status {
	case "ok":
		var result Result
		if err := json.Unmarshal(response.Outcome.Data, &result); err != nil {
			return fmt.Errorf("decode Inline sidecar result: %w", err)
		}
		if result.Type != resultType {
			return fmt.Errorf("unexpected Inline sidecar result %q, expected %q", result.Type, resultType)
		}
		if out == nil {
			return nil
		}
		if err := json.Unmarshal(result.Data, out); err != nil {
			return fmt.Errorf("decode Inline sidecar %s result: %w", result.Type, err)
		}
		return nil
	case "error":
		var sidecarErr Error
		if err := json.Unmarshal(response.Outcome.Data, &sidecarErr); err != nil {
			return fmt.Errorf("decode Inline sidecar error: %w", err)
		}
		return &sidecarErr
	default:
		return fmt.Errorf("unknown Inline sidecar outcome %q", response.Outcome.Status)
	}
}

func websocketURL(baseURL, path string) string {
	baseURL = strings.TrimRight(baseURL, "/")
	switch {
	case strings.HasPrefix(baseURL, "https://"):
		return "wss://" + strings.TrimPrefix(baseURL, "https://") + path
	case strings.HasPrefix(baseURL, "http://"):
		return "ws://" + strings.TrimPrefix(baseURL, "http://") + path
	default:
		return baseURL + path
	}
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var reqBody *bytes.Reader
	if body == nil {
		reqBody = bytes.NewReader(nil)
	} else {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode Inline sidecar request: %w", err)
		}
		reqBody = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reqBody)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return c.doRequest(req, out)
}

func (c *Client) doRaw(ctx context.Context, method, path, contentType string, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return c.doRequest(req, out)
}

func (c *Client) doRequest(req *http.Request, out any) error {
	if strings.TrimSpace(c.SessionNamespace) != "" {
		req.Header.Set("X-Inline-Session-Namespace", c.SessionNamespace)
	}
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Inline sidecar %s %s returned HTTP %d", req.Method, req.URL.Path, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode Inline sidecar response: %w", err)
	}
	return nil
}
