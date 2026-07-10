package connector

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	"go.mau.fi/util/configupgrade"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

const (
	loginFlowEmail = "chat.inline.matrix.email"
	loginFlowPhone = "chat.inline.matrix.phone"

	inlineRemoteDisplayName = "Inline"

	defaultDisplayName               = "Inline"
	defaultNetworkURL                = "https://inline.chat"
	defaultNetworkIcon               = "mxc://matrix.org/ITxccqHQkLCnPQDouWfsPhqs"
	defaultNetworkID                 = "inline"
	defaultBeeperBridgeType          = "github.com/inline-chat/matrix-inline"
	defaultPort                      = 29343
	defaultCommandPrefix             = "!inline"
	currentBridgeStateVersion uint32 = 2
)

type InlineConnector struct {
	br     *bridgev2.Bridge
	Config Config
}

type Config struct {
	DisplayName string `yaml:"displayname" json:"displayname"`
	NetworkURL  string `yaml:"network_url" json:"network_url"`
	NetworkIcon string `yaml:"network_icon" json:"network_icon"`
	SidecarURL  string `yaml:"sidecar_url" json:"sidecar_url"`
}

type UserLoginMetadata struct {
	AccountID           string `json:"account_id"`
	RemoteName          string `json:"remote_name,omitempty"`
	SidecarURL          string `json:"sidecar_url,omitempty"`
	StoreNamespace      string `json:"store_namespace,omitempty"`
	LastSidecarSequence uint64 `json:"last_sidecar_sequence,omitempty"`
	BridgeStateVersion  uint32 `json:"bridge_state_version,omitempty"`
	SessionInvalidated  bool   `json:"session_invalidated,omitempty"`
}

type MessageMetadata struct {
	InlineFingerprint string `json:"inline_fingerprint,omitempty"`
	MediaHandled      bool   `json:"media_handled,omitempty"`
}

func (meta *MessageMetadata) CopyFrom(other any) {
	switch source := other.(type) {
	case *MessageMetadata:
		if source != nil {
			*meta = *source
		}
	case MessageMetadata:
		*meta = source
	}
}

var _ bridgev2.NetworkConnector = (*InlineConnector)(nil)

func (ic *InlineConnector) Init(bridge *bridgev2.Bridge) {
	ic.br = bridge
	registerInlineCommands(bridge)
}

func (ic *InlineConnector) Start(ctx context.Context) error {
	if _, ok := ic.br.Matrix.(bridgev2.MatrixConnectorWithServer); !ok {
		return fmt.Errorf("matrix connector does not implement MatrixConnectorWithServer")
	}
	if err := ic.validateConfig(); err != nil {
		return err
	}
	if err := validatePortalEventDelivery(); err != nil {
		return err
	}
	return nil
}

func validatePortalEventDelivery() error {
	if bridgev2.PortalEventBuffer == 0 {
		return nil
	}
	return fmt.Errorf(
		"MAUTRIX_PORTAL_EVENT_BUFFER must be 0 for durable Inline event delivery (got %d)",
		bridgev2.PortalEventBuffer,
	)
}

func (ic *InlineConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 2, 1
}

func (ic *InlineConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{
		AggressiveUpdateInfo: true,
		ImplicitReadReceipts: true,
		Provisioning: bridgev2.ProvisioningCapabilities{
			ResolveIdentifier: bridgev2.ResolveIdentifierCapabilities{
				LookupUsername: true,
				Search:         false,
				ContactList:    false,
				CreateDM:       true,
			},
			GroupCreation: map[string]bridgev2.GroupTypeCapabilities{
				"thread": {
					TypeDescription: "Inline thread",
					Name: bridgev2.GroupFieldCapability{
						Allowed:   true,
						Required:  true,
						MinLength: 1,
						MaxLength: 256,
					},
					Topic: bridgev2.GroupFieldCapability{
						Allowed:   true,
						MaxLength: 4096,
					},
					Participants: bridgev2.GroupFieldCapability{
						Allowed: true,
					},
				},
			},
		},
	}
}

func (ic *InlineConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:          configString(ic.Config.DisplayName, defaultDisplayName),
		NetworkURL:           configString(ic.Config.NetworkURL, defaultNetworkURL),
		NetworkIcon:          id.ContentURIString(configString(ic.Config.NetworkIcon, defaultNetworkIcon)),
		NetworkID:            defaultNetworkID,
		BeeperBridgeType:     defaultBeeperBridgeType,
		DefaultPort:          defaultPort,
		DefaultCommandPrefix: defaultCommandPrefix,
	}
}

func (ic *InlineConnector) GetConfig() (example string, data any, upgrader configupgrade.Upgrader) {
	return `displayname: Inline
network_url: https://inline.chat
# Defaults to the official Inline bridge icon. Override with a Matrix Content URI (mxc://...).
network_icon: ""
sidecar_url: http://127.0.0.1:29342
`, &ic.Config, nil
}

func (ic *InlineConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		Message: func() any {
			return &MessageMetadata{}
		},
		UserLogin: func() any {
			return &UserLoginMetadata{}
		},
	}
}

func (ic *InlineConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	meta := login.Metadata.(*UserLoginMetadata)
	sidecarURL := ic.sidecarURL(meta.SidecarURL)
	if err := validateSidecarURL(sidecarURL); err != nil {
		return err
	}
	login.Client = newInlineClient(login, meta, sidecarURL)
	return nil
}

func (ic *InlineConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{
		{
			Name:        "Email login",
			Description: "Login to Inline with an email verification code",
			ID:          loginFlowEmail,
		},
		{
			Name:        "SMS login",
			Description: "Login to Inline with an SMS verification code",
			ID:          loginFlowPhone,
		},
	}
}

func (ic *InlineConnector) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	kind := sidecar.AuthContactEmail
	switch flowID {
	case "", loginFlowEmail:
		kind = sidecar.AuthContactEmail
	case loginFlowPhone:
		kind = sidecar.AuthContactPhone
	default:
		return nil, fmt.Errorf("unsupported Inline login flow %q", flowID)
	}
	storeNamespace, err := newLoginStoreNamespace()
	if err != nil {
		return nil, err
	}
	sidecarURL := ic.sidecarURL("")
	if err := validateSidecarURL(sidecarURL); err != nil {
		return nil, err
	}
	return &InlineCodeLogin{
		User:           user,
		SidecarURL:     sidecarURL,
		StoreNamespace: storeNamespace,
		Kind:           kind,
	}, nil
}

func (ic *InlineConnector) sidecarURL(override string) string {
	if strings.TrimSpace(override) != "" {
		return override
	}
	if envURL := strings.TrimSpace(os.Getenv("INLINE_SIDECAR_URL")); envURL != "" {
		return envURL
	}
	if strings.TrimSpace(ic.Config.SidecarURL) != "" {
		return ic.Config.SidecarURL
	}
	return sidecar.DefaultBaseURL
}

func (ic *InlineConnector) validateConfig() error {
	if err := validateSidecarURL(ic.sidecarURL("")); err != nil {
		return err
	}
	icon := strings.TrimSpace(ic.Config.NetworkIcon)
	if icon == "" {
		return nil
	}
	if _, err := id.ParseContentURI(icon); err != nil {
		return fmt.Errorf("invalid network_icon %q: %w", icon, err)
	}
	return nil
}

func validateSidecarURL(rawURL string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("invalid Inline sidecar URL: %w", err)
	}
	if parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil {
		return fmt.Errorf("Inline sidecar URL must be an unauthenticated loopback http URL")
	}
	if (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("Inline sidecar URL must not include a path, query, or fragment")
	}
	host := strings.TrimSpace(parsed.Hostname())
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("Inline sidecar URL must use a loopback host")
		}
	}
	return nil
}

func configString(value, fallback string) string {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}
	return fallback
}

func newInlineClient(login *bridgev2.UserLogin, meta *UserLoginMetadata, sidecarURL string) *InlineClient {
	storeNamespace := strings.TrimSpace(meta.StoreNamespace)
	if storeNamespace == "" {
		storeNamespace = strings.TrimSpace(meta.AccountID)
		meta.StoreNamespace = storeNamespace
	}
	return &InlineClient{
		UserLogin:           login,
		AccountID:           meta.AccountID,
		Sidecar:             sidecar.NewClient(sidecarURL).WithSessionNamespace(storeNamespace),
		storeNamespace:      storeNamespace,
		lastSidecarSequence: meta.LastSidecarSequence,
		bridgeStateVersion:  meta.BridgeStateVersion,
		loggedIn:            !meta.SessionInvalidated,
		users:               make(map[int64]sidecar.UserRecord),
		dialogs:             make(map[int64]sidecar.DialogRecord),
		details:             make(map[int64]struct{}),
		history:             make(map[int64]int64),
		historyLoaded:       make(map[int64]struct{}),
	}
}

func makeUserID(userID string) networkid.UserID {
	return networkid.UserID(userID)
}

func makePortalID(chatID string) networkid.PortalID {
	return networkid.PortalID(chatID)
}
