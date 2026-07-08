package connector

import (
	"context"
	"fmt"
	"strings"

	"go.mau.fi/util/configupgrade"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

const (
	loginFlowEmail = "chat.inline.matrix.email"
	loginFlowPhone = "chat.inline.matrix.phone"
)

type InlineConnector struct {
	br     *bridgev2.Bridge
	Config Config
}

type Config struct {
	SidecarURL string `yaml:"sidecar_url" json:"sidecar_url"`
}

type UserLoginMetadata struct {
	AccountID          string `json:"account_id"`
	RemoteName         string `json:"remote_name,omitempty"`
	SidecarURL         string `json:"sidecar_url,omitempty"`
	StoreNamespace     string `json:"store_namespace,omitempty"`
	SessionInvalidated bool   `json:"session_invalidated,omitempty"`
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
	return nil
}

func (ic *InlineConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
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
		DisplayName:      "Inline",
		NetworkURL:       "https://inline.chat",
		NetworkIcon:      "",
		NetworkID:        "inline",
		BeeperBridgeType: "github.com/inline-chat/matrix-inline",
		DefaultPort:      29343,
	}
}

func (ic *InlineConnector) GetConfig() (example string, data any, upgrader configupgrade.Upgrader) {
	return "sidecar_url: http://127.0.0.1:29342\n", &ic.Config, nil
}

func (ic *InlineConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		UserLogin: func() any {
			return &UserLoginMetadata{}
		},
	}
}

func (ic *InlineConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	meta := login.Metadata.(*UserLoginMetadata)
	login.Client = newInlineClient(login, meta, ic.sidecarURL(meta.SidecarURL))
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
	return &InlineCodeLogin{
		User:       user,
		SidecarURL: ic.sidecarURL(""),
		Kind:       kind,
	}, nil
}

func (ic *InlineConnector) sidecarURL(override string) string {
	if strings.TrimSpace(override) != "" {
		return override
	}
	if strings.TrimSpace(ic.Config.SidecarURL) != "" {
		return ic.Config.SidecarURL
	}
	return sidecar.DefaultBaseURL
}

func newInlineClient(login *bridgev2.UserLogin, meta *UserLoginMetadata, sidecarURL string) *InlineClient {
	return &InlineClient{
		UserLogin: login,
		AccountID: meta.AccountID,
		Sidecar:   sidecar.NewClient(sidecarURL),
		loggedIn:  !meta.SessionInvalidated,
		users:     make(map[int64]sidecar.UserRecord),
	}
}

func makeUserID(userID string) networkid.UserID {
	return networkid.UserID(userID)
}

func makePortalID(chatID string) networkid.PortalID {
	return networkid.PortalID(chatID)
}
