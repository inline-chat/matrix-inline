package connector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

const inlineLoginDeviceName = "matrix-inline bridge"

type InlineCodeLogin struct {
	User           *bridgev2.User
	SidecarURL     string
	StoreNamespace string
	Kind           sidecar.AuthContactKind
	contact        string
	challengeToken string
}

var _ bridgev2.LoginProcessUserInput = (*InlineCodeLogin)(nil)

func (login *InlineCodeLogin) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	return login.contactStep(login.contactInstructions()), nil
}

func (login *InlineCodeLogin) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	if login.contact == "" {
		return login.submitContact(ctx, input)
	}
	return login.submitCode(ctx, input)
}

func (login *InlineCodeLogin) submitContact(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	contact := strings.TrimSpace(input[login.contactFieldID()])
	if contact == "" {
		return login.contactStep(login.contactRequiredInstructions()), nil
	}
	sidecarClient := sidecar.NewClient(login.SidecarURL).WithSessionNamespace(login.StoreNamespace)
	start, err := sidecarClient.AuthStart(ctx, sidecar.AuthStartRequest{
		Contact:    contact,
		Kind:       login.Kind,
		DeviceName: inlineLoginDeviceName,
	})
	if err != nil {
		return login.contactStep(fmt.Sprintf("Inline login failed: %v", err)), nil
	}
	if start.NeedsInviteCode {
		return login.contactStep("This Inline account requires an invite code. Complete signup in Inline, then log in through the bridge."), nil
	}
	if !start.ExistingUser {
		return login.contactStep("No existing Inline account was found for that contact. Create or join Inline first, then try again."), nil
	}
	login.contact = contact
	login.challengeToken = start.ChallengeToken
	return login.codeStep("Enter the verification code sent by Inline."), nil
}

func (login *InlineCodeLogin) submitCode(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	code := strings.TrimSpace(input["verification_code"])
	if code == "" {
		return login.codeStep("Inline verification code is required."), nil
	}
	sidecarClient := sidecar.NewClient(login.SidecarURL).WithSessionNamespace(login.StoreNamespace)
	verify, err := sidecarClient.AuthVerify(ctx, sidecar.AuthVerifyRequest{
		Contact:          login.contact,
		Kind:             login.Kind,
		Code:             code,
		ChallengeToken:   login.challengeToken,
		DeviceName:       inlineLoginDeviceName,
		AccountNamespace: login.StoreNamespace,
	})
	if err != nil {
		return login.codeStep(fmt.Sprintf("Inline login failed: %v", err)), nil
	}
	accountID := strconv.FormatInt(verify.UserID, 10)
	storeNamespace := strings.TrimSpace(verify.AccountNamespace)
	if storeNamespace == "" {
		storeNamespace = accountID
	}

	meta := &UserLoginMetadata{
		AccountID:          accountID,
		RemoteName:         inlineRemoteDisplayName,
		SidecarURL:         sidecarClient.BaseURL,
		StoreNamespace:     storeNamespace,
		BridgeStateVersion: currentBridgeStateVersion,
	}

	ul, err := login.User.NewLogin(ctx, &database.UserLogin{
		ID:            networkid.UserLoginID(accountID),
		RemoteName:    inlineRemoteDisplayName,
		RemoteProfile: status.RemoteProfile{Name: inlineRemoteDisplayName},
		Metadata:      meta,
	}, &bridgev2.NewLoginParams{
		LoadUserLogin: func(ctx context.Context, ul *bridgev2.UserLogin) error {
			ul.Client = newInlineClient(ul, meta, sidecarClient.BaseURL)
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Inline user login: %w", err)
	}

	connectCompletedLogin(ctx, ul)
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       "chat.inline.matrix.complete",
		Instructions: "Successfully logged in to Inline.",
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: ul.ID,
			UserLogin:   ul,
		},
	}, nil
}

func newLoginStoreNamespace() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate Inline login namespace: %w", err)
	}
	return "login-" + hex.EncodeToString(bytes[:]), nil
}

func connectCompletedLogin(ctx context.Context, ul *bridgev2.UserLogin) {
	if ul == nil || ul.Client == nil {
		return
	}
	connectCtx := context.WithoutCancel(ctx)
	if ul.Bridge != nil && ul.Bridge.BackgroundCtx != nil {
		connectCtx = ul.Bridge.BackgroundCtx
	}
	ul.Client.Connect(connectCtx)
}

func (login *InlineCodeLogin) Cancel() {}

func (login *InlineCodeLogin) contactStep(instructions string) *bridgev2.LoginStep {
	field := bridgev2.LoginInputDataField{
		Type:        bridgev2.LoginInputFieldTypeEmail,
		ID:          "email",
		Name:        "Inline email",
		Description: "Email address for an existing Inline account.",
	}
	if login.Kind == sidecar.AuthContactPhone {
		field = bridgev2.LoginInputDataField{
			Type:        bridgev2.LoginInputFieldTypePhoneNumber,
			ID:          "phone_number",
			Name:        "Inline phone number",
			Description: "Phone number for an existing Inline account, in international format.",
		}
	}
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "chat.inline.matrix.enter_contact",
		Instructions: instructions,
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{field},
		},
	}
}

func (login *InlineCodeLogin) codeStep(instructions string) *bridgev2.LoginStep {
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "chat.inline.matrix.enter_code",
		Instructions: instructions,
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{{
				Type:        bridgev2.LoginInputFieldType2FACode,
				ID:          "verification_code",
				Name:        "Inline verification code",
				Description: "Code sent by Inline.",
			}},
		},
	}
}

func (login *InlineCodeLogin) contactFieldID() string {
	if login.Kind == sidecar.AuthContactPhone {
		return "phone_number"
	}
	return "email"
}

func (login *InlineCodeLogin) contactInstructions() string {
	if login.Kind == sidecar.AuthContactPhone {
		return "Enter the phone number for your existing Inline account."
	}
	return "Enter the email for your existing Inline account."
}

func (login *InlineCodeLogin) contactRequiredInstructions() string {
	if login.Kind == sidecar.AuthContactPhone {
		return "Inline phone number is required."
	}
	return "Inline email is required."
}
