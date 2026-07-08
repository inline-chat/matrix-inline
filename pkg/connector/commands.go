package connector

import (
	"context"
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

var (
	commandInlineStatus = &commands.FullHandler{
		Func:          fnInlineStatus,
		Name:          "inline-status",
		Aliases:       []string{"istatus"},
		RequiresLogin: true,
		Help: commands.HelpMeta{
			Section:     commands.HelpSectionAuth,
			Description: "Check Inline sidecar and bridge login status",
		},
	}
	commandInlineReconnect = &commands.FullHandler{
		Func:          fnInlineReconnect,
		Name:          "inline-reconnect",
		Aliases:       []string{"ireconnect"},
		RequiresLogin: true,
		Help: commands.HelpMeta{
			Section:     commands.HelpSectionAuth,
			Description: "Reconnect the Inline sidecar session and restart bridge event handling",
		},
	}
)

func registerInlineCommands(bridge *bridgev2.Bridge) {
	processor, ok := bridge.Commands.(*commands.Processor)
	if !ok {
		bridge.Log.Warn().Msg("Bridge command processor does not support custom Inline commands")
		return
	}
	processor.AddHandlers(commandInlineStatus, commandInlineReconnect)
}

func fnInlineStatus(ce *commands.Event) {
	login, client := inlineCommandLogin(ce)
	if client == nil {
		ce.Reply("Your default login is not an Inline login.")
		return
	}
	current, err := client.Sidecar.Status(ce.Ctx)
	ce.Reply(inlineStatusSummary(login, client, current, err))
}

func fnInlineReconnect(ce *commands.Event) {
	login, client := inlineCommandLogin(ce)
	if client == nil {
		ce.Reply("Your default login is not an Inline login.")
		return
	}
	ce.Reply("Reconnecting Inline login `%s`...", login.ID)
	if err := reconnectInlineLogin(ce.Ctx, ce.Bridge, login, client); err != nil {
		ce.Reply("Inline reconnect failed: %v", err)
		return
	}
	ce.Reply("Inline reconnect requested. Use `$cmdprefix inline-status` to check the result.")
}

func inlineCommandLogin(ce *commands.Event) (*bridgev2.UserLogin, *InlineClient) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		return nil, nil
	}
	client, ok := login.Client.(*InlineClient)
	if !ok {
		return login, nil
	}
	return login, client
}

func reconnectInlineLogin(ctx context.Context, bridge *bridgev2.Bridge, login *bridgev2.UserLogin, current *InlineClient) error {
	current.Disconnect()
	if err := bridge.Network.LoadUserLogin(ctx, login); err != nil {
		return fmt.Errorf("reload Inline login: %w", err)
	}
	login.Client.Connect(ctx)
	return nil
}

func inlineStatusSummary(login *bridgev2.UserLogin, client *InlineClient, current *sidecar.Status, err error) string {
	var out strings.Builder
	out.WriteString("Inline status\n\n")
	if login != nil {
		fmt.Fprintf(&out, "- Login: `%s`\n", login.ID)
		fmt.Fprintf(&out, "- Bridge state: `%s`\n", login.BridgeState.GetPrev().StateEvent)
	}
	if client != nil {
		fmt.Fprintf(&out, "- Account ID: `%s`\n", client.AccountID)
		fmt.Fprintf(&out, "- Go client logged in: `%t`\n", client.IsLoggedIn())
	}
	if err != nil {
		fmt.Fprintf(&out, "- Sidecar: check failed: `%v`\n", err)
		return out.String()
	}
	if current == nil {
		out.WriteString("- Sidecar: no status returned\n")
		return out.String()
	}
	fmt.Fprintf(&out, "- Sidecar: `%s`\n", current.Status)
	if current.Protocol.ClientVersion != "" || current.Protocol.ProtocolVersion != 0 {
		fmt.Fprintf(&out, "- Sidecar protocol: `%d`, client: `%s`\n", current.Protocol.ProtocolVersion, current.Protocol.ClientVersion)
	}
	if current.Failure != nil {
		fmt.Fprintf(&out, "- Last failure: `%s`: %s\n", current.Failure.Category, current.Failure.Message)
	}
	return out.String()
}
