package connector

import (
	"context"
	"errors"
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
	commandInlineSettings = &commands.FullHandler{
		Func:          fnInlineSettings,
		Name:          "inline-settings",
		Aliases:       []string{"isettings"},
		RequiresLogin: true,
		Help: commands.HelpMeta{
			Section:     commands.HelpSectionMisc,
			Description: "Show settings for your Inline bridge account",
		},
	}
	commandInlineHiddenChats = &commands.FullHandler{
		Func:          fnInlineHiddenChats,
		Name:          "inline-hidden-chats",
		Aliases:       []string{"ihidden"},
		RequiresLogin: true,
		Help: commands.HelpMeta{
			Section:     commands.HelpSectionMisc,
			Description: "Choose whether Inline chats hidden from the chat list are bridged",
			Args:        "<exclude|include|default>",
		},
	}
)

func registerInlineCommands(bridge *bridgev2.Bridge) {
	processor, ok := bridge.Commands.(*commands.Processor)
	if !ok {
		bridge.Log.Warn().Msg("Bridge command processor does not support custom Inline commands")
		return
	}
	processor.AddHandlers(commandInlineStatus, commandInlineReconnect, commandInlineSettings, commandInlineHiddenChats, commandInlineMigrate, commandInlineChannelDefault)
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

func fnInlineSettings(ce *commands.Event) {
	_, client := inlineCommandLogin(ce)
	if client == nil {
		ce.Reply("Your default login is not an Inline login.")
		return
	}
	ce.Reply(inlineSettingsSummary(client))
}

func fnInlineHiddenChats(ce *commands.Event) {
	login, client := inlineCommandLogin(ce)
	if client == nil {
		ce.Reply("Your default login is not an Inline login.")
		return
	}
	policy, err := parseHiddenDialogsCommand(ce.Args)
	if err != nil {
		ce.Reply("Usage: `$cmdprefix inline-hidden-chats <exclude|include|default>`\n\n%s", err)
		return
	}
	// Stop cursor writers before resetting the durable stable-scan cursor. This
	// prevents an in-flight old-policy page from overwriting the reset.
	client.Disconnect()
	if err := client.persistHiddenDialogsOverride(ce.Ctx, policy); err != nil {
		client.Connect(ce.Ctx)
		ce.Reply("Failed to save Inline hidden chat setting: %v", err)
		return
	}
	if err := loadAndConnectInlineLogin(ce.Ctx, ce.Bridge, login); err != nil {
		client.Connect(ce.Ctx)
		ce.Reply("Hidden chat setting was saved, but the Inline bridge reconnect failed: %v", err)
		return
	}
	updated, ok := login.Client.(*InlineClient)
	if !ok {
		ce.Reply("Hidden chat setting was saved, but the reloaded Inline client is unavailable.")
		return
	}
	ce.Reply("Inline hidden chat setting updated.\n\n%s", inlineSettingsSummary(updated))
}

func parseHiddenDialogsCommand(args []string) (hiddenDialogsPolicy, error) {
	if len(args) != 1 {
		return "", errors.New("choose exactly one setting")
	}
	switch strings.ToLower(strings.TrimSpace(args[0])) {
	case "exclude", "hide", "off":
		return hiddenDialogsExclude, nil
	case "include", "show", "on":
		return hiddenDialogsInclude, nil
	case "default", "inherit":
		return "", nil
	default:
		return "", fmt.Errorf("unknown setting %q", args[0])
	}
}

func inlineSettingsSummary(client *InlineClient) string {
	defaults, override, effective := client.hiddenDialogsSettings()
	overrideLabel := string(override)
	if overrideLabel == "" {
		overrideLabel = "default"
	}
	muteDefault, muteOverride, muteEffective := client.channelMuteSettings()
	return fmt.Sprintf(
		"Inline settings\n\n- Hidden chats: `%s`\n- Account override: `%s`\n- Bridge default: `%s`\n\nHidden chats are Inline dialogs with `showInChatList: false`. Excluding them prevents new Matrix portal creation, history fill, and inbound event projection. Existing Matrix rooms are not deleted.",
		effective,
		overrideLabel,
		defaults,
	) + fmt.Sprintf("\n\nDesired channel mute default: `%s` (bridge `%s`, account `%s`); automatic writes disabled. ", muteEffective, muteDefault, muteOverride) + notificationPolicyDegraded + ". Set Beeper Settings → Accounts → Inline → Show in All Inbox to DMs only; mute and Low Priority are separate user choices. Presentation version: 1. Manual unread projection remains pending until origin-safe transport is proven."
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
	return loadAndConnectInlineLogin(ctx, bridge, login)
}

func loadAndConnectInlineLogin(ctx context.Context, bridge *bridgev2.Bridge, login *bridgev2.UserLogin) error {
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
		fmt.Fprintf(&out, "- Notification policy: %s\n", notificationPolicyDegraded)
		out.WriteString(client.projectionStatus(context.Background()))
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

var commandInlineMigrate = &commands.FullHandler{
	Func: fnInlineMigrate, Name: "inline-migrate", RequiresLogin: true,
	Help: commands.HelpMeta{Section: commands.HelpSectionMisc, Description: "Preview or repair selected existing Inline rooms in place", Args: "[preview|apply|rollback] <chat-id...>"},
}

func fnInlineMigrate(ce *commands.Event) {
	_, client := inlineCommandLogin(ce)
	if client == nil {
		ce.Reply("Your default login is not an Inline login.")
		return
	}
	mode, ids, err := parseMigrationCommand(ce.Args)
	if err != nil {
		ce.Reply("Usage: `$cmdprefix inline-migrate [preview|apply|rollback] <chat-id...>`\n\n%s", err)
		return
	}
	ce.Reply(client.migrateSelected(ce.Ctx, mode, ids))
}
