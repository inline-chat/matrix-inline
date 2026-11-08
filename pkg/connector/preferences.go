package connector

import (
	"context"
	"errors"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
)

// This is desired policy only. No automatic local preference write is enabled
// until the target can prove bridge/user origin and preserve later choices.
func channelMuteDefaultForLogin(login *bridgev2.UserLogin) string {
	if login != nil && login.Bridge != nil {
		if connector, ok := login.Bridge.Network.(*InlineConnector); ok && connector.Config.MuteChannelsByDefault != nil && !*connector.Config.MuteChannelsByDefault {
			return "normal"
		}
	}
	return "muted"
}
func (ic *InlineClient) channelMuteSettings() (defaults, override, effective string) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	defaults = ic.channelMuteDefault
	if defaults == "" {
		defaults = "muted"
	}
	override = ic.channelMuteOverride
	effective = override
	if override == "" {
		override = "default"
		effective = defaults
	}
	return
}
func parseChannelMuteCommand(args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("choose muted, normal or default")
	}
	switch args[0] {
	case "default":
		return "", nil
	case "muted", "normal":
		return args[0], nil
	default:
		return "", errors.New("choose muted, normal or default")
	}
}
func (ic *InlineClient) persistChannelMuteOverride(ctx context.Context, policy string) error {
	if policy != "" && policy != "muted" && policy != "normal" {
		return errors.New("invalid channel default")
	}
	if ic.UserLogin == nil {
		return errors.New("Inline login unavailable")
	}
	ic.metadataSaveMu.Lock()
	defer ic.metadataSaveMu.Unlock()
	meta, ok := ic.UserLogin.Metadata.(*UserLoginMetadata)
	if !ok || meta == nil {
		return errors.New("Inline login metadata unavailable")
	}
	before := meta.ChannelMuteDefault
	meta.ChannelMuteDefault = policy
	if err := ic.UserLogin.Save(ctx); err != nil {
		meta.ChannelMuteDefault = before
		return fmt.Errorf("save desired channel default: %w", err)
	}
	ic.mu.Lock()
	ic.channelMuteOverride = policy
	ic.mu.Unlock()
	return nil
}

var commandInlineChannelDefault = &commands.FullHandler{
	Name: "inline-channel-default", RequiresLogin: true,
	Help: commands.HelpMeta{Section: commands.HelpSectionMisc, Description: "Set desired channel mute policy (automatic writes disabled)", Args: "<muted|normal|default>"},
	Func: func(ce *commands.Event) {
		_, client := inlineCommandLogin(ce)
		if client == nil {
			ce.Reply("Your default login is not an Inline login.")
			return
		}
		policy, err := parseChannelMuteCommand(ce.Args)
		if err == nil {
			err = client.persistChannelMuteOverride(ce.Ctx, policy)
		}
		if err != nil {
			ce.Reply("Channel default: %s", err)
			return
		}
		ce.Reply("%s", inlineSettingsSummary(client))
	},
}

func channelMutePolicyFromMetadata(value string) string {
	if value == "muted" || value == "normal" {
		return value
	}
	return ""
}
