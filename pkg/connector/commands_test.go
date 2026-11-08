package connector

import (
	"errors"
	"maunium.net/go/mautrix/bridgev2"
	"strings"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

func TestInlineStatusSummaryIncludesSidecarAndClientState(t *testing.T) {
	client := &InlineClient{
		AccountID: "42",
		loggedIn:  true,
	}
	got := inlineStatusSummary(nil, client, &sidecar.Status{
		Protocol: sidecar.ProtocolInfo{
			ProtocolVersion: 1,
			ClientVersion:   "0.4.0",
		},
		Status: sidecar.StatusConnected,
	}, nil)

	for _, want := range []string{
		"Inline status",
		"Account ID: `42`",
		"Go client logged in: `true`",
		"Sidecar: `Connected`",
		"Sidecar protocol: `1`, client: `0.4.0`",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary missing %q:\n%s", want, got)
		}
	}
}

func TestInlineStatusSummaryReportsSidecarFailure(t *testing.T) {
	got := inlineStatusSummary(nil, nil, nil, errors.New("sidecar down"))
	if !strings.Contains(got, "Sidecar: check failed: `sidecar down`") {
		t.Fatalf("summary = %q, want sidecar failure", got)
	}
}

func TestParseHiddenDialogsCommand(t *testing.T) {
	tests := []struct {
		input []string
		want  hiddenDialogsPolicy
	}{
		{input: []string{"exclude"}, want: hiddenDialogsExclude},
		{input: []string{"show"}, want: hiddenDialogsInclude},
		{input: []string{"default"}, want: ""},
	}
	for _, test := range tests {
		got, err := parseHiddenDialogsCommand(test.input)
		if err != nil || got != test.want {
			t.Fatalf("parseHiddenDialogsCommand(%q) = %q, %v; want %q", test.input, got, err, test.want)
		}
	}
	if _, err := parseHiddenDialogsCommand([]string{"sometimes"}); err == nil {
		t.Fatal("unknown hidden dialog setting was accepted")
	}
}

func TestInlineSettingsSummaryExplainsEffectivePolicy(t *testing.T) {
	client := &InlineClient{
		hiddenDialogsDefault:  hiddenDialogsExclude,
		hiddenDialogsOverride: hiddenDialogsInclude,
	}
	got := inlineSettingsSummary(client)
	for _, want := range []string{
		"Hidden chats: `include`",
		"Account override: `include`",
		"Bridge default: `exclude`",
		"Existing Matrix rooms are not deleted",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("settings summary missing %q:\n%s", want, got)
		}
	}
}

func TestMigrationCommandRequiresExplicitBoundedNumericSelection(t *testing.T) {
	for _, args := range [][]string{nil, {"apply"}, {"apply", "space:1"}, {"rollback", "7", "7"}, {"apply", "0"}, {"apply", "-1"}, {"anything", "7"}} {
		if _, _, err := parseMigrationCommand(args); err == nil {
			t.Errorf("unsafe selection accepted: %v", args)
		}
	}
	mode, ids, err := parseMigrationCommand([]string{"7", "8"})
	if err != nil || mode != "preview" || len(ids) != 2 || ids[0] != 7 || ids[1] != 8 {
		t.Fatalf("default preview=%s %v %v", mode, ids, err)
	}
}
func TestSettingsDiscloseDisabledAutomaticMute(t *testing.T) {
	got := inlineSettingsSummary(&InlineClient{})
	if !strings.Contains(got, "notification_policy_degraded") || !strings.Contains(got, "DMs only") {
		t.Fatal("settings hide automatic-mute prerequisite or Inbox setup")
	}
}

func TestChannelMutePreferenceIsDesiredOnlyAndAccountScoped(t *testing.T) {
	disabled := false
	login := &bridgev2.UserLogin{Bridge: &bridgev2.Bridge{Network: &InlineConnector{Config: Config{MuteChannelsByDefault: &disabled}}}}
	a := newInlineClient(login, &UserLoginMetadata{AccountID: "1", ChannelMuteDefault: "muted"}, "http://127.0.0.1:1")
	b := newInlineClient(login, &UserLoginMetadata{AccountID: "2"}, "http://127.0.0.1:1")
	defaults, override, effective := a.channelMuteSettings()
	if defaults != "normal" || override != "muted" || effective != "muted" {
		t.Fatal("account preference lost", defaults, override, effective)
	}
	_, override, effective = b.channelMuteSettings()
	if override != "default" || effective != "normal" {
		t.Fatal("preference leaked between logins")
	}
	if !strings.Contains(inlineSettingsSummary(a), "automatic writes disabled") {
		t.Fatal("desired mute preference represented as applied")
	}
	for _, value := range []string{"default", "muted", "normal"} {
		if _, err := parseChannelMuteCommand([]string{value}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := parseChannelMuteCommand([]string{"attested"}); err == nil {
		t.Fatal("unsupported origin attestation accepted")
	}
}
