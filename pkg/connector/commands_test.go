package connector

import (
	"errors"
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
