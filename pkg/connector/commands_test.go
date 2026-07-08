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
