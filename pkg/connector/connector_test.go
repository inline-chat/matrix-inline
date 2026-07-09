package connector

import (
	"testing"

	"maunium.net/go/mautrix/id"
)

func TestGetNameUsesProductionDefaults(t *testing.T) {
	name := (&InlineConnector{}).GetName()

	if name.DisplayName != defaultDisplayName {
		t.Fatalf("DisplayName = %q, want %q", name.DisplayName, defaultDisplayName)
	}
	if name.NetworkURL != defaultNetworkURL {
		t.Fatalf("NetworkURL = %q, want %q", name.NetworkURL, defaultNetworkURL)
	}
	if name.NetworkIcon != defaultNetworkIcon {
		t.Fatalf("NetworkIcon = %q, want %q", name.NetworkIcon, defaultNetworkIcon)
	}
	if name.NetworkID != defaultNetworkID {
		t.Fatalf("NetworkID = %q, want %q", name.NetworkID, defaultNetworkID)
	}
	if name.BeeperBridgeType != defaultBeeperBridgeType {
		t.Fatalf("BeeperBridgeType = %q, want %q", name.BeeperBridgeType, defaultBeeperBridgeType)
	}
	if name.DefaultCommandPrefix != defaultCommandPrefix {
		t.Fatalf("DefaultCommandPrefix = %q, want %q", name.DefaultCommandPrefix, defaultCommandPrefix)
	}
}

func TestGetNameUsesConfiguredBridgeProfile(t *testing.T) {
	icon := id.ContentURIString("mxc://example.org/inline")
	connector := &InlineConnector{Config: Config{
		DisplayName: "Inline Beta",
		NetworkURL:  "https://inline.chat/docs",
		NetworkIcon: string(icon),
	}}

	name := connector.GetName()

	if name.DisplayName != "Inline Beta" {
		t.Fatalf("DisplayName = %q, want Inline Beta", name.DisplayName)
	}
	if name.NetworkURL != "https://inline.chat/docs" {
		t.Fatalf("NetworkURL = %q, want https://inline.chat/docs", name.NetworkURL)
	}
	if name.NetworkIcon != icon {
		t.Fatalf("NetworkIcon = %q, want %q", name.NetworkIcon, icon)
	}
}

func TestValidateConfigRejectsInvalidNetworkIcon(t *testing.T) {
	connector := &InlineConnector{Config: Config{NetworkIcon: "https://inline.chat/icon.svg"}}

	if err := connector.validateConfig(); err == nil {
		t.Fatal("validateConfig() = nil, want invalid network_icon error")
	}
}

func TestValidateConfigAcceptsMXCNetworkIcon(t *testing.T) {
	connector := &InlineConnector{Config: Config{NetworkIcon: "mxc://example.org/inline"}}

	if err := connector.validateConfig(); err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
}

func TestSidecarURLPrecedence(t *testing.T) {
	t.Setenv("INLINE_SIDECAR_URL", "http://127.0.0.1:29352")
	connector := &InlineConnector{Config: Config{SidecarURL: "http://127.0.0.1:29342"}}

	if got := connector.sidecarURL("http://127.0.0.1:29399"); got != "http://127.0.0.1:29399" {
		t.Fatalf("sidecarURL override = %q", got)
	}
	if got := connector.sidecarURL(""); got != "http://127.0.0.1:29352" {
		t.Fatalf("sidecarURL env = %q", got)
	}

	t.Setenv("INLINE_SIDECAR_URL", "")
	if got := connector.sidecarURL(""); got != "http://127.0.0.1:29342" {
		t.Fatalf("sidecarURL config = %q", got)
	}
}
