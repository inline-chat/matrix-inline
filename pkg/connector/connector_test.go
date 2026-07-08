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
	if name.NetworkIcon != "" {
		t.Fatalf("NetworkIcon = %q, want empty default", name.NetworkIcon)
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
