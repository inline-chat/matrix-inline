package connector

import (
	"encoding/json"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
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

func TestStartRequiresSynchronousPortalDelivery(t *testing.T) {
	previous := bridgev2.PortalEventBuffer
	bridgev2.PortalEventBuffer = 64
	t.Cleanup(func() { bridgev2.PortalEventBuffer = previous })

	err := validatePortalEventDelivery()
	if err == nil || !strings.Contains(err.Error(), "MAUTRIX_PORTAL_EVENT_BUFFER must be 0") {
		t.Fatalf("Start() error = %v, want synchronous portal delivery error", err)
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

func TestValidateSidecarURLRequiresLoopbackHTTP(t *testing.T) {
	for _, allowed := range []string{
		"http://127.0.0.1:29342",
		"http://[::1]:29342",
		"http://localhost:29342/",
	} {
		if err := validateSidecarURL(allowed); err != nil {
			t.Fatalf("validateSidecarURL(%q) error = %v", allowed, err)
		}
	}
	for _, rejected := range []string{
		"http://0.0.0.0:29342",
		"http://192.0.2.1:29342",
		"https://127.0.0.1:29342",
		"http://user:pass@127.0.0.1:29342",
		"http://127.0.0.1:29342/rpc",
	} {
		if err := validateSidecarURL(rejected); err == nil {
			t.Fatalf("validateSidecarURL(%q) = nil, want rejection", rejected)
		}
	}
}

func TestNewInlineClientBackfillsLegacyStoreNamespace(t *testing.T) {
	meta := &UserLoginMetadata{AccountID: "42"}

	client := newInlineClient(nil, meta, "http://127.0.0.1:29342")

	if client.storeNamespace != "42" {
		t.Fatalf("storeNamespace = %q, want account ID fallback", client.storeNamespace)
	}
	if meta.StoreNamespace != "42" {
		t.Fatalf("metadata store namespace = %q, want account ID fallback", meta.StoreNamespace)
	}
	if !client.needsBridgeStateRecovery() {
		t.Fatal("legacy login should require one-time bridge state recovery")
	}
}

func TestLegacyLoginMetadataDefaultsToBridgeStateRecovery(t *testing.T) {
	var meta UserLoginMetadata
	if err := json.Unmarshal([]byte(`{"account_id":"42"}`), &meta); err != nil {
		t.Fatalf("decode legacy metadata: %v", err)
	}
	client := newInlineClient(nil, &meta, "http://127.0.0.1:29342")
	if !client.needsBridgeStateRecovery() {
		t.Fatal("metadata without a bridge state version should require recovery")
	}

	meta.BridgeStateVersion = currentBridgeStateVersion
	client = newInlineClient(nil, &meta, "http://127.0.0.1:29342")
	if client.needsBridgeStateRecovery() {
		t.Fatal("current bridge state version should not repeat recovery")
	}
}
