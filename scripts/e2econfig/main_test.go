package main

import "testing"

func TestPatchBridgeConfigSetsLocalE2EFields(t *testing.T) {
	doc := map[string]any{
		"network": map[string]any{},
		"database": map[string]any{
			"type": "postgres",
			"uri":  "postgres://example",
		},
		"homeserver": map[string]any{},
		"appservice": map[string]any{
			"bot": map[string]any{},
		},
		"bridge": map[string]any{},
	}

	patchBridgeConfig(doc, bridgePatch{
		HomeserverAddress: "http://127.0.0.1:18008",
		HomeserverDomain:  "localhost",
		AppserviceAddress: "http://host.docker.internal:29343",
		AppserviceHost:    "127.0.0.1",
		AppservicePort:    29343,
		SidecarURL:        "http://127.0.0.1:29342",
		DatabaseURI:       "file:/tmp/matrix-inline-e2e.db?_txlock=immediate",
		AdminLocalpart:    "alice",
	})

	assertPath(t, doc, "network.sidecar_url", "http://127.0.0.1:29342")
	assertPath(t, doc, "database.type", "sqlite3-fk-wal")
	assertPath(t, doc, "database.uri", "file:/tmp/matrix-inline-e2e.db?_txlock=immediate")
	assertPath(t, doc, "database.max_open_conns", "1")
	assertPath(t, doc, "homeserver.address", "http://127.0.0.1:18008")
	assertPath(t, doc, "homeserver.domain", "localhost")
	assertPath(t, doc, "appservice.address", "http://host.docker.internal:29343")
	assertPath(t, doc, "appservice.hostname", "127.0.0.1")
	assertPath(t, doc, "appservice.port", "29343")
	assertPath(t, doc, "appservice.bot.username", "inlinebot")
	assertPath(t, doc, "appservice.bot.avatar", "")
	assertPath(t, doc, "bridge.permissions.localhost", "user")
	assertPath(t, doc, "bridge.permissions.@alice:localhost", "admin")
}

func TestPatchSynapseConfigSetsRegistrationAndLocalRegistration(t *testing.T) {
	doc := map[string]any{}

	patchSynapseConfig(doc, synapsePatch{
		RegistrationPath: "/data/matrix-inline-registration.yaml",
		PublicBaseURL:    "http://127.0.0.1:18008/",
		SharedSecret:     "local-secret",
	})

	assertPath(t, doc, "public_baseurl", "http://127.0.0.1:18008/")
	assertPath(t, doc, "enable_registration", "true")
	assertPath(t, doc, "enable_registration_without_verification", "true")
	assertPath(t, doc, "registration_shared_secret", "local-secret")
	assertPath(t, doc, "report_stats", "false")
	assertPath(t, doc, "suppress_key_server_warning", "true")

	files, ok := doc["app_service_config_files"].([]any)
	if !ok || len(files) != 1 || files[0] != "/data/matrix-inline-registration.yaml" {
		t.Fatalf("app_service_config_files = %#v", doc["app_service_config_files"])
	}
}

func assertPath(t *testing.T, doc map[string]any, path, want string) {
	t.Helper()
	got, ok := getPath(doc, splitPath(path))
	if !ok {
		t.Fatalf("path %s not found", path)
	}
	if got != want {
		t.Fatalf("%s = %q, want %q", path, got, want)
	}
}

func splitPath(path string) []string {
	parts := make([]string, 0, 1)
	start := 0
	for i, char := range path {
		if char == '.' {
			parts = append(parts, path[start:i])
			start = i + 1
		}
	}
	return append(parts, path[start:])
}
