package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: e2econfig <patch-bridge|patch-synapse|get> [flags]")
	}
	switch args[0] {
	case "patch-bridge":
		return runPatchBridge(args[1:])
	case "patch-synapse":
		return runPatchSynapse(args[1:])
	case "get":
		return runGet(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runPatchBridge(args []string) error {
	fs := flag.NewFlagSet("patch-bridge", flag.ContinueOnError)
	configPath := fs.String("config", "", "bridge config path")
	homeserverAddress := fs.String("homeserver-address", "", "homeserver URL reachable from the bridge")
	homeserverDomain := fs.String("homeserver-domain", "", "Matrix server_name")
	appserviceAddress := fs.String("appservice-address", "", "appservice URL reachable from the homeserver")
	appserviceHostname := fs.String("appservice-hostname", "127.0.0.1", "appservice bind hostname")
	appservicePort := fs.Int("appservice-port", 29343, "appservice bind port")
	sidecarURL := fs.String("sidecar-url", "http://127.0.0.1:29342", "Inline sidecar URL")
	databaseURI := fs.String("database-uri", "", "bridge sqlite database URI")
	adminLocalpart := fs.String("admin-localpart", "alice", "local Matrix admin user")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" || *homeserverAddress == "" || *homeserverDomain == "" || *appserviceAddress == "" || *databaseURI == "" {
		return errors.New("patch-bridge requires --config, --homeserver-address, --homeserver-domain, --appservice-address, and --database-uri")
	}

	doc, err := readYAMLMap(*configPath)
	if err != nil {
		return err
	}
	patchBridgeConfig(doc, bridgePatch{
		HomeserverAddress: *homeserverAddress,
		HomeserverDomain:  *homeserverDomain,
		AppserviceAddress: *appserviceAddress,
		AppserviceHost:    *appserviceHostname,
		AppservicePort:    *appservicePort,
		SidecarURL:        *sidecarURL,
		DatabaseURI:       *databaseURI,
		AdminLocalpart:    *adminLocalpart,
	})
	return writeYAMLMap(*configPath, doc)
}

func runPatchSynapse(args []string) error {
	fs := flag.NewFlagSet("patch-synapse", flag.ContinueOnError)
	configPath := fs.String("config", "", "Synapse homeserver.yaml path")
	registrationPath := fs.String("registration-path", "/data/matrix-inline-registration.yaml", "appservice registration path inside the Synapse container")
	publicBaseURL := fs.String("public-base-url", "", "public base URL for local Matrix clients")
	registrationSecret := fs.String("registration-shared-secret", "", "local registration shared secret")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" || *publicBaseURL == "" || *registrationSecret == "" {
		return errors.New("patch-synapse requires --config, --public-base-url, and --registration-shared-secret")
	}

	doc, err := readYAMLMap(*configPath)
	if err != nil {
		return err
	}
	patchSynapseConfig(doc, synapsePatch{
		RegistrationPath: *registrationPath,
		PublicBaseURL:    *publicBaseURL,
		SharedSecret:     *registrationSecret,
	})
	return writeYAMLMap(*configPath, doc)
}

func runGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	configPath := fs.String("config", "", "YAML path")
	path := fs.String("path", "", "dot-separated path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" || *path == "" {
		return errors.New("get requires --config and --path")
	}
	doc, err := readYAMLMap(*configPath)
	if err != nil {
		return err
	}
	value, ok := getPath(doc, strings.Split(*path, "."))
	if !ok {
		return fmt.Errorf("path %q not found", *path)
	}
	fmt.Println(value)
	return nil
}

type bridgePatch struct {
	HomeserverAddress string
	HomeserverDomain  string
	AppserviceAddress string
	AppserviceHost    string
	AppservicePort    int
	SidecarURL        string
	DatabaseURI       string
	AdminLocalpart    string
}

func patchBridgeConfig(doc map[string]any, patch bridgePatch) {
	setPath(doc, "network.displayname", "Inline")
	setPath(doc, "network.network_url", "https://inline.chat")
	setPath(doc, "network.sidecar_url", patch.SidecarURL)

	setPath(doc, "database.type", "sqlite3-fk-wal")
	setPath(doc, "database.uri", patch.DatabaseURI)
	setPath(doc, "database.max_open_conns", 1)
	setPath(doc, "database.max_idle_conns", 1)

	setPath(doc, "homeserver.address", patch.HomeserverAddress)
	setPath(doc, "homeserver.domain", patch.HomeserverDomain)
	setPath(doc, "homeserver.software", "standard")

	setPath(doc, "appservice.address", patch.AppserviceAddress)
	setPath(doc, "appservice.public_address", "")
	setPath(doc, "appservice.hostname", patch.AppserviceHost)
	setPath(doc, "appservice.port", patch.AppservicePort)
	setPath(doc, "appservice.bot.username", "inlinebot")
	setPath(doc, "appservice.bot.displayname", "Inline bridge bot")
	setPath(doc, "appservice.bot.avatar", "")
	setPath(doc, "appservice.ephemeral_events", true)
	setPath(doc, "appservice.async_transactions", false)

	adminMXID := fmt.Sprintf("@%s:%s", patch.AdminLocalpart, patch.HomeserverDomain)
	setPath(doc, "bridge.permissions", map[string]any{
		"*":                    "relay",
		patch.HomeserverDomain: "user",
		adminMXID:              "admin",
	})
}

type synapsePatch struct {
	RegistrationPath string
	PublicBaseURL    string
	SharedSecret     string
}

func patchSynapseConfig(doc map[string]any, patch synapsePatch) {
	setPath(doc, "public_baseurl", patch.PublicBaseURL)
	setPath(doc, "enable_registration", true)
	setPath(doc, "enable_registration_without_verification", true)
	setPath(doc, "registration_shared_secret", patch.SharedSecret)
	setPath(doc, "app_service_config_files", []any{patch.RegistrationPath})
	setPath(doc, "report_stats", false)
	setPath(doc, "suppress_key_server_warning", true)
}

func readYAMLMap(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	doc := make(map[string]any)
	if err = yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return doc, nil
}

func writeYAMLMap(path string, doc map[string]any) error {
	data, err := yaml.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	if err = os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func setPath(doc map[string]any, path string, value any) {
	parts := strings.Split(path, ".")
	current := doc
	for _, part := range parts[:len(parts)-1] {
		next, _ := current[part].(map[string]any)
		if next == nil {
			next = make(map[string]any)
			current[part] = next
		}
		current = next
	}
	current[parts[len(parts)-1]] = value
}

func getPath(doc map[string]any, parts []string) (string, bool) {
	var current any = doc
	for _, part := range parts {
		mapping, ok := current.(map[string]any)
		if !ok {
			return "", false
		}
		current, ok = mapping[part]
		if !ok {
			return "", false
		}
	}
	switch value := current.(type) {
	case string:
		return value, true
	case int:
		return strconv.Itoa(value), true
	case bool:
		return strconv.FormatBool(value), true
	default:
		return fmt.Sprint(value), true
	}
}
