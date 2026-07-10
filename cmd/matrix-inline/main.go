package main

import (
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"

	"github.com/inline-chat/matrix-inline/pkg/connector"
)

// Build metadata filled by release builds with -X linker flags.
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

func main() {
	// Lossless Inline events are acknowledged only after mautrix has finished
	// handling them. A buffered portal queue reports success when an event is
	// merely resident in memory, which creates a crash-loss window between that
	// report and Matrix projection. Operators may still set the corresponding
	// mautrix environment variable, but the connector will reject non-zero
	// values at startup to preserve this delivery invariant.
	bridgev2.PortalEventBuffer = 0

	m := mxmain.BridgeMain{
		Name:        "matrix-inline",
		Description: "A Matrix-Inline bridge",
		URL:         "https://github.com/inline-chat/matrix-inline",
		Version:     "0.2.0",
		Connector:   &connector.InlineConnector{},
	}
	m.InitVersion(Tag, Commit, BuildTime)
	m.Run()
}
