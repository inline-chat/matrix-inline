package main

import (
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
	m := mxmain.BridgeMain{
		Name:        "matrix-inline",
		Description: "A Matrix-Inline bridge",
		URL:         "https://github.com/inline-chat/matrix-inline",
		Version:     "0.1.0",
		Connector:   &connector.InlineConnector{},
	}
	m.InitVersion(Tag, Commit, BuildTime)
	m.Run()
}
