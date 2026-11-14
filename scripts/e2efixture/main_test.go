package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

func TestFixtureRestartChangesGenerationAndReplaysNewSequence(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "fixture.json")
	old := newFixtureServer(statePath)
	current := newFixtureServer(statePath)
	if old.generation == "" || old.generation == current.generation {
		t.Fatal("fixture restart reused an event generation")
	}
	mux := http.NewServeMux()
	current.register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	client := sidecar.NewClient(server.URL)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	health, err := client.Health(ctx)
	if err != nil || health == nil || health.EventGeneration != current.generation {
		t.Fatalf("health generation: %+v %v", health, err)
	}
	if err := current.broadcast(map[string]any{"ChatUpserted": map[string]any{"chat_id": dmChatID}}); err != nil {
		t.Fatal(err)
	}
	if stream, err := client.EventsAfterGeneration(ctx, "16000", old.generation, 99); !errors.Is(err, sidecar.ErrEventReplayUnavailable) {
		if stream != nil {
			stream.Close(websocket.StatusNormalClosure, "done")
		}
		t.Fatalf("old cursor was silently accepted after restart: %v", err)
	}
	stream, err := client.EventsAfterGeneration(ctx, "16000", current.generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close(websocket.StatusNormalClosure, "done")
	event, err := stream.Recv(ctx)
	if err != nil || event == nil || event.Generation != current.generation || event.Sequence == nil || *event.Sequence != 1 {
		t.Fatalf("new process sequence did not replay: %+v %v", event, err)
	}
	if err := client.AckEventsGeneration(ctx, "16000", old.generation, 1); err == nil {
		t.Fatal("old generation acknowledgement accepted")
	}
	if err := client.AckEventsGeneration(ctx, "16000", current.generation, 1); err != nil {
		t.Fatal(err)
	}
}
