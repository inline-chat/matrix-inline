package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"hegel.dev/go/hegel"
)

// Isolate the former stack-overflow regression: a bad preflight must fail this
// test rather than take down the entire package's test process.
func TestClientSubpathPreflightRegression(t *testing.T) {
	if os.Getenv("INLINE_PROTOCOL_SUBPATH_FIXTURE") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, binary, "-test.run=^TestClientSubpathPreflightRegression$")
		cmd.Env = append(os.Environ(), "INLINE_PROTOCOL_SUBPATH_FIXTURE=1")
		output, err := cmd.CombinedOutput()
		if err != nil {
			if len(output) > 3000 {
				output = output[:3000]
			}
			t.Fatalf("subpath fixture failed: %v\n%s", err, output)
		}
		return
	}
	debug.SetMaxStack(1 << 20)
	var probes, operations atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(ProtocolVersionHeader) != "5" {
			t.Error("fixture request omitted the version header")
		}
		if strings.HasSuffix(r.URL.Path, "/compatibility") {
			if r.Header.Get("X-Inline-Session-Namespace") != "" {
				t.Error("compatibility selected an account")
			}
			probes.Add(1)
			writeJSON(t, w, ProtocolInfo{ProtocolVersion: ProtocolVersion})
			return
		}
		if r.Header.Get("X-Inline-Session-Namespace") != "fixture" {
			t.Error("account operation lost its namespace")
		}
		operations.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, "/ws/events"):
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.CloseNow()
			_ = conn.Close(websocket.StatusNormalClosure, "fixture complete")
		case strings.HasSuffix(r.URL.Path, "/rpc/upload"):
			writeRPCResult(t, w, "message", MessageMutation{})
		default:
			writeRPCResult(t, w, "empty", struct{}{})
		}
	}))
	defer server.Close()
	hegel.Test(t, func(ht *hegel.T) {
		// Vary the endpoint configuration over the full numeric segment domain;
		// the path deliberately has more than one segment.
		segment := hegel.Draw(ht, hegel.Integers(uint64(0), uint64(math.MaxUint64)))
		client := NewClient(server.URL + fmt.Sprintf("/fixture-%d/nested", segment)).WithSessionNamespace("fixture")
		beforeProbes, beforeOperations := probes.Load(), operations.Load()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := client.Compatibility(ctx); err != nil {
			ht.Fatal(err)
		}
		if err := client.Read(ctx, ReadRequest{}); err != nil {
			ht.Fatal(err)
		}
		if _, err := client.Upload(ctx, UploadRequest{}, []byte("fixture")); err != nil {
			ht.Fatal(err)
		}
		stream, err := client.Events(ctx)
		if err != nil {
			ht.Fatal(err)
		}
		_ = stream.Close(websocket.StatusNormalClosure, "fixture complete")
		if probes.Load()-beforeProbes != 4 || operations.Load()-beforeOperations != 3 {
			ht.Fatalf("probe/operation counts = %d/%d, want 4/3", probes.Load()-beforeProbes, operations.Load()-beforeOperations)
		}
	}, hegel.WithTestCases(1000), hegel.WithDatabase(t.TempDir()))
}

type protocolFixtureTransport func(*http.Request) (*http.Response, error)

func (fn protocolFixtureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

// Run exported acknowledgement methods against controlled HTTP responses,
// including exact uint64 values that must never pass through float64.
func ackFixtureClient(payload string, status int, calls *atomic.Int64) *Client {
	client := NewClient("http://fixture.invalid").WithSessionNamespace("fixture")
	client.HTTP = &http.Client{Transport: protocolFixtureTransport(func(req *http.Request) (*http.Response, error) {
		body, code := payload, status
		if req.URL.Path == "/compatibility" {
			body, code = `{"protocol_version":5}`, http.StatusOK
		} else {
			calls.Add(1)
		}
		return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})}
	return client
}

func TestAckGenerationResetMetadataProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		latest := hegel.Draw(ht, hegel.Integers(uint64(0), uint64(math.MaxUint64)))
		generation := fmt.Sprintf("generation-%d", latest)
		payload := fmt.Sprintf(`{"error":"sidecar_event_generation_reset","latest_sequence":%d,"event_generation":%q,"reset_required":true}`, latest, generation)
		var calls atomic.Int64
		client := ackFixtureClient(payload, http.StatusGone, &calls)
		err := client.AckEventsGeneration(context.Background(), "fixture", "old-generation", latest)
		var reset *EventReplayUnavailableError
		if !errors.Is(err, ErrEventReplayUnavailable) || !errors.As(err, &reset) {
			ht.Fatalf("ack reset error = %v, want typed replay-unavailable", err)
		}
		if !reset.ResetRequired || reset.EventGeneration != generation || reset.LatestSequence == nil || *reset.LatestSequence != latest {
			ht.Fatalf("reset metadata changed: %#v", reset)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestAckConfirmationProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		sequence := hegel.Draw(ht, hegel.Integers(uint64(0), uint64(math.MaxUint64)))
		generation := fmt.Sprintf("generation-%d", hegel.Draw(ht, hegel.Integers(uint64(0), uint64(math.MaxUint64))))
		variant := hegel.Draw(ht, hegel.Integers(0, 8))
		payload := fmt.Sprintf(`{"generation":%q,"acknowledged_sequence":%d}`, generation, sequence)
		switch variant {
		case 1:
			payload = `{}`
		case 2:
			payload = `null`
		case 3:
			payload = fmt.Sprintf(`{"acknowledged_sequence":%d}`, sequence)
		case 4:
			payload = fmt.Sprintf(`{"generation":null,"acknowledged_sequence":%d}`, sequence)
		case 5:
			payload = fmt.Sprintf(`{"generation":%q}`, generation)
		case 6:
			payload = fmt.Sprintf(`{"generation":%q,"acknowledged_sequence":null}`, generation)
		case 7:
			payload = fmt.Sprintf(`{"generation":%q,"acknowledged_sequence":%d}`, generation+"-other", sequence)
		case 8:
			payload = fmt.Sprintf(`{"generation":%q,"acknowledged_sequence":%d}`, generation, sequence^1)
		}
		var calls atomic.Int64
		err := ackFixtureClient(payload, http.StatusOK, &calls).AckEventsGeneration(context.Background(), "fixture", generation, sequence)
		if (err == nil) != (variant == 0) {
			ht.Fatalf("confirmation %s: error=%v, valid=%v", payload, err, variant == 0)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestAckMissingConfirmationDoesNotMeanZeroRegression(t *testing.T) {
	for _, payload := range []string{`{}`, `null`, `{"generation":"G"}`, `{"generation":"G","acknowledged_sequence":null}`} {
		var calls atomic.Int64
		if err := ackFixtureClient(payload, http.StatusOK, &calls).AckEventsGeneration(context.Background(), "fixture", "G", 0); err == nil {
			t.Fatalf("missing zero confirmation accepted: %s", payload)
		}
	}
	var calls atomic.Int64
	if err := ackFixtureClient(`{"generation":"G","acknowledged_sequence":0}`, http.StatusOK, &calls).AckEventsGeneration(context.Background(), "fixture", "G", 0); err != nil {
		t.Fatalf("explicit zero confirmation rejected: %v", err)
	}
}

func TestAckRequiresGenerationBeforeTransport(t *testing.T) {
	for _, generation := range []string{"", " ", "\t\n"} {
		var calls atomic.Int64
		client := ackFixtureClient(`{}`, http.StatusOK, &calls)
		if err := client.AckEventsGeneration(context.Background(), "fixture", generation, 1); err == nil {
			t.Fatalf("missing generation %q accepted", generation)
		}
		if calls.Load() != 0 {
			t.Fatal("missing generation reached account transport")
		}
	}
	var calls atomic.Int64
	if err := ackFixtureClient(`{}`, http.StatusOK, &calls).AckEvents(context.Background(), "fixture", 1); err == nil || calls.Load() != 0 {
		t.Fatalf("legacy ack without lineage must fail before transport: err=%v calls=%d", err, calls.Load())
	}
}

func TestAckHTTPFailureIsNotAlwaysAnEventReset(t *testing.T) {
	for _, fixture := range []struct {
		status int
		body   string
	}{
		{http.StatusGone, `{"error":"unrelated_resource_gone"}`},
		{http.StatusGone, `not json`},
		{http.StatusConflict, `{"error":"ProtocolMismatch"}`},
		{http.StatusServiceUnavailable, `{"error":"unavailable"}`},
	} {
		var calls atomic.Int64
		err := ackFixtureClient(fixture.body, fixture.status, &calls).AckEventsGeneration(context.Background(), "fixture", "G", 1)
		if err == nil || errors.Is(err, ErrEventReplayUnavailable) {
			t.Fatalf("HTTP %d %s incorrectly mapped: %v", fixture.status, fixture.body, err)
		}
	}
}

func TestCompatibilityRejectsBeforeDirectMutation(t *testing.T) {
	for _, version := range []int{0, 4, 6} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			var effects atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/compatibility" {
					if r.Header.Get("X-Inline-Session-Namespace") != "" {
						t.Error("compatibility probe must not select an account")
					}
					_ = json.NewEncoder(w).Encode(ProtocolInfo{ProtocolVersion: version})
					return
				}
				effects.Add(1)
				writeRPCResult(t, w, "empty", struct{}{})
			}))
			defer server.Close()
			err := NewClient(server.URL).WithSessionNamespace("fixture").Read(context.Background(), ReadRequest{})
			if err == nil {
				t.Error("incompatible adapter accepted mutation")
			}
			if got := effects.Load(); got != 0 {
				t.Errorf("incompatible adapter executed %d mutations", got)
			}
		})
	}
}

func TestEveryMutationCarriesVersionHeader(t *testing.T) {
	var probes, effects atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/compatibility" {
			probes.Add(1)
			_ = json.NewEncoder(w).Encode(ProtocolInfo{ProtocolVersion: ProtocolVersion})
			return
		}
		if got := r.Header.Get("X-Inline-Protocol-Version"); got != "5" {
			t.Errorf("request version header = %q, want 5", got)
			http.Error(w, "protocol mismatch", http.StatusConflict)
			return
		}
		effects.Add(1)
		writeRPCResult(t, w, "empty", struct{}{})
	}))
	defer server.Close()
	if err := NewClient(server.URL).Read(context.Background(), ReadRequest{}); err != nil {
		t.Fatal(err)
	}
	if probes.Load() != 1 || effects.Load() != 1 {
		t.Fatalf("probes=%d effects=%d", probes.Load(), effects.Load())
	}
}

func TestCompleteStructureSurvivesJSONWithExplicitClears(t *testing.T) {
	raw := []byte(`{"chat_id":7,"structure":{"kind":"channel","peer_user_id":null,"space_id":null,"parent_chat_id":null,"parent_message_id":null}}`)
	var dialog DialogRecord
	if err := json.Unmarshal(raw, &dialog); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(dialog)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	if string(result["structure"]) != `{"kind":"channel","peer_user_id":null,"space_id":null,"parent_chat_id":null,"parent_message_id":null}` {
		t.Fatalf("complete structure lost explicit clear semantics: %s", encoded)
	}
}

func TestStructuralAuthorityRoundTripAcrossIDDomain(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		remoteID := hegel.Draw(ht, hegel.Integers(int64(math.MinInt64), int64(math.MaxInt64)))
		optionalID := func() *int64 {
			if !hegel.Draw(ht, hegel.Booleans()) {
				return nil
			}
			value := hegel.Draw(ht, hegel.Integers(int64(math.MinInt64), int64(math.MaxInt64)))
			return &value
		}
		original := DialogRecord{ChatID: remoteID, Structure: &DialogStructure{
			Kind:       hegel.Draw(ht, hegel.SampledFrom([]string{"direct", "channel", "reply_child"})),
			PeerUserID: optionalID(), SpaceID: optionalID(), ParentChatID: optionalID(), ParentMessageID: optionalID(),
		}}
		raw, err := json.Marshal(original)
		if err != nil {
			ht.Fatal(err)
		}
		var decoded DialogRecord
		if err := json.Unmarshal(raw, &decoded); err != nil {
			ht.Fatal(err)
		}
		if !reflect.DeepEqual(decoded, original) {
			ht.Fatalf("structural authority changed across JSON: %s", raw)
		}

	})
}

func TestPartialStructureCannotMasqueradeAsCompleteDetach(t *testing.T) {
	for _, field := range []string{"kind", "peer_user_id", "space_id", "parent_chat_id", "parent_message_id"} {
		t.Run(field, func(t *testing.T) {
			descriptor := map[string]any{"kind": "channel", "peer_user_id": nil, "space_id": nil, "parent_chat_id": nil, "parent_message_id": nil}
			delete(descriptor, field)
			raw, _ := json.Marshal(map[string]any{"chat_id": 7, "structure": descriptor})
			var dialog DialogRecord
			if err := json.Unmarshal(raw, &dialog); err == nil {
				t.Fatalf("partial descriptor accepted as authoritative: %s", raw)
			}
		})
	}
}

func TestLegacyDescriptorAbsenceStaysUnknown(t *testing.T) {
	var unknown DialogRecord
	if err := json.Unmarshal([]byte(`{"chat_id":1}`), &unknown); err != nil {
		t.Fatal(err)
	}
	if unknown.Structure != nil {
		t.Fatal("legacy absence invented an authoritative descriptor")
	}
}
