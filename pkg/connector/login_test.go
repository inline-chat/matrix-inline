package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

func TestInlineCodeLoginStartsEmailCodeFlow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/auth/start" {
			t.Fatalf("path = %s, want /rpc/auth/start", r.URL.Path)
		}
		var request sidecar.AuthStartRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode auth start request: %v", err)
		}
		if request.Contact != "mo@example.com" || request.Kind != sidecar.AuthContactEmail {
			t.Fatalf("auth start request = %#v, want email contact", request)
		}
		writeLoginRPCResult(t, w, "auth_start", sidecar.AuthStartResult{
			ExistingUser:   true,
			ChallengeToken: "challenge-token",
		})
	}))
	defer server.Close()

	login := &InlineCodeLogin{
		SidecarURL: server.URL,
		Kind:       sidecar.AuthContactEmail,
	}

	start, err := login.Start(context.Background())
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if start.StepID != "chat.inline.matrix.enter_contact" || len(start.UserInputParams.Fields) != 1 {
		t.Fatalf("start step = %#v, want contact input", start)
	}
	if start.UserInputParams.Fields[0].Type != bridgev2.LoginInputFieldTypeEmail {
		t.Fatalf("field type = %q, want email", start.UserInputParams.Fields[0].Type)
	}

	next, err := login.SubmitUserInput(context.Background(), map[string]string{
		"email": " mo@example.com ",
	})
	if err != nil {
		t.Fatalf("SubmitUserInput() error = %v", err)
	}
	if next.StepID != "chat.inline.matrix.enter_code" || len(next.UserInputParams.Fields) != 1 {
		t.Fatalf("next step = %#v, want code input", next)
	}
	if next.UserInputParams.Fields[0].Type != bridgev2.LoginInputFieldType2FACode {
		t.Fatalf("field type = %q, want 2fa code", next.UserInputParams.Fields[0].Type)
	}
	if login.contact != "mo@example.com" || login.challengeToken != "challenge-token" {
		t.Fatalf("login state contact=%q challenge=%q", login.contact, login.challengeToken)
	}
}

func TestInlineCodeLoginDefersInviteSignup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeLoginRPCResult(t, w, "auth_start", sidecar.AuthStartResult{
			NeedsInviteCode: true,
		})
	}))
	defer server.Close()

	login := &InlineCodeLogin{
		SidecarURL: server.URL,
		Kind:       sidecar.AuthContactPhone,
	}
	next, err := login.SubmitUserInput(context.Background(), map[string]string{
		"phone_number": "+15551234567",
	})
	if err != nil {
		t.Fatalf("SubmitUserInput() error = %v", err)
	}
	if next.StepID != "chat.inline.matrix.enter_contact" {
		t.Fatalf("step = %q, want contact retry", next.StepID)
	}
	if login.contact != "" || login.challengeToken != "" {
		t.Fatalf("login state contact=%q challenge=%q, want empty", login.contact, login.challengeToken)
	}
}

func writeLoginRPCResult(t *testing.T, w http.ResponseWriter, resultType string, data any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	response := sidecar.Response{
		ProtocolVersion: sidecar.ProtocolVersion,
		ID:              "http-1",
		Outcome: sidecar.ResponseOutcome{
			Status: "ok",
			Data:   mustLoginJSON(t, sidecar.Result{Type: resultType, Data: mustLoginJSON(t, data)}),
		},
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func mustLoginJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return data
}
