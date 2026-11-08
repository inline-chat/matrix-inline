package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

func TestFixturePresentationControlsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.json")
	srv := newFixtureServer(path)
	srv.loggedIn = true
	mux := http.NewServeMux()
	srv.register(mux)
	post := func(raw string) {
		t.Helper()
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/fixture/presentation", strings.NewReader(raw))
		mux.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("control %s: %d %s", raw, w.Code, w.Body.String())
		}
	}
	post(`{"action":"move","chat_id":202,"space_id":302}`)
	post(`{"action":"read","chat_id":202,"read_max_id":10,"marked_unread":true}`)
	restarted := newFixtureServer(path)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if restarted.dialogs[1].Structure.SpaceID == nil || *restarted.dialogs[1].Structure.SpaceID != 302 {
		t.Fatal("move lost after fixture restart")
	}
	if r := restarted.readStates[202]; r.ReadMaxID == nil || *r.ReadMaxID != 10 || !r.MarkedUnread {
		t.Fatal("read state lost after fixture restart")
	}
	if restarted.sequence == 0 || restarted.generation == "" {
		t.Fatal("fixture replay lineage lost")
	}
}
func TestFixtureCompatibilityNeverNeedsLogin(t *testing.T) {
	srv := newFixtureServer(filepath.Join(t.TempDir(), "fixture.json"))
	mux := http.NewServeMux()
	srv.register(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/compatibility", nil))
	var p sidecar.ProtocolInfo
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil || p.ProtocolVersion != sidecar.ProtocolVersion || srv.loggedIn {
		t.Fatal("static compatibility contract failed")
	}
}

func TestFixtureAckConfirmsGenerationAndSequence(t *testing.T) {
	srv := newFixtureServer(filepath.Join(t.TempDir(), "fixture.json"))
	mux := http.NewServeMux()
	srv.register(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/rpc/events/ack", strings.NewReader(`{"generation":"fixture-generation","sequence":0}`)))
	var ack sidecar.EventAckResponse
	if err := json.Unmarshal(w.Body.Bytes(), &ack); err != nil || ack.Generation != "fixture-generation" || ack.AcknowledgedSequence != 0 {
		t.Fatalf("ack failed confirmation: %+v %v", ack, err)
	}
}

func TestFixtureReceiptCaseIsFreshAndDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.json")
	srv := newFixtureServer(path)
	mux := http.NewServeMux()
	srv.register(mux)
	request := `{"action":"receipt_case","chat_id":10001}`
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/fixture/presentation", strings.NewReader(request)))
	if w.Code != 200 {
		t.Fatalf("receipt case: %d %s", w.Code, w.Body.String())
	}
	restarted := newFixtureServer(path)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	messages := restarted.history[10001]
	if len(messages) != 2 || messages[0].MessageID != 20 || messages[0].Timestamp != 100 || messages[1].MessageID != 10 || messages[1].Timestamp != 200 {
		t.Fatalf("receipt order lost: %+v", messages)
	}
	read := restarted.readStates[10001]
	if read.ReadMaxID == nil || *read.ReadMaxID != 10 || !read.MarkedUnread {
		t.Fatal("initial source read state lost")
	}
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/fixture/presentation", strings.NewReader(request)))
	if w.Code != http.StatusConflict {
		t.Fatal("receipt case overwrote existing source chat")
	}
}
