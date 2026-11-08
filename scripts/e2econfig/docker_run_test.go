package main

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"hegel.dev/go/hegel"
)

// Execute the real entrypoint readiness helper against a protocol-enforcing
// HTTP server. No config, account store, service, or container is created.
func checkEntrypointReadiness(t *testing.T, failures uint64) (bool, int64) {
	t.Helper()
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if r.URL.Path != "/health" || r.Header.Get("X-Inline-Protocol-Version") != "5" {
			http.Error(w, "protocol mismatch", 409)
			return
		}
		if uint64(n) <= failures {
			http.Error(w, "starting", 503)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	address, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal(err)
	}
	curl, err := exec.LookPath("curl")
	if err != nil {
		t.Fatal(err)
	}
	script, err := filepath.Abs("../../docker-run.sh")
	if err != nil {
		t.Fatal(err)
	}
	temp := t.TempDir()
	bin := filepath.Join(temp, "bin")
	if err = os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(bin, "curl"), []byte("#!/bin/sh\nexec \"$CURL_BINARY\" -A 'OpenAI File Downloader, XaiImageApiFetch/1.0' \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bash, "-c", `source "$1"; sidecar_pid=$$; sleep() { :; }; wait_for_sidecar`, "fixture", script)
	cmd.Env = []string{"PATH=" + bin + ":/usr/bin:/bin", "CURL_BINARY=" + curl, "HOME=" + temp, "DATA_DIR=" + filepath.Join(temp, "must-not-create"), "INLINE_SIDECAR_BIND=" + address.Host}
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("readiness timed out: %s", output)
	}
	if _, statErr := os.Stat(filepath.Join(temp, "must-not-create")); !os.IsNotExist(statErr) {
		t.Fatal("sourcing readiness helper had filesystem effects")
	}
	return err == nil, calls.Load()
}
func TestDockerEntrypointProtocolReadiness(t *testing.T) {
	ok, calls := checkEntrypointReadiness(t, 0)
	if !ok || calls != 2 {
		t.Fatalf("healthy protocol5 sidecar rejected: ok%t calls%d", ok, calls)
	}
}
func TestDockerEntrypointReadinessRetryBudgetProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		failures := hegel.Draw(ht, hegel.Integers[uint64](0, math.MaxUint64))
		if failures > 61 {
			failures = 61
		}
		ok, calls := checkEntrypointReadiness(t, failures)
		expected := int64(failures) + 2
		if expected > 61 {
			expected = 61
		}
		if ok != (failures <= 60) || calls != expected {
			ht.Fatalf("startupfailures%d: ok%t calls%d wantok%t calls%d", failures, ok, calls, failures <= 60, expected)
		}
	}, hegel.WithTestCases(30), hegel.WithDatabase(t.TempDir()))
}
