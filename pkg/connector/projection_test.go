package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"math"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/util/dbutil"
	"hegel.dev/go/hegel"
	"maunium.net/go/mautrix/id"
)

func fixtureJournal(t *testing.T, path string) *projectionStore {
	t.Helper()
	raw, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	s := &projectionStore{db: db}
	if err = s.init(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestProjectionJournalRestartIsolationAndCAS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.db")
	s := fixtureJournal(t, path)
	key := projectionKey{Bridge: "b", User: "@a:test", Login: "1", Receiver: "1", Chat: "7"}
	a, err := s.load(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	a.ReadMaxID = 10
	a.ManualUnread = true
	a.ReadPending = true
	if err = s.save(t.Context(), key, &a); err != nil {
		t.Fatal(err)
	}
	stale := a
	a.ReadMaxID = 20
	if err = s.save(t.Context(), key, &a); err != nil {
		t.Fatal(err)
	}
	stale.ReadApplied = 10
	if err = s.save(t.Context(), key, &stale); err == nil {
		t.Fatal("stale callback overwrote later desire")
	}
	restarted := fixtureJournal(t, path)
	got, err := restarted.load(t.Context(), key)
	if err != nil || got.ReadMaxID != 20 || got.ReadApplied != 0 || !got.ManualUnread {
		t.Fatalf("restart: %+v %v", got, err)
	}
	for _, other := range []projectionKey{{Bridge: "c", User: key.User, Login: key.Login, Receiver: key.Receiver, Chat: key.Chat}, {Bridge: key.Bridge, User: "@b:test", Login: key.Login, Receiver: key.Receiver, Chat: key.Chat}, {Bridge: key.Bridge, User: key.User, Login: "2", Receiver: key.Receiver, Chat: key.Chat}, {Bridge: key.Bridge, User: key.User, Login: key.Login, Receiver: "2", Chat: key.Chat}} {
		x, e := s.load(t.Context(), other)
		if e != nil || x.ReadMaxID != 0 {
			t.Fatalf("cross-account leak: %+v %v", x, e)
		}
	}
}
func TestProjectionSafePrefix(t *testing.T) {
	for _, tc := range []struct {
		order  []int64
		cursor int64
		want   id.EventID
	}{
		{[]int64{20, 10}, 10, ""}, {[]int64{10, 20}, 10, "$a"}, {[]int64{3, 10, 20}, 10, "$b"}, {[]int64{0, 10}, 10, ""}, {[]int64{10, 5, 20}, 10, "$b"}, {nil, 10, ""},
	} {
		rows := make([]receiptEvent, len(tc.order))
		for i, n := range tc.order {
			rows[i] = receiptEvent{EventID: id.EventID("$" + string(rune('a'+i))), SourceID: n}
		}
		if got := safeReceiptPrefix(rows, tc.cursor); got != tc.want {
			t.Errorf("%v cursor%d got%s want%s", tc.order, tc.cursor, got, tc.want)
		}
	}
}
func TestProjectionPrefixNeverCoversUnread(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		ids := hegel.Draw(ht, hegel.Lists(hegel.Integers[int64](math.MinInt64, math.MaxInt64)))
		cursor := int64(hegel.Draw(ht, hegel.Integers[int64](0, math.MaxInt64)))
		rows := make([]receiptEvent, len(ids))
		for i, n := range ids {
			rows[i] = receiptEvent{EventID: id.EventID(string(rune(i + 1))), SourceID: int64(n)}
		}
		got := safeReceiptPrefix(rows, cursor)
		if got != "" {
			for _, row := range rows {
				if row.SourceID <= 0 || row.SourceID > cursor {
					ht.Fatal("receipt covers unread/unknown source message")
				}
				if row.EventID == got {
					return
				}
			}
			ht.Fatal("unmapped receipt")
		}
		// Positive direction: a nonempty all-read prefix must produce its last event.
		if len(rows) > 0 && rows[0].SourceID > 0 && rows[0].SourceID <= cursor {
			ht.Fatal("lost valid safe prefix")
		}
	}, hegel.WithTestCases(3000), hegel.WithDatabase(t.TempDir()))
}

func TestPrivateReceiptUsesOnlyPrivateMarkerAndPreservesUnread(t *testing.T) {
	var posted map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/read_markers"):
			if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
				t.Error(err)
			}
			json.NewEncoder(w).Encode(map[string]any{})
		case strings.HasSuffix(r.URL.Path, "/m.fully_read"):
			json.NewEncoder(w).Encode(map[string]any{"event_id": "$read"})
		case strings.HasSuffix(r.URL.Path, "/m.marked_unread"):
			if r.Method != "GET" {
				t.Error("source read wrote manual-unread account data")
			}
			json.NewEncoder(w).Encode(map[string]any{"unread": true})
		case strings.HasSuffix(r.URL.Path, "/sync"):
			w.Write([]byte(`{"next_batch":"before","rooms":{"join":{"!room:test":{"ephemeral":{"events":[{"type":"m.receipt","content":{"$read":{"m.read.private":{"@user:test":{}}}}}]}}}}}`))
		default:
			t.Errorf("unexpected Matrix call %s", r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer server.Close()
	cli, err := mautrix.NewClient(server.URL, "@user:test", "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	unread, err := applyPrivateReceipt(t.Context(), cli, "!room:test", "@user:test", "$read")
	if err != nil || unread == nil || !*unread {
		t.Fatalf("private receipt readback=%v %v", unread, err)
	}
	if posted["m.read"] != nil || posted["m.read.private"] != "$read" || posted["m.fully_read"] != "$read" {
		t.Fatalf("unsafe marker request: %v", posted)
	}
}
func TestFrameworkPrivateReceiptCannotEchoToInline(t *testing.T) {
	// A nil bridge would panic on user/source lookup. The pinned exported seam
	// must ignore private receipts entirely, including delayed replay after restart.
	for range 2 {
		p := testPortal("7")
		content := event.ReceiptEventContent{}
		content.Set("$read", event.ReceiptTypeReadPrivate, "@user:test", event.ReadReceipt{})
		result := p.Internal().HandleMatrixReceipts(t.Context(), &event.Event{Type: event.EphemeralEventReceipt, Content: event.Content{Parsed: &content}})
		if !result.Success {
			t.Fatal("private receipt rejected before safe filtering")
		}
	}
}

func TestHistoryCompletionRejectsMissingAndFakeMappings(t *testing.T) {
	key := networkid.PortalKey{ID: "7", Receiver: "1"}
	records := []sidecar.MessageRecord{{ChatID: 7, MessageID: 20}, {ChatID: 7, MessageID: 10}}
	rows := map[networkid.MessageID]*database.Message{}
	lookup := func(mid networkid.MessageID) (*database.Message, error) { return rows[mid], nil }
	if persistedHistoryPrefix(key, records, lookup) != 0 {
		t.Fatal("empty callback advanced delivery")
	}
	rows["7/20"] = &database.Message{Room: key, ID: "7/20", MXID: "$20"}
	if persistedHistoryPrefix(key, records, lookup) != 0 {
		t.Fatal("missing lower mapping skipped")
	}
	rows["7/10"] = &database.Message{Room: key, ID: "7/10", MXID: "$10"}
	if persistedHistoryPrefix(key, records, lookup) != 20 {
		t.Fatal("complete mapping page not checkpointed")
	}
	rows["7/10"].SetFakeMXID()
	if persistedHistoryPrefix(key, records, lookup) != 0 {
		t.Fatal("fake Matrix event checkpointed")
	}
}

type noPuppetMatrix struct{ bridgev2.MatrixConnector }

func (noPuppetMatrix) NewUserIntent(context.Context, id.UserID, string) (bridgev2.MatrixAPI, string, error) {
	return nil, "", errors.New("fixture has no puppet")
}
func fixtureProjectionClient(t *testing.T, store *projectionStore) *InlineClient {
	t.Helper()
	br := &bridgev2.Bridge{DB: &database.Database{Database: store.db, BridgeID: "fixture"}, Matrix: noPuppetMatrix{}}
	user := &bridgev2.User{User: &database.User{MXID: "@user:test"}, Bridge: br}
	return &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login"}, User: user, Bridge: br}}
}
func TestReadIntentPrecedesDeliveryAndMissingPuppetStaysPendingAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "read.db")
	store := fixtureJournal(t, path)
	sourceWrites := 0
	server := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rpc/state/chat" {
			writeConnectorSidecarResult(t, w, "chat_state", sidecar.ChatStateSnapshot{ChatID: 7, ReadState: &sidecar.ReadStateRecord{ChatID: 7, ReadMaxID: foundationInt64(10), MarkedUnread: true}})
		} else {
			sourceWrites++
			http.Error(w, "unexpected source write", 500)
		}
	}))
	defer server.Close()
	ic := fixtureProjectionClient(t, store)
	ic.Sidecar = sidecar.NewClient(server.URL)
	if err := ic.readDesireBeforeDelivery(t.Context(), 7); err != nil {
		t.Fatal(err)
	}
	_, key, err := ic.projectionJournal(t.Context(), "7")
	if err != nil {
		t.Fatal(err)
	}
	p, err := store.load(t.Context(), key)
	if err != nil || p.ReadMaxID != 10 || !p.ManualUnread || !p.ReadPending || p.ReadApplied != 0 {
		t.Fatal("source intent not durable before delivery", err)
	}
	restarted := fixtureProjectionClient(t, fixtureJournal(t, path))
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "7", Receiver: "login"}, MXID: "!room:test"}}
	restarted.reconcileProjection(t.Context(), portal)
	p, err = store.load(t.Context(), key)
	if err != nil || !p.ReadPending || !p.UnreadPending || p.ReadApplied != 0 || p.ReadError != "double_puppet_unavailable" || sourceWrites != 0 {
		t.Fatalf("missing puppet falsely applied or echoed source: %+v writes%d %v", p, sourceWrites, err)
	}
}

func TestReceiptCoverageUsesPrefixMaximumAndRearmsForLateRead(t *testing.T) {
	order := []receiptEvent{{EventID: "$20", SourceID: 20}, {EventID: "$10", SourceID: 10}}
	plan := receiptPlanForOrder(order, 20)
	if plan.Target != "$10" || plan.CoveredMax != 20 || !plan.Complete {
		t.Fatalf("timestamp disorder falsely stays pending: %+v", plan)
	}
	order = append(order, receiptEvent{EventID: "$15", SourceID: 15})
	late := receiptPlanForOrder(order, 20)
	if late.Target != "$15" || late.CoveredMax != 20 || !late.Complete {
		t.Fatalf("late source-read event did not rearm target: %+v", late)
	}
	blocked := receiptPlanForOrder(append(order, receiptEvent{EventID: "$25", SourceID: 25}, receiptEvent{EventID: "$16", SourceID: 16}), 20)
	if blocked.Target != "$15" || blocked.CoveredMax != 20 || blocked.Complete {
		t.Fatalf("nonprefix read falsely completed: %+v", blocked)
	}
}

func TestReceiptPlanCoversOnlyActualReadPrefix(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		ids := hegel.Draw(ht, hegel.Lists(hegel.Integers[int64](math.MinInt64, math.MaxInt64)))
		cursor := int64(hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64)))
		rows := make([]receiptEvent, len(ids))
		for i, n := range ids {
			rows[i] = receiptEvent{EventID: id.EventID(fmt.Sprintf("$%d", i)), SourceID: int64(n)}
		}
		plan := receiptPlanForOrder(rows, cursor)
		end := 0
		for end < len(ids) && ids[end] > 0 && int64(ids[end]) <= cursor {
			end++
		}
		var maximum int64
		for _, n := range ids[:end] {
			if int64(n) > maximum {
				maximum = int64(n)
			}
		}
		if plan.CoveredMax != maximum {
			ht.Fatal("coverage differs from maximum source ID in safe prefix")
		}
		if end == 0 && plan.Target != "" {
			ht.Fatal("empty safe prefix emitted receipt")
		}
		if end > 0 && plan.Target != rows[end-1].EventID {
			ht.Fatal("safe prefix did not target final Matrix event")
		}
		allKnownReadsCovered := true
		for _, n := range ids[end:] {
			if n > 0 && int64(n) <= cursor {
				allKnownReadsCovered = false
			}
		}
		if plan.Complete != (maximum >= cursor && allKnownReadsCovered) {
			ht.Fatal("completion hides nonprefix read message or missing cursor")
		}
	}, hegel.WithTestCases(3000), hegel.WithDatabase(t.TempDir()))
}

func TestPrivateReceiptFailuresNeverImplyReadCompletion(t *testing.T) {
	for _, failure := range []string{"write", "fully_read", "manual_unread", "private_readback"} {
		t.Run(failure, func(t *testing.T) {
			unreadReads := 0
			markerWrites := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/read_markers"):
					markerWrites++
					var content map[string]any
					_ = json.NewDecoder(r.Body).Decode(&content)
					if _, public := content["m.read"]; public {
						t.Error("source read emitted public receipt")
					}
					if failure == "write" {
						w.WriteHeader(400)
						w.Write([]byte(`{"errcode":"M_BAD_JSON","error":"injected"}`))
						return
					}
					w.Write([]byte(`{}`))
				case strings.HasSuffix(r.URL.Path, "/m.fully_read"):
					target := "$target"
					if failure == "fully_read" {
						target = "$other"
					}
					json.NewEncoder(w).Encode(map[string]any{"event_id": target})
				case strings.HasSuffix(r.URL.Path, "/m.marked_unread"):
					if r.Method != "GET" {
						t.Error("source read wrote manual unread")
					}
					unreadReads++
					unread := true
					if failure == "manual_unread" && unreadReads > 1 {
						unread = false
					}
					json.NewEncoder(w).Encode(map[string]any{"unread": unread})
				case strings.HasSuffix(r.URL.Path, "/sync"):
					w.Write([]byte(`{"next_batch":"before","rooms":{"join":{"!room:test":{"ephemeral":{"events":[]}}}}}`))
				default:
					t.Errorf("unexpected call %s", r.URL.Path)
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			cli, err := mautrix.NewClient(server.URL, "@user:test", "fixture")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = applyPrivateReceipt(t.Context(), cli, "!room:test", "@user:test", "$target"); err == nil {
				t.Fatal("failed write/readback falsely completed")
			}
			if markerWrites != 1 {
				t.Fatal("receipt write repeated", markerWrites)
			}
		})
	}
}

func TestReceiptScannerBlocksUnreadStickerPollAndUnknownEvents(t *testing.T) {
	store := fixtureJournal(t, filepath.Join(t.TempDir(), "scanner.db"))
	db := database.New("fixture", database.MetaTypes{}, store.db)
	if err := db.Upgrade(t.Context()); err != nil {
		t.Fatal(err)
	}
	key := networkid.PortalKey{ID: "7", Receiver: "login"}
	if err := db.Portal.Insert(t.Context(), &database.Portal{PortalKey: key}); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int64{10, 20} {
		if err := db.Message.Insert(t.Context(), &database.Message{ID: makeMessageID(7, n), MXID: id.EventID(fmt.Sprintf("$%d", n)), Room: key}); err != nil {
			t.Fatal(err)
		}
	}
	blocker := "m.sticker"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"chunk": []map[string]any{
			{"event_id": "$20", "type": "m.room.message", "content": map[string]any{"body": "20", "msgtype": "m.text"}},
			{"event_id": "$unknown", "type": blocker, "content": map[string]any{}},
			{"event_id": "$10", "type": "m.room.message", "content": map[string]any{"body": "10", "msgtype": "m.text"}},
		}})
	}))
	defer server.Close()
	cli, err := mautrix.NewClient(server.URL, "@user:test", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	ic := fixtureProjectionClient(t, store)
	ic.UserLogin.Bridge.DB = db
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: key, MXID: "!room:test"}}
	raw := &matrix.ASIntent{Matrix: &appservice.IntentAPI{Client: cli}}
	for _, typ := range []string{"m.sticker", "org.matrix.msc3381.poll.start", "unknown.content"} {
		blocker = typ
		plan, err := ic.receiptTarget(t.Context(), portal, raw, 20)
		if err != nil || plan.Target != "$10" || plan.Complete {
			t.Fatalf("scanner crossed unread-capable %s: %+v %v", typ, plan, err)
		}
	}
}

func TestHistoricalRecoverySelectsMissingIDsDespiteTimestampCutoff(t *testing.T) {
	records := []sidecar.MessageRecord{{ChatID: 7, MessageID: 20, Timestamp: 100}, {ChatID: 7, MessageID: 10, Timestamp: 200}}
	key := networkid.PortalKey{ID: "7", Receiver: "login"}
	// Characterize the pinned framework path we must bypass for source IDs.
	p := testPortal("7")
	messages := []*bridgev2.BackfillMessage{{ID: "7/20", Timestamp: time.Unix(100, 0)}, {ID: "7/10", Timestamp: time.Unix(200, 0)}}
	cut := p.Internal().CutoffMessages(t.Context(), messages, false, true, &database.Message{ID: "7/30", Timestamp: time.Unix(300, 0)})
	if len(cut) != 0 {
		t.Fatal("pinned framework timestamp-cutoff characterization changed")
	}
	lookup := func(networkid.MessageID) (*database.Message, error) { return nil, nil }
	missing, err := pendingHistoricalRecords(key, records, lookup)
	if err != nil || len(missing) != 2 {
		t.Fatal("late source messages incorrectly discarded", err)
	}
	rows := map[networkid.MessageID]*database.Message{"7/20": {Room: key, MXID: "$20"}}
	lookup = func(mid networkid.MessageID) (*database.Message, error) { return rows[mid], nil }
	missing, err = pendingHistoricalRecords(key, records, lookup)
	if err != nil || len(missing) != 1 || missing[0].MessageID != 10 {
		t.Fatal("recovery repeated mapped event or lost missing event", err)
	}
	other := append(records, sidecar.MessageRecord{ChatID: 8, MessageID: 10})
	if _, err = pendingHistoricalRecords(key, other, lookup); err == nil {
		t.Fatal("cross-chat history bundle accepted")
	}
}

func TestPrivateReceiptReadbackUsesPrewriteSyncWatermark(t *testing.T) {
	posted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/m.marked_unread"):
			w.Write([]byte(`{"unread":true}`))
		case strings.HasSuffix(r.URL.Path, "/read_markers"):
			posted = true
			w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/m.fully_read"):
			w.Write([]byte(`{"event_id":"$late"}`))
		case strings.HasSuffix(r.URL.Path, "/sync"):
			target := "$cached"
			if r.URL.Query().Get("since") == "before" && posted {
				target = "$late"
			}
			json.NewEncoder(w).Encode(map[string]any{"next_batch": "before", "rooms": map[string]any{"join": map[string]any{"!room:test": map[string]any{"ephemeral": map[string]any{"events": []map[string]any{{"type": "m.receipt", "content": map[string]any{target: map[string]any{"m.read.private": map[string]any{"@user:test": map[string]any{}}}}}}}}}}})
		default:
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	cli, err := mautrix.NewClient(server.URL, "@user:test", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = applyPrivateReceipt(t.Context(), cli, "!room:test", "@user:test", "$late"); err != nil {
		t.Fatal("cached initial sync hid successful private receipt", err)
	}
}

func TestPrivateReceiptRetryAdvancesCachedSync(t *testing.T) {
	cursor := ""
	posted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/m.marked_unread"):
			w.Write([]byte(`{"unread":true}`))
		case strings.HasSuffix(r.URL.Path, "/m.fully_read"):
			w.Write([]byte(`{"event_id":"$late"}`))
		case strings.HasSuffix(r.URL.Path, "/read_markers"):
			posted = true
			w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/sync"):
			since := r.URL.Query().Get("since")
			next := "initial"
			target := "$old"
			if since == "initial" {
				next = "cached"
			}
			if since == "cached" && posted {
				next = "fresh"
				target = "$late"
			}
			json.NewEncoder(w).Encode(map[string]any{"next_batch": next, "rooms": map[string]any{"join": map[string]any{"!room:test": map[string]any{"ephemeral": map[string]any{"events": []map[string]any{{"type": "m.receipt", "content": map[string]any{target: map[string]any{"m.read.private": map[string]any{"@user:test": map[string]any{}}}}}}}}}}})
		default:
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	cli, _ := mautrix.NewClient(server.URL, "@user:test", "fixture")
	for range 3 {
		_, err := applyPrivateReceipt(t.Context(), cli, "!room:test", "@user:test", "$late", &cursor)
		if err == nil && cursor == "fresh" {
			return
		}
	}
	t.Fatal("retry never advanced beyond cached sync cursor", cursor)
}

func TestFinalReviewReceiptEvidenceSurvivesFailedReadback(t *testing.T) {
	// This server returns incremental receipt changes, not a snapshot on every
	// sync. Repeating the same receipt is an idempotent no-op.
	atTarget := false
	fullReads := 0
	markerChanges := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/m.marked_unread"):
			w.Write([]byte(`{"unread":true}`))
		case strings.HasSuffix(r.URL.Path, "/read_markers"):
			if !atTarget {
				atTarget = true
				markerChanges++
			}
			w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/m.fully_read"):
			fullReads++
			if fullReads <= 2 {
				w.WriteHeader(503)
				w.Write([]byte(`{"errcode":"M_UNKNOWN","error":"synthetic transient readback failure"}`))
				return
			}
			w.Write([]byte(`{"event_id":"$target"}`))
		case strings.HasSuffix(r.URL.Path, "/sync"):
			events := []any{}
			next := "s0"
			if atTarget {
				next = "s1"
				if r.URL.Query().Get("since") != "s1" {
					events = append(events, map[string]any{"type": "m.receipt", "content": map[string]any{"$target": map[string]any{"m.read.private": map[string]any{"@user:test": map[string]any{}}}}})
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"next_batch": next, "rooms": map[string]any{"join": map[string]any{"!room:test": map[string]any{"ephemeral": map[string]any{"events": events}}}}})
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	cli, err := mautrix.NewClient(server.URL, "@user:test", "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	cli.DefaultHTTPRetries = 0
	path := filepath.Join(t.TempDir(), "receipt.db")
	store := fixtureJournal(t, path)
	key := projectionKey{Bridge: "b", User: "@user:test", Login: "login", Receiver: "login", Chat: "7"}
	state, _ := store.load(t.Context(), key)
	state.ReadMaxID = 20
	state.ReadPending = true
	if err = store.save(t.Context(), key, &state); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 4; attempt++ {
		// Match reconcileProjection: retain the returned sync token even on an
		// apply error, then reload it from a new SQLite handle for retry.
		state, err = store.load(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		_, err = applyPrivateReceiptWithEvidence(t.Context(), cli, receiptOperation{Key: key, Room: "!room:test", User: "@user:test", Target: "$target", ReadMaxID: state.ReadMaxID, ManualUnread: state.ManualUnread}, &state.ReceiptSyncToken, &state.ReceiptObservation)
		t.Logf("attempt%d token=%s err=%v", attempt, state.ReceiptSyncToken, err)
		if attempt <= 2 && err == nil {
			t.Fatal("injected readback failure ignored")
		}
		if attempt > 2 && err == nil {
			return
		}
		if saveErr := store.save(t.Context(), key, &state); saveErr != nil {
			t.Fatal(saveErr)
		}
		store = fixtureJournal(t, path)
	}
	t.Fatalf("correct private and fully-read target remains pending after failures clear; token=%s, marker_changes=%d", state.ReceiptSyncToken, markerChanges)
}

func TestPrivateReceiptEvidenceCannotCrossOperation(t *testing.T) {
	base := receiptOperation{Key: projectionKey{Bridge: "bridge", User: "@login:test", Login: "login", Receiver: "receiver", Chat: "7"}, Room: "!room:test", User: "@user:test", Target: "$target", ReadMaxID: 20, ManualUnread: true}
	for _, field := range []string{"same", "bridge", "account", "login", "receiver", "chat", "room", "puppet", "target", "cursor", "manual_unread"} {
		t.Run(field, func(t *testing.T) {
			op := base
			switch field {
			case "bridge":
				op.Key.Bridge += "other"
			case "account":
				op.Key.User += "other"
			case "login":
				op.Key.Login += "other"
			case "receiver":
				op.Key.Receiver += "other"
			case "chat":
				op.Key.Chat = "8"
			case "room":
				op.Room = "!other:test"
			case "puppet":
				op.User = "@other:test"
			case "target":
				op.Target = "$other"
			case "cursor":
				op.ReadMaxID++
			case "manual_unread":
				op.ManualUnread = false
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/m.marked_unread"):
					w.Write([]byte(`{"unread":true}`))
				case strings.HasSuffix(r.URL.Path, "/read_markers"):
					w.Write([]byte(`{}`))
				case strings.HasSuffix(r.URL.Path, "/m.fully_read"):
					json.NewEncoder(w).Encode(map[string]any{"event_id": op.Target})
				case strings.HasSuffix(r.URL.Path, "/sync"):
					w.Write([]byte(`{"next_batch":"s2","rooms":{"join":{}}}`))
				default:
					t.Errorf("unexpected route %s", r.URL.Path)
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			cli, err := mautrix.NewClient(server.URL, op.User, "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			evidence := &receiptObservation{Operation: base, SyncToken: "s1"}
			cursor := "s1"
			_, err = applyPrivateReceiptWithEvidence(t.Context(), cli, op, &cursor, &evidence)
			if field == "same" {
				if err != nil {
					t.Fatal("lost exact retained proof", err)
				}
			} else if err == nil || evidence != nil {
				t.Fatalf("evidence crossed %s: evidence%+v err%v", field, evidence, err)
			}
		})
	}
}

func TestPrivateReceiptEvidenceRejectsConflictingAndThreadedTargets(t *testing.T) {
	op := receiptOperation{Room: "!room:test", User: "@user:test", Target: "$target"}
	for _, tc := range []struct {
		name, content string
		retained      bool
	}{
		{"exact", `{"$target":{"m.read.private":{"@user:test":{}}}}`, true},
		{"later-other", `{"$other":{"m.read.private":{"@user:test":{}}}}`, false},
		{"ambiguous", `{"$target":{"m.read.private":{"@user:test":{}}},"$other":{"m.read.private":{"@user:test":{}}}}`, false},
		{"public-only", `{"$target":{"m.read":{"@user:test":{}}}}`, false},
		{"thread-only", `{"$target":{"m.read.private":{"@user:test":{"thread_id":"$thread"}}}}`, false},
		{"main-only", `{"$target":{"m.read.private":{"@user:test":{"thread_id":"main"}}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var response mautrix.RespSync
			raw := `{"next_batch":"s1","rooms":{"join":{"!room:test":{"ephemeral":{"events":[{"type":"m.receipt","content":` + tc.content + `}]}}}}}`
			if err := json.Unmarshal([]byte(raw), &response); err != nil {
				t.Fatal(err)
			}
			var evidence *receiptObservation
			if tc.name == "later-other" || tc.name == "ambiguous" {
				evidence = &receiptObservation{Operation: op, SyncToken: "s0"}
			}
			if err := observePrivateReceipt(&response, op, &evidence); err != nil {
				t.Fatal(err)
			}
			if (evidence != nil) != tc.retained {
				t.Fatalf("evidence%+v wantRetained%t", evidence, tc.retained)
			}
		})
	}
}

func TestReceiptEvidenceClearsOnNewDesireAndRejectsStaleCompletion(t *testing.T) {
	ic := fixtureHistoryClient(t, filepath.Join(t.TempDir(), "intent.db"))
	ic.UserLogin.User = &bridgev2.User{User: &database.User{MXID: "@user:test"}, Bridge: ic.UserLogin.Bridge}
	read := sidecar.ReadStateRecord{ChatID: 7, ReadMaxID: foundationInt64(10), MarkedUnread: true}
	if err := ic.storeReadDesire(t.Context(), &read); err != nil {
		t.Fatal(err)
	}
	journal, key, err := ic.projectionJournal(t.Context(), "7")
	if err != nil {
		t.Fatal(err)
	}
	state, err := journal.load(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	state.ReceiptObservation = &receiptObservation{Operation: receiptOperation{Key: key, Room: "!room:test", User: "@user:test", Target: "$10", ReadMaxID: 10, ManualUnread: true}, SyncToken: "s1"}
	if err = journal.save(t.Context(), key, &state); err != nil {
		t.Fatal(err)
	}
	stale := state
	read.ReadMaxID = foundationInt64(20)
	if err = ic.storeReadDesire(t.Context(), &read); err != nil {
		t.Fatal(err)
	}
	stale.ReadApplied = 10
	if err = journal.save(t.Context(), key, &stale); !errors.Is(err, errProjectionConflict) {
		t.Fatalf("stale receipt completion accepted: %v", err)
	}
	// Returning to the same scalar cursor is a new desire. It must not revive
	// a proof from the operation before the intervening cursor change.
	read.ReadMaxID = foundationInt64(10)
	if err = ic.storeReadDesire(t.Context(), &read); err != nil {
		t.Fatal(err)
	}
	state, err = journal.load(t.Context(), key)
	if err != nil || state.ReceiptObservation != nil || state.ReadApplied != 0 || !state.ReadPending {
		t.Fatalf("old evidence revived: %+v %v", state, err)
	}
}
