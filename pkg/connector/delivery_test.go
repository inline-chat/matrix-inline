package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func openTestBridgeDatabase(t *testing.T, path string) *database.Database {
	t.Helper()
	raw, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	wrapped, err := dbutil.NewWithDB(raw, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	db := database.New("inline-test", (&InlineConnector{}).GetDBMetaTypes(), wrapped)
	if err := db.Upgrade(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	return db
}
func testBridgeDatabase(t *testing.T) *database.Database {
	return openTestBridgeDatabase(t, filepath.Join(t.TempDir(), "bridge.db"))
}
func testDeliveryClient(db *database.Database) *InlineClient {
	return &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "42"}, Bridge: &bridgev2.Bridge{ID: db.BridgeID, DB: db, BackgroundCtx: context.Background()}}, storeNamespace: "account-42"}
}
func TestDeliveryMappingSurvivesReopenAndRejectsMissingOrForeign(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	db := openTestBridgeDatabase(t, path)
	client := testDeliveryClient(db)
	if err := client.verifyMessageDelivery(t.Context(), 7, 11); err == nil {
		t.Fatal("unmapped delivery accepted")
	}
	key := client.portalKeyForChat(7)
	if err := db.Portal.Insert(t.Context(), &database.Portal{PortalKey: key, MXID: "!room:example.org"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Ghost.Insert(t.Context(), &database.Ghost{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Message.Insert(t.Context(), &database.Message{ID: makeMessageID(7, 11), Room: key, SenderID: "1", MXID: "$message", Timestamp: time.Now(), Metadata: &MessageMetadata{}}); err != nil {
		t.Fatal(err)
	}
	if err := client.verifyMessageDelivery(t.Context(), 7, 11); err != nil {
		t.Fatal(err)
	}
	db.RawDB.Close()
	reopened := testDeliveryClient(openTestBridgeDatabase(t, path))
	if err := reopened.verifyMessageDelivery(t.Context(), 7, 11); err != nil {
		t.Fatal(err)
	}
	reopened.UserLogin.ID = "43"
	if err := reopened.verifyMessageDelivery(t.Context(), 7, 11); err == nil {
		t.Fatal("foreign login mapping accepted")
	}
}
func TestReadIntentDurableAndAccountIsolated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	db := openTestBridgeDatabase(t, path)
	client := testDeliveryClient(db)
	maxID := int64(99)
	if err := client.saveReadIntent(t.Context(), &sidecar.ReadStateRecord{ChatID: 7, ReadMaxID: &maxID, MarkedUnread: true}); err != nil {
		t.Fatal(err)
	}
	db.RawDB.Close()
	client = testDeliveryClient(openTestBridgeDatabase(t, path))
	state, err := client.loadReadIntent(t.Context(), 7)
	if err != nil || state == nil || *state.ReadMaxID != 99 || !state.MarkedUnread {
		t.Fatalf("lost durable intent: %+v %v", state, err)
	}
	client.storeNamespace = "new-account"
	if state, err := client.loadReadIntent(t.Context(), 7); err != nil || state != nil {
		t.Fatalf("intent crossed namespaces: %+v %v", state, err)
	}
	client.UserLogin.Bridge.DB.RawDB.Close()
	if err := client.saveReadIntent(t.Context(), &sidecar.ReadStateRecord{ChatID: 7}); err == nil {
		t.Fatal("failed write accepted")
	}
}
func TestHistoryCompletionRejectsAdmissionAndBackgrounding(t *testing.T) {
	for _, res := range []bridgev2.EventHandlingResult{bridgev2.EventHandlingResultQueued, {Success: true, Queued: true, Error: bridgev2.ErrHandlerBackgrounded}, {Success: true, Error: errors.New("late failure")}} {
		if historyDeliveryAccepted(res) || eventHandlingResultError("test", res) == nil {
			t.Fatalf("pending result accepted: %+v", res)
		}
	}
	request := historyRequestForDelivery(sidecar.DialogRecord{ChatID: 7}, 0, 20)
	if request.AfterMessageID == nil || *request.AfterMessageID != 0 {
		t.Fatal("initial catchup requests latest window instead of oldest page")
	}
}
func TestVerifiedReadPrefixStopsBeforeUnreadOrUnmappedEvent(t *testing.T) {
	key := networkid.PortalKey{ID: "7", Receiver: "42"}
	lookup := func(_ context.Context, eventID id.EventID) (*database.Message, error) {
		switch eventID {
		case "$read1":
			return &database.Message{ID: "7/1", Room: key, MXID: eventID}, nil
		case "$unread":
			return &database.Message{ID: "7/9", Room: key, MXID: eventID}, nil
		case "$read2":
			return &database.Message{ID: "7/2", Room: key, MXID: eventID}, nil
		}
		return nil, nil
	}
	for _, barrier := range []*event.Event{{ID: "$unread", Type: event.EventMessage}, {ID: "$unknown", Type: event.EventSticker}} {
		target, err := verifiedReadPrefix(t.Context(), []*event.Event{{ID: "$read1", Type: event.EventMessage}, barrier, {ID: "$read2", Type: event.EventMessage}}, key, 2, lookup)
		if err != nil || target != "$read1" {
			t.Fatalf("crossed timeline barrier: %s %v", target, err)
		}
	}
	target, err := verifiedReadPrefix(t.Context(), []*event.Event{{ID: "$read1", Type: event.EventMessage}}, networkid.PortalKey{ID: "7", Receiver: "other"}, 2, lookup)
	if err != nil || target != "" {
		t.Fatalf("crossed account boundary: %s %v", target, err)
	}
}
func TestBackfillRejectsForeignChatInsteadOfSkipping(t *testing.T) {
	_, _, err := (&InlineClient{}).backfillMessagesFromHistory(t.Context(), testPortal("7"), []sidecar.MessageRecord{{ChatID: 8, MessageID: 1, Content: sidecar.MessageContent{Type: "text", Text: "foreign"}}})
	if err == nil {
		t.Fatal("foreign backfill accepted")
	}
}

// Exercise the pinned framework itself: an unbuffered queue can still return
// success while its handler is blocked. Such a result must not retire source work.
type deliveryTestMatrix struct{ bridgev2.MatrixConnector }

func (*deliveryTestMatrix) Init(*bridgev2.Bridge)         {}
func (*deliveryTestMatrix) BotIntent() bridgev2.MatrixAPI { return &deliveryTestIntent{} }
func (*deliveryTestMatrix) NewUserIntent(context.Context, id.UserID, string) (bridgev2.MatrixAPI, string, error) {
	return nil, "", nil
}

type deliveryTestIntent struct{ bridgev2.MatrixAPI }

func (*deliveryTestIntent) EnsureInvited(context.Context, id.RoomID, id.UserID) error { return nil }

type deliveryTestNetwork struct{ bridgev2.NetworkConnector }

func (*deliveryTestNetwork) Init(*bridgev2.Bridge) {}
func (*deliveryTestNetwork) GetDBMetaTypes() database.MetaTypes {
	return (&InlineConnector{}).GetDBMetaTypes()
}
func (*deliveryTestNetwork) LoadUserLogin(_ context.Context, login *bridgev2.UserLogin) error {
	login.Client = &InlineClient{UserLogin: login}
	return nil
}
func (*deliveryTestNetwork) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}
func TestFrameworkBackgroundResultCannotCommitDelivery(t *testing.T) {
	db := testBridgeDatabase(t)
	br := bridgev2.NewBridge(db.BridgeID, db.Database, zerolog.Nop(), &bridgeconfig.BridgeConfig{SplitPortals: true}, &deliveryTestMatrix{}, &deliveryTestNetwork{}, func(*bridgev2.Bridge) bridgev2.CommandProcessor { return nil })
	br.BackgroundCtx = t.Context()
	if err := db.User.Insert(t.Context(), &database.User{MXID: "@test:example.org"}); err != nil {
		t.Fatal(err)
	}
	if err := db.UserLogin.Insert(t.Context(), &database.UserLogin{ID: "42", UserMXID: "@test:example.org", Metadata: &UserLoginMetadata{}}); err != nil {
		t.Fatal(err)
	}
	login, err := br.GetExistingUserLoginByID(t.Context(), "42")
	if err != nil || login == nil {
		t.Fatalf("load login: %v", err)
	}
	t.Cleanup(login.BridgeState.Destroy)
	key := networkid.PortalKey{ID: "7", Receiver: "42"}
	if err := db.Portal.Insert(t.Context(), &database.Portal{PortalKey: key, MXID: "!room:example.org"}); err != nil {
		t.Fatal(err)
	}
	previousBuffer, previousTicks := bridgev2.PortalEventBuffer, bridgev2.EventHandlingTimeoutTicks
	bridgev2.PortalEventBuffer = 0
	bridgev2.EventHandlingTimeoutTicks = 0
	t.Cleanup(func() {
		bridgev2.PortalEventBuffer = previousBuffer
		bridgev2.EventHandlingTimeoutTicks = previousTicks
	})
	release, done := make(chan struct{}), make(chan struct{})
	res := br.QueueRemoteEvent(login, &simplevent.EventMeta{Type: bridgev2.RemoteEventUnknown, PortalKey: key, PreHandleFunc: func(context.Context, *bridgev2.Portal) { <-release }, PostHandleFunc: func(context.Context, *bridgev2.Portal) { close(done) }})
	if !res.Success || !res.Queued {
		close(release)
		t.Fatalf("framework behavior changed: %+v", res)
	}
	if eventHandlingResultError("held event", res) == nil {
		close(release)
		t.Fatal("backgrounded event acknowledged")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("held handler failed to finish")
	}
	// The production configuration keeps the lane synchronous even for slow work.
	bridgev2.EventHandlingTimeoutTicks = int(^uint(0) >> 1)
	if err := validatePortalEventDelivery(); err != nil {
		t.Fatal(err)
	}
	release, done = make(chan struct{}), make(chan struct{})
	entered := make(chan struct{})
	results := make(chan bridgev2.EventHandlingResult, 1)
	go func() {
		results <- br.QueueRemoteEvent(login, &simplevent.EventMeta{Type: bridgev2.RemoteEventUnknown, PortalKey: key, PreHandleFunc: func(context.Context, *bridgev2.Portal) { close(entered); <-release }})
	}()
	<-entered
	select {
	case result := <-results:
		close(release)
		t.Fatalf("returned before completion: %+v", result)
	default:
	}
	close(release)
	select {
	case result := <-results:
		if err := eventHandlingResultError("finished event", result); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("completed event did not return")
	}
}

func TestPrivateReadTransportDoesNotClearManualUnread(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/read_markers") {
			t.Errorf("unexpected read side effect: %s %s", r.Method, r.URL.Path)
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request["m.read.private"] != "$safe" || request["m.fully_read"] != "$safe" {
			t.Errorf("missing private markers: %+v", request)
		}
		if _, public := request["m.read"]; public {
			t.Error("source receipt exposed publicly")
		}
		if _, unread := request["marked_unread"]; unread {
			t.Error("manual unread overwritten")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "{}")
	}))
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, "@self:example.org", "test-token")
	if err != nil {
		t.Fatal(err)
	}
	as := appservice.Create()
	as.HomeserverDomain = "example.org"
	as.Registration = appservice.CreateRegistration()
	as.Registration.SenderLocalpart = "bot"
	as.DoublePuppetValue = "inline-test"
	if err := as.SetHomeserverURL(server.URL); err != nil {
		t.Fatal(err)
	}
	intent := as.NewIntentAPI("self")
	intent.Client = client
	intent.IsCustomPuppet = true
	puppet := &matrix.ASIntent{Matrix: intent}
	if err := writePrivateReadMarkers(t.Context(), puppet, "!room:example.org", "$safe"); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("receipt made %d writes", calls)
	}
}
func TestReadTimelineRequiresOriginAndHonorsBound(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"start":"%d","end":"%d","chunk":[{"event_id":"$%d","type":"m.room.message","content":{"msgtype":"m.text","body":"x"}}]}`, calls, calls+1, calls)
	}))
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, "@self:example.org", "test-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readTimeline(t.Context(), client, "!room:example.org"); err == nil {
		t.Fatal("partial timeline was treated as complete")
	}
	if calls != 20 {
		t.Fatalf("unbounded inspection: %d requests", calls)
	}
}

func TestReadTimelineContinuesAcrossEmptyIntermediatePage(t *testing.T) {
	var cursors []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("from")
		cursors = append(cursors, cursor)
		w.Header().Set("Content-Type", "application/json")
		switch cursor {
		case "":
			fmt.Fprint(w, `{"start":"latest","end":"gap","chunk":[{"event_id":"$new","type":"m.room.message","content":{"msgtype":"m.text","body":"new"}}]}`)
		case "gap":
			fmt.Fprint(w, `{"start":"gap","end":"old","chunk":[]}`)
		case "old":
			fmt.Fprint(w, `{"start":"old","chunk":[{"event_id":"$old","type":"m.room.message","content":{"msgtype":"m.text","body":"old"}}]}`)
		default:
			t.Errorf("unexpected timeline cursor %q", cursor)
			http.Error(w, "unexpected cursor", 400)
		}
	}))
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, "@self:example.org", "test-token")
	if err != nil {
		t.Fatal(err)
	}
	timeline, err := readTimeline(t.Context(), client, "!room:example.org")
	if err != nil {
		t.Fatal(err)
	}
	if len(cursors) != 3 || len(timeline) != 2 || timeline[0].ID != "$old" || timeline[1].ID != "$new" {
		t.Fatalf("empty page silently omitted older history: cursors=%v events=%v", cursors, timeline)
	}
}
