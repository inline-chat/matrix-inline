package connector

import (
	"encoding/json"
	"fmt"
	"hegel.dev/go/hegel"
	"math"
	"maunium.net/go/mautrix/id"
	"path/filepath"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

type frontierMatrix struct{ noPuppetMatrix }

func (frontierMatrix) ServerName() string            { return "test" }
func (frontierMatrix) Init(*bridgev2.Bridge)         {}
func (frontierMatrix) BotIntent() bridgev2.MatrixAPI { return nil }

func fixtureHistoryClient(t *testing.T, path string) *InlineClient {
	t.Helper()
	store := fixtureJournal(t, path)
	br := bridgev2.NewBridge("fixture", store.db, zerolog.Nop(), nil, frontierMatrix{}, &InlineConnector{}, func(*bridgev2.Bridge) bridgev2.CommandProcessor { return nil })
	br.BackgroundCtx = t.Context()
	if err := br.DB.Upgrade(t.Context()); err != nil {
		t.Fatal(err)
	}
	return &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login"}, Bridge: br}}
}

func TestHistoricalCheckpointIgnoresInterleavedLiveMaximumAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	ic := fixtureHistoryClient(t, path)
	var metadata PortalMetadata
	if err := json.Unmarshal([]byte(`{"history_checkpoint":21,"historical_checkpoint":10}`), &metadata); err != nil {
		t.Fatal(err)
	}
	portal := &database.Portal{PortalKey: ic.portalKeyForChat(7), Metadata: &metadata}
	if err := ic.UserLogin.Bridge.DB.Portal.Insert(t.Context(), portal); err != nil {
		t.Fatal(err)
	}
	restarted := fixtureHistoryClient(t, path)
	checkpoint, err := restarted.loadHistoryCheckpoint(t.Context(), 7)
	if err != nil || checkpoint != 10 {
		t.Fatalf("live maximum suppressed owed historical page: checkpoint%d err%v", checkpoint, err)
	}
	d := sidecar.DialogRecord{ChatID: 7, LastMessageID: foundationInt64(20)}
	if !restarted.needsHistoryDelivery(d) {
		t.Fatal("owed history lost after live21/restart")
	}
	request := historyRequestForDelivery(d, checkpoint, 100)
	if request.AfterMessageID == nil || *request.AfterMessageID != 10 {
		t.Fatal("history request skipped pending IDs11..20")
	}
}

func TestHistoricalFrontierRequiresEveryMappingDespiteLiveInterleaving(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		start := hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64))
		count := hegel.Draw(ht, hegel.Integers(1, math.MaxInt))
		// Bound the materialized fixture, retaining full scalar ID/size domains.
		if count > 12 {
			count = 12
		}
		if start > math.MaxInt64-int64(count)-1 {
			start = math.MaxInt64 - int64(count) - 1
		}
		liveFirst := hegel.Draw(ht, hegel.Booleans())
		staleRequest := hegel.Draw(ht, hegel.Booleans())
		missing := hegel.Draw(ht, hegel.Integers(0, count)) // count means complete page
		path := filepath.Join(t.TempDir(), fmt.Sprintf("case-%d-%d-%t-%d.db", start, count, liveFirst, missing))
		ic := fixtureHistoryClient(t, path)
		portal, err := ic.UserLogin.Bridge.GetPortalByKey(t.Context(), ic.portalKeyForChat(7))
		if err != nil {
			ht.Fatal(err)
		}
		if err = ic.persistHistoryCheckpoint(t.Context(), 7, start); err != nil {
			ht.Fatal(err)
		}
		live := start + int64(count) + 1
		if liveFirst {
			if err = ic.persistDeliveryCheckpoint(t.Context(), 7, live, false); err != nil {
				ht.Fatal(err)
			}
		}
		records := make([]sidecar.MessageRecord, count)
		for i := range count {
			n := start + int64(i) + 1
			records[i] = sidecar.MessageRecord{ChatID: 7, MessageID: n, Timestamp: int64(count - i)}
			if i == missing {
				continue
			}
			if err = ic.UserLogin.Bridge.DB.Message.Insert(t.Context(), &database.Message{ID: makeMessageID(7, n), MXID: id.EventID(fmt.Sprintf("$%d", n)), Room: portal.PortalKey}); err != nil {
				ht.Fatal(err)
			}
		}
		requestAfter := start
		if staleRequest {
			requestAfter--
		}
		err = ic.advanceHistoricalCoverage(t.Context(), portal, historicalPage{ChatID: 7, Records: records, CatchupAfter: &requestAfter})
		want := start + int64(count)
		if missing < count || staleRequest {
			want = start
			if err == nil {
				ht.Fatal("incomplete or stale page advanced frontier")
			}
		} else if err != nil {
			ht.Fatal(err)
		}
		if !liveFirst {
			if err = ic.persistDeliveryCheckpoint(t.Context(), 7, live, false); err != nil {
				ht.Fatal(err)
			}
		}
		restarted := fixtureHistoryClient(t, path)
		got, err := restarted.loadHistoryCheckpoint(t.Context(), 7)
		if err != nil || got != want {
			ht.Fatalf("historical frontier %d want%d err%v", got, want, err)
		}
		stored, err := restarted.UserLogin.Bridge.GetPortalByKey(t.Context(), ic.portalKeyForChat(7))
		if err != nil || stored.Metadata.(*PortalMetadata).HistoryCheckpoint != live {
			ht.Fatal("legacy live maximum changed", err)
		}
	}, hegel.WithTestCases(100), hegel.WithDatabase(t.TempDir()))
}

func TestHistoricalCoverageRejectsStaleRequestFrontierAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stale.db")
	ic := fixtureHistoryClient(t, path)
	portal, err := ic.UserLogin.Bridge.GetPortalByKey(t.Context(), ic.portalKeyForChat(7))
	if err != nil {
		t.Fatal(err)
	}
	if err = ic.persistHistoryCheckpoint(t.Context(), 7, 10); err != nil {
		t.Fatal(err)
	}
	// This response was requested after 20, an unrelated maximum. All selected
	// records are mapped, but none prove coverage of the owed gap 11..20.
	row := &database.Message{ID: "7/21", MXID: "$21", Room: portal.PortalKey}
	if err = ic.UserLogin.Bridge.DB.Message.Insert(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	stale := int64(20)
	if err = ic.advanceHistoricalCoverage(t.Context(), portal, historicalPage{ChatID: 7, Records: []sidecar.MessageRecord{{ChatID: 7, MessageID: 21}}, CatchupAfter: &stale}); err == nil {
		t.Fatal("unrelated forward frontier accepted")
	}
	restarted := fixtureHistoryClient(t, path)
	if got, err := restarted.loadHistoryCheckpoint(t.Context(), 7); err != nil || got != 10 {
		t.Fatalf("stale proof persisted %d %v", got, err)
	}
}

func TestHistoryRequestForDeliveryStartsAtExplicitZero(t *testing.T) {
	req := historyRequestForDelivery(sidecar.DialogRecord{ChatID: 7}, 0, 100)
	if req.AfterMessageID == nil || *req.AfterMessageID != 0 || req.BeforeMessageID != nil {
		t.Fatalf("empty frontier selected recent tail instead of oldest source IDs: %+v", req)
	}
}
