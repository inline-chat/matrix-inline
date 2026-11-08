package connector

import (
	"context"
	"errors"
	"strconv"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
)

type historicalPage struct {
	ChatID       int64
	Records      []sidecar.MessageRecord
	CatchupAfter *int64
}

// A completion hook can run after a fully deduplicated (or truncated) batch.
// Only actual non-fake mappings for every requested record prove delivery.
func persistedHistoryPrefix(key networkid.PortalKey, records []sidecar.MessageRecord, lookup func(networkid.MessageID) (*database.Message, error)) int64 {
	var maximum int64
	for _, record := range records {
		if record.ChatID <= 0 || record.MessageID <= 0 || makePortalID(strconv.FormatInt(record.ChatID, 10)) != key.ID {
			return 0
		}
		row, err := lookup(makeMessageID(record.ChatID, record.MessageID))
		if err != nil || row == nil || row.MXID == "" || row.HasFakeMXID() || row.Room != key {
			return 0
		}
		if record.MessageID > maximum {
			maximum = record.MessageID
		}
	}
	return maximum
}

func (ic *InlineClient) persistHistoricalMappings(ctx context.Context, portal *bridgev2.Portal, records []sidecar.MessageRecord) error {
	if portal == nil || ic.UserLogin == nil || ic.UserLogin.Bridge == nil {
		return errors.New("history mapping store unavailable")
	}
	maximum := persistedHistoryPrefix(portal.PortalKey, records, func(mid networkid.MessageID) (*database.Message, error) {
		return ic.UserLogin.Bridge.DB.Message.GetFirstPartByID(ctx, portal.Receiver, mid)
	})
	if len(records) > 0 && maximum == 0 {
		return errors.New("historical delivery awaits persisted Matrix mappings")
	}

	return nil
}

// Only an explicit source-ID catch-up request can advance forward coverage.
// A generic or cached page verifies delivery, not the absence of source gaps.
func (ic *InlineClient) advanceHistoricalCoverage(ctx context.Context, portal *bridgev2.Portal, page historicalPage) error {
	if err := ic.persistHistoricalMappings(ctx, portal, page.Records); err != nil {
		return err
	}
	if page.CatchupAfter == nil || len(page.Records) == 0 {
		return nil
	}
	chat, err := chatIDFromPortal(portal)
	if err != nil || chat != page.ChatID {
		return errors.New("historical catchup belongs to another chat")
	}
	maximum, err := cachedHistoryPageMax(page.Records, *page.CatchupAfter)
	if err != nil {
		return err
	}
	metadata, ok := portal.Metadata.(*PortalMetadata)
	if !ok || metadata == nil {
		return errors.New("historical coverage metadata unavailable")
	}
	current := metadata.HistoricalCheckpoint
	if current != *page.CatchupAfter {
		if maximum <= current {
			return nil
		} // Already verified coverage; never leap from a stale request.
		return errors.New("historical catchup frontier changed; refetch required")
	}
	if err = ic.persistHistoryCheckpoint(ctx, chat, maximum); err != nil {
		return err
	}
	ic.rememberHistoryDelivered(chat, maximum)
	return nil
}

// Read-state absence is unknown. It never permits implicit history mark-read.
// If authoritative intent exists, journal failure stops delivery before effects.
func (ic *InlineClient) readDesireBeforeDelivery(ctx context.Context, chat int64) error {
	state, err := ic.Sidecar.ChatState(ctx, sidecar.ChatStateRequest{ChatID: chat})
	if err != nil {
		return nil
	}
	if state.Dialog != nil {
		ic.rememberDialog(*state.Dialog)
	}
	if state.ReadState != nil {
		return ic.storeReadDesire(ctx, state.ReadState)
	}
	return nil
}

func (ic *InlineClient) queueHistoricalPage(ctx context.Context, chat int64, records []sidecar.MessageRecord, catchupAfter *int64) error {
	if len(records) == 0 {
		return nil
	}
	if err := ic.readDesireBeforeDelivery(ctx, chat); err != nil {
		return err
	}
	result := ic.UserLogin.Bridge.QueueRemoteEvent(ic.UserLogin, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: ic.portalKeyForChat(chat), CreatePortal: true, PostHandleFunc: func(ctx context.Context, p *bridgev2.Portal) {
			_ = ic.fillHistoricalHolesInLane(ctx, p, records)
			ic.projectionPostHandler(ctx, p)
		}},
		GetChatInfoFunc:        ic.migrationGatedInfoProvider(&bridgev2.ChatInfo{}),
		BundledBackfillData:    historicalPage{ChatID: chat, Records: records},
		CheckNeedsBackfillFunc: func(context.Context, *database.Message) (bool, error) { return true, nil },
	})
	if err := eventHandlingResultError("historical delivery", result); err != nil {
		return err
	}
	portal, err := ic.UserLogin.Bridge.GetExistingPortalByKey(ctx, ic.portalKeyForChat(chat))
	if err != nil {
		return err
	}
	return ic.advanceHistoricalCoverage(ctx, portal, historicalPage{ChatID: chat, Records: records, CatchupAfter: catchupAfter})
}

func pendingHistoricalRecords(key networkid.PortalKey, records []sidecar.MessageRecord, lookup func(networkid.MessageID) (*database.Message, error)) ([]sidecar.MessageRecord, error) {
	pending := make([]sidecar.MessageRecord, 0, len(records))
	seen := map[networkid.MessageID]bool{}
	for _, record := range records {
		if record.ChatID <= 0 || record.MessageID <= 0 || makePortalID(strconv.FormatInt(record.ChatID, 10)) != key.ID {
			return nil, errors.New("history record belongs to another chat")
		}
		mid := makeMessageID(record.ChatID, record.MessageID)
		if seen[mid] {
			continue
		}
		seen[mid] = true
		row, err := lookup(mid)
		if err != nil {
			return nil, err
		}
		if row != nil {
			if row.Room != key || row.MXID == "" || row.HasFakeMXID() {
				return nil, errors.New("history mapping is unverified")
			}
			continue
		}
		pending = append(pending, record)
	}
	return pending, nil
}

// The pinned high-level forward backfill cuts by display timestamp. Source-ID
// recovery must also deliver older-timestamp holes. Keep the deprecated exported
// seam in this bounded compatibility module, in the existing portal lane. It
// discards its error return, so only subsequent persisted mappings prove success.
func (ic *InlineClient) fillHistoricalHolesInLane(ctx context.Context, portal *bridgev2.Portal, records []sidecar.MessageRecord) error {
	if portal.MXID == "" {
		return errors.New("historical room not ready")
	}
	if !portal.Bridge.Config.Backfill.Enabled {
		return errors.New("history delivery requires backfill.enabled")
	}
	pending, err := pendingHistoricalRecords(portal.PortalKey, records, func(mid networkid.MessageID) (*database.Message, error) {
		return portal.Bridge.DB.Message.GetFirstPartByID(ctx, portal.Receiver, mid)
	})
	if err != nil {
		return err
	}
	if len(pending) > 0 {
		messages, _ := ic.backfillMessagesFromHistory(ctx, portal, pending)
		if len(messages) > 0 {
			portal.Internal().SendBackfill(ctx, ic.UserLogin, messages, true, false, false, nil)
		}
	}
	return ic.persistHistoricalMappings(ctx, portal, records)
}
