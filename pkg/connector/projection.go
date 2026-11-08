package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
)

const projectionVersion = 1

var errProjectionConflict = errors.New("presentation state changed while reconciling")

type projectionKey struct{ Bridge, User, Login, Receiver, Chat string }
type projectionState struct {
	Revision           int64                  `json:"revision"`
	Version            int                    `json:"version"`
	ReadMaxID          int64                  `json:"read_max_id,omitempty"`
	ManualUnread       bool                   `json:"manual_unread"`
	ReadPending        bool                   `json:"read_pending"`
	ReadApplied        int64                  `json:"read_applied,omitempty"`
	ReceiptObservation *receiptObservation    `json:"receipt_observation,omitempty"`
	ReceiptSyncToken   string                 `json:"receipt_sync_token,omitempty"`
	ReceiptTarget      id.EventID             `json:"receipt_target,omitempty"`
	ReadError          string                 `json:"read_error,omitempty"`
	UnreadPending      bool                   `json:"unread_pending"`
	DesiredParent      string                 `json:"desired_parent,omitempty"`
	ParentKnown        bool                   `json:"parent_known"`
	OwnedParents       []id.RoomID            `json:"owned_parents,omitempty"`
	ParentPending      bool                   `json:"parent_pending"`
	AppliedParent      string                 `json:"applied_parent,omitempty"`
	ParentError        string                 `json:"parent_error,omitempty"`
	NotificationPolicy string                 `json:"notification_policy,omitempty"`
	Migration          *presentationMigration `json:"migration,omitempty"`
}

// Receipt evidence belongs to one desired operation, never merely a source ID.
type receiptOperation struct {
	Key          projectionKey `json:"key"`
	Room         id.RoomID     `json:"room"`
	User         id.UserID     `json:"user"`
	Target       id.EventID    `json:"target"`
	ReadMaxID    int64         `json:"read_max_id"`
	ManualUnread bool          `json:"manual_unread"`
}
type receiptObservation struct {
	Operation receiptOperation `json:"operation"`
	SyncToken string           `json:"sync_token"`
}

type projectionStore struct{ db *dbutil.Database }

func (s *projectionStore) init(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `CREATE TABLE IF NOT EXISTS inline_projection (
 bridge TEXT NOT NULL, matrix_user TEXT NOT NULL, login TEXT NOT NULL,
 receiver TEXT NOT NULL, chat TEXT NOT NULL, revision BIGINT NOT NULL, state TEXT NOT NULL,
 PRIMARY KEY (bridge,matrix_user,login,receiver,chat))`)
	return err
}
func (s *projectionStore) load(ctx context.Context, k projectionKey) (projectionState, error) {
	var raw string
	err := s.db.QueryRow(ctx, `SELECT state FROM inline_projection WHERE bridge=$1 AND matrix_user=$2 AND login=$3 AND receiver=$4 AND chat=$5`, k.Bridge, k.User, k.Login, k.Receiver, k.Chat).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return projectionState{Version: projectionVersion}, nil
	}
	if err != nil {
		return projectionState{}, err
	}
	var p projectionState
	err = json.Unmarshal([]byte(raw), &p)
	if err == nil && p.Version != projectionVersion {
		err = errors.New("unsupported presentation journal version")
	}
	return p, err
}
func (s *projectionStore) save(ctx context.Context, k projectionKey, p *projectionState) error {
	next := *p
	next.Revision++
	raw, err := json.Marshal(next)
	if err != nil {
		return err
	}
	args := []any{k.Bridge, k.User, k.Login, k.Receiver, k.Chat, next.Revision, string(raw)}
	var result sql.Result
	if p.Revision == 0 {
		result, err = s.db.Exec(ctx, `INSERT INTO inline_projection (bridge,matrix_user,login,receiver,chat,revision,state) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, args...)
	} else {
		args = append(args, p.Revision)
		result, err = s.db.Exec(ctx, `UPDATE inline_projection SET revision=$6,state=$7 WHERE bridge=$1 AND matrix_user=$2 AND login=$3 AND receiver=$4 AND chat=$5 AND revision=$8`, args...)
	}
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errProjectionConflict
	}
	*p = next
	return nil
}
func (ic *InlineClient) projectionJournal(ctx context.Context, chat string) (*projectionStore, projectionKey, error) {
	if ic.UserLogin == nil || ic.UserLogin.Bridge == nil || ic.UserLogin.Bridge.DB == nil || ic.UserLogin.User == nil {
		return nil, projectionKey{}, errors.New("presentation journal unavailable")
	}
	login := ic.UserLogin
	key := projectionKey{Bridge: string(login.Bridge.DB.BridgeID), User: string(login.User.MXID), Login: string(login.ID), Receiver: string(ic.portalKeyForChat(1).Receiver), Chat: chat}
	s := &projectionStore{db: login.Bridge.DB.Database}
	return s, key, s.init(ctx)
}

// Persist source intent before attempting any Matrix effects. A separate revision
// guards completion against a source update arriving during an HTTP request.
func (ic *InlineClient) storeReadDesire(ctx context.Context, read *sidecar.ReadStateRecord) error {
	if read.ChatID <= 0 {
		return errors.New("invalid read-state chat")
	}
	s, k, err := ic.projectionJournal(ctx, fmt.Sprint(read.ChatID))
	if err != nil {
		return err
	}
	for range 5 {
		p, err := s.load(ctx, k)
		if err != nil {
			return err
		}
		cursor := int64(0)
		if read.ReadMaxID != nil && *read.ReadMaxID > 0 {
			cursor = *read.ReadMaxID
		}
		if p.ReadMaxID != cursor || p.ManualUnread != read.MarkedUnread {
			p.ReceiptObservation = nil
		}
		p.ReadMaxID = cursor
		p.ManualUnread = read.MarkedUnread
		p.ReadPending = true
		p.UnreadPending = true
		p.ReadError = "awaiting_persisted_mapping"
		if err = s.save(ctx, k, &p); !errors.Is(err, errProjectionConflict) {
			return err
		}
	}
	return errProjectionConflict
}

type receiptEvent struct {
	EventID  id.EventID
	SourceID int64
}

// The actual target timeline must be a source-read prefix. IDs and display
// timestamps alone cannot establish this, especially after backward imports.
type receiptPlan struct {
	Target     id.EventID
	CoveredMax int64
	Complete   bool
}

func receiptPlanForOrder(order []receiptEvent, cursor int64) receiptPlan {
	var plan receiptPlan
	barrier, blockedRead := false, false
	for _, e := range order {
		read := e.SourceID > 0 && e.SourceID <= cursor && e.EventID != ""
		if !read {
			barrier = true
			continue
		}
		if barrier {
			blockedRead = true
			continue
		}
		plan.Target = e.EventID
		if e.SourceID > plan.CoveredMax {
			plan.CoveredMax = e.SourceID
		}
	}
	plan.Complete = plan.CoveredMax >= cursor && cursor > 0 && !blockedRead
	return plan
}
func safeReceiptPrefix(order []receiptEvent, cursor int64) id.EventID {
	return receiptPlanForOrder(order, cursor).Target
}

func (ic *InlineClient) projectionPostHandler(ctx context.Context, p *bridgev2.Portal) {
	if p != nil {
		if chat, err := chatIDFromPortal(p); err == nil {
			if d, ok := ic.cachedDialog(chat); ok {
				// This callback is already in the portal lane. Only retry a
				// durable pending topic here; do not enqueue recursively or
				// read Matrix state for every otherwise healthy message.
				if meta, ok := p.Metadata.(*PortalMetadata); ok && meta != nil && meta.InlineTopic != nil && !meta.InlineTopic.UserOwned && meta.InlineTopic.Pending != nil && p.MXID != "" && ic.topicProjectionEligible(d) && ic.reserveTopicRetry(chat) {
					bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
					if err := ic.reconcileSourceTopic(bounded, p, d); err != nil {
						zerolog.Ctx(ctx).Warn().Err(err).Msg("Source topic projection remains pending")
					}
					cancel()
					if meta.InlineTopic == nil || meta.InlineTopic.Pending == nil {
						ic.clearTopicRetry(chat)
					}
				}
				ic.projectDialogInLane(ctx, p, d)
				return
			}
		}
	}
	ic.reconcileProjection(ctx, p)
}

// Limit retries for busy rooms. Idle rooms still require a later portal event.
func (ic *InlineClient) reserveTopicRetry(chat int64) bool {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	if ic.topicRetryAfter == nil {
		ic.topicRetryAfter = make(map[int64]time.Time)
	}
	now := time.Now()
	if now.Before(ic.topicRetryAfter[chat]) {
		return false
	}
	ic.topicRetryAfter[chat] = now.Add(30 * time.Second)
	return true
}

func (ic *InlineClient) clearTopicRetry(chat int64) {
	ic.mu.Lock()
	delete(ic.topicRetryAfter, chat)
	ic.mu.Unlock()
}
func (ic *InlineClient) reconcileProjection(ctx context.Context, portal *bridgev2.Portal) {
	if portal == nil || portal.MXID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	s, k, err := ic.projectionJournal(ctx, string(portal.ID))
	if err != nil {
		return
	}
	p, err := s.load(ctx, k)
	if err != nil || p.ReadMaxID <= 0 {
		return
	}
	// Even an applied source cursor needs a new target when a late source-read
	// message is delivered. Reinspect order; avoid rewriting an unchanged target.
	p.ReadPending = true
	p.ReadError = "double_puppet_unavailable"
	if ic.UserLogin.User == nil {
		return
	}
	raw, ok := ic.UserLogin.User.DoublePuppet(ctx).(*matrix.ASIntent)
	if ok && raw != nil && raw.Matrix != nil && raw.IsDoublePuppet() && p.ReadMaxID > 0 {
		plan, err := ic.receiptTarget(ctx, portal, raw, p.ReadMaxID)
		if err != nil {
			p.ReadError = "read_order_unverified"
		} else if plan.Target == "" {
			p.ReadError = "awaiting_safe_prefix"
		} else if plan.Target == p.ReceiptTarget && plan.CoveredMax == p.ReadApplied {
			p.ReadPending = !plan.Complete
			p.ReadError = ""
			if p.ReadPending {
				p.ReadError = "awaiting_safe_prefix"
			}
		} else {
			operation := receiptOperation{Key: k, Room: portal.MXID, User: raw.GetMXID(), Target: plan.Target, ReadMaxID: p.ReadMaxID, ManualUnread: p.ManualUnread}
			unread, err := applyPrivateReceiptWithEvidence(ctx, raw.Matrix.Client, operation, &p.ReceiptSyncToken, &p.ReceiptObservation)
			if err != nil {
				p.ReadError = "private_receipt_write_or_readback_failed"
				var stage *privateReceiptError
				if errors.As(err, &stage) {
					p.ReadError += ":" + stage.stage
				}
			} else {
				p.ReceiptObservation = nil
				p.ReadApplied = plan.CoveredMax
				p.ReceiptTarget = plan.Target
				p.ReadPending = !plan.Complete
				p.ReadError = ""
				if p.ReadPending {
					p.ReadError = "awaiting_safe_prefix"
				}
				if unread != nil && *unread == p.ManualUnread {
					p.UnreadPending = false
				} else {
					p.UnreadPending = true
					p.ReadError = "manual_unread_origin_unverified"
				}
			}

		}
	}
	_ = s.save(ctx, k, &p) // A stale revision intentionally leaves newer desire pending.
}

func (ic *InlineClient) receiptTarget(ctx context.Context, p *bridgev2.Portal, raw *matrix.ASIntent, cursor int64) (receiptPlan, error) {
	chat, err := chatIDFromPortal(p)
	if err != nil {
		return receiptPlan{}, err
	}
	var reverse []receiptEvent
	from := ""
	for range 20 { // 10k timeline events, then conservatively defer.
		page, err := raw.Matrix.Client.Messages(ctx, p.MXID, from, "", mautrix.DirectionBackward, nil, 500)
		if err != nil {
			return receiptPlan{}, err
		}
		for _, evt := range page.Chunk {
			// State changes are receipt-neutral. Every non-state timeline event
			// needs a source mapping: sticker, poll and future content types can
			// carry unread state just like text or encrypted messages.
			if evt.StateKey != nil || evt.Type.IsState() {
				continue
			}
			record, err := ic.UserLogin.Bridge.DB.Message.GetPartByMXID(ctx, evt.ID)
			if err != nil {
				return receiptPlan{}, err
			}
			n := int64(0)
			if record != nil && record.Room == p.PortalKey {
				sourceChat, id, err := parseMessageID(record.ID)
				if err == nil && sourceChat == chat {
					n = id
				}
			}
			reverse = append(reverse, receiptEvent{EventID: evt.ID, SourceID: n})
		}
		if page.End == "" || len(page.Chunk) == 0 {
			slices.Reverse(reverse)
			return receiptPlanForOrder(reverse, cursor), nil
		}
		if page.End == from {
			break
		}
		from = page.End
	}
	return receiptPlan{}, errors.New("timeline order exceeds bounded inspection")
}

// Every origin-sensitive local preference write remains disabled. Source modes
// stay cached independently; actual Matrix user changes still go to Inline.
const notificationPolicyDegraded = "notification_policy_degraded: origin attribution unverified; mute channels manually"

// Keep imported history from being auto-read by the framework's age threshold.
func disableImplicitHistoryRead(br *bridgev2.Bridge) { br.Config.Backfill.UnreadHoursThreshold = 0 }

func (ic *InlineClient) projectionStatus(ctx context.Context) string {
	s, k, err := ic.projectionJournal(ctx, "")
	if err != nil {
		return "- Presentation journal: unavailable\n"
	}
	rows, err := s.db.Query(ctx, `SELECT state FROM inline_projection WHERE bridge=$1 AND matrix_user=$2 AND login=$3 AND receiver=$4`, k.Bridge, k.User, k.Login, k.Receiver)
	if err != nil {
		return "- Presentation journal: unavailable\n"
	}
	defer rows.Close()
	var total, reads, unread, parents, migrations int
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return "- Presentation journal: unavailable\n"
		}
		var p projectionState
		if json.Unmarshal([]byte(raw), &p) != nil {
			return "- Presentation journal: unavailable\n"
		}
		total++
		if p.ReadPending {
			reads++
		}
		if p.UnreadPending {
			unread++
		}
		if p.ParentPending {
			parents++
		}
		if p.Migration != nil && p.Migration.Phase != "applied" && p.Migration.Phase != "rolled_back" {
			migrations++
		}
	}
	if rows.Err() != nil {
		return "- Presentation journal: unavailable\n"
	}
	ic.mu.RLock()
	sequence := ic.lastSidecarSequence
	var ambiguous, unknownWorkspaces, historyPending int
	for _, d := range ic.dialogs {
		if classifyDialog(d) == roomUnknown {
			ambiguous++
		}
		if d.Structure != nil && d.Structure.SpaceID != nil {
			access := ic.spaceAccess[*d.Structure.SpaceID]
			if access != "accessible" && access != "removed" && !ic.spaceAccessComplete {
				unknownWorkspaces++
			}
		}
		if _, loaded := ic.historyLoaded[d.ChatID]; loaded && d.LastMessageID != nil && *d.LastMessageID > ic.history[d.ChatID] {
			historyPending++
		}
	}
	ic.mu.RUnlock()
	backfillEnabled := ic.UserLogin.Bridge.Config != nil && ic.UserLogin.Bridge.Config.Backfill.Enabled
	return fmt.Sprintf("- Presentation v%d: %d rooms; pending receipts %d, manual unread %d, hierarchy %d, migration %d; ambiguous %d, unverified workspace access %d\n- Historical pages awaiting coverage: %d; backfill enabled: %t\n- Migration: controlled window required; remote state guards are not atomic\n- Last persisted/acknowledgeable sequence: %d\n- Read fidelity: actual Matrix prefix only; unverified targets remain pending\n", projectionVersion, total, reads, unread, parents, migrations, ambiguous, unknownWorkspaces, historyPending, backfillEnabled, sequence)
}

// applyPrivateReceipt never emits public m.read. The pinned v0.28 framework
// ignores m.read.private, so an unmarked server echo cannot change Inline.
// It verifies the private receipt itself and that manual-unread was untouched.
type privateReceiptError struct {
	stage string
	cause error
}

func (e *privateReceiptError) Error() string {
	return "private receipt " + e.stage + ": " + e.cause.Error()
}
func (e *privateReceiptError) Unwrap() error { return e.cause }

func applyPrivateReceipt(ctx context.Context, cli *mautrix.Client, room id.RoomID, user id.UserID, target id.EventID, cursors ...*string) (*bool, error) {
	var cursor *string
	if len(cursors) > 0 {
		cursor = cursors[0]
	}
	var evidence *receiptObservation
	return applyPrivateReceiptWithEvidence(ctx, cli, receiptOperation{Room: room, User: user, Target: target}, cursor, &evidence)
}

// An incremental sync can consume the only receipt event before a later GET
// fails. Keep its exact operation and watermark with the advanced cursor so an
// idempotent retry does not depend on the homeserver emitting it again.
func applyPrivateReceiptWithEvidence(ctx context.Context, cli *mautrix.Client, operation receiptOperation, cursorState *string, evidence **receiptObservation) (unread *bool, failure error) {
	room, target := operation.Room, operation.Target
	if *evidence != nil && (*evidence).Operation != operation {
		*evidence = nil
	}
	stage := "manual_unread_before"
	defer func() {
		if failure != nil {
			failure = &privateReceiptError{stage: stage, cause: failure}
		}
	}()
	before, err := readManualUnread(ctx, cli, room)
	if err != nil {
		return nil, err
	}
	filter, _ := json.Marshal(map[string]any{"room": map[string]any{"rooms": []id.RoomID{room}, "timeline": map[string]any{"limit": 1}, "ephemeral": map[string]any{"types": []string{"m.receipt"}}, "state": map[string]any{"types": []string{}}, "account_data": map[string]any{"types": []string{}}}, "presence": map[string]any{"types": []string{}}, "account_data": map[string]any{"types": []string{}}})
	// Synapse may cache an initial sync response. Capture a pre-write stream
	// position and read the receipt incrementally so a later target is visible.
	stage = "sync_before"
	cursor := ""
	if cursorState != nil {
		cursor = *cursorState
	}
	observe := func(sync *mautrix.RespSync) error {
		if cursorState != nil && sync.NextBatch != "" {
			*cursorState = sync.NextBatch
		}
		return observePrivateReceipt(sync, operation, evidence)
	}
	beforeSync, err := cli.SyncRequest(ctx, 0, cursor, string(filter), false, event.PresenceOffline)
	if err != nil {
		return nil, err
	}
	if err = observe(beforeSync); err != nil {
		return nil, err
	}
	if beforeSync.NextBatch == "" {
		return nil, errors.New("private receipt pre-write sync watermark missing")
	}
	stage = "write_markers"
	if err = cli.SetReadMarkers(ctx, room, &mautrix.ReqSetReadMarkers{ReadPrivate: target, FullyRead: target}); err != nil {
		return nil, err
	}
	var fullyRead struct {
		EventID id.EventID `json:"event_id"`
	}
	stage = "fully_read_readback"
	if err = cli.GetRoomAccountData(ctx, room, event.AccountDataFullyRead.Type, &fullyRead); err != nil {
		return nil, err
	}
	if fullyRead.EventID != target {
		return nil, errors.New("fully-read readback differs")
	}
	stage = "manual_unread_after"
	after, err := readManualUnread(ctx, cli, room)
	if err != nil {
		return nil, err
	}
	if (before == nil) != (after == nil) || (before != nil && *before != *after) {
		return nil, errors.New("manual unread changed during receipt reconciliation")
	}
	stage = "sync_after"
	sync, err := cli.SyncRequest(ctx, 0, beforeSync.NextBatch, string(filter), false, event.PresenceOffline)
	if err != nil {
		return nil, err
	}
	if err = observe(sync); err != nil {
		return nil, err
	}
	stage = "private_receipt_missing"
	if *evidence != nil && (*evidence).Operation == operation && (*evidence).SyncToken != "" {
		return after, nil
	}
	return nil, errors.New("private receipt readback missing")
}

func observePrivateReceipt(sync *mautrix.RespSync, operation receiptOperation, evidence **receiptObservation) error {
	joined := sync.Rooms.Join[operation.Room]
	if joined == nil {
		return nil
	}
	for _, evt := range joined.Ephemeral.Events {
		if evt.Type != event.EphemeralEventReceipt {
			continue
		}
		encoded, err := evt.Content.MarshalJSON()
		if err != nil {
			*evidence = nil
			return err
		}
		var receipts event.ReceiptEventContent
		if err = json.Unmarshal(encoded, &receipts); err != nil {
			*evidence = nil
			return err
		}
		found, conflicting := false, false
		for target, byType := range receipts {
			receipt, ok := byType[event.ReceiptTypeReadPrivate][operation.User]
			// SetReadMarkers writes an unthreaded receipt. A thread-local
			// receipt does not establish the room-wide operation's target.
			if !ok || receipt.ThreadID != "" {
				continue
			}
			if target == operation.Target {
				found = true
			} else {
				conflicting = true
			}
		}
		// Conflicting targets in one event have no usable ordering.
		if conflicting {
			*evidence = nil
		} else if found && sync.NextBatch != "" {
			*evidence = &receiptObservation{Operation: operation, SyncToken: sync.NextBatch}
		}
	}
	return nil
}

func readManualUnread(ctx context.Context, cli *mautrix.Client, room id.RoomID) (*bool, error) {
	var content event.MarkedUnreadEventContent
	if err := cli.GetRoomAccountData(ctx, room, event.AccountDataMarkedUnread.Type, &content); err != nil {
		if errors.Is(err, mautrix.MNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &content.Unread, nil
}
