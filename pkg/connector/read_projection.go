package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Use the bridge's existing durable KV table, with errors propagated. The
// framework's convenience KV helpers only log write failures.
func (ic *InlineClient) readProjectionKey(chatID int64) string {
	namespace, _, _ := ic.sidecarEventCursor()
	key, _ := json.Marshal([]any{"inline_read_v1", string(ic.UserLogin.ID), namespace, chatID})
	return string(key)
}

func (ic *InlineClient) saveReadIntent(ctx context.Context, state *sidecar.ReadStateRecord) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = ic.UserLogin.Bridge.DB.Exec(ctx, `INSERT INTO kv_store (bridge_id, key, value) VALUES ($1, $2, $3) ON CONFLICT (bridge_id, key) DO UPDATE SET value=$3`, ic.UserLogin.Bridge.ID, ic.readProjectionKey(state.ChatID), string(data))
	return err
}

func (ic *InlineClient) loadReadIntent(ctx context.Context, chatID int64) (*sidecar.ReadStateRecord, error) {
	var data string
	err := ic.UserLogin.Bridge.DB.QueryRow(ctx, `SELECT value FROM kv_store WHERE bridge_id=$1 AND key=$2`, ic.UserLogin.Bridge.ID, ic.readProjectionKey(chatID)).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var state sidecar.ReadStateRecord
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// A read intent is durable before the lossless source event is acknowledged.
// Failed/unsupported projections remain retryable on the regular dialog scan.
func (ic *InlineClient) queueReadState(state *sidecar.ReadStateRecord) error {
	if state == nil {
		return nil
	}
	if state.ChatID <= 0 {
		return errors.New("invalid read-state chat")
	}
	ctx := ic.UserLogin.Bridge.BackgroundCtx
	ic.readProjectionMu.Lock()
	defer ic.readProjectionMu.Unlock()
	if err := ic.saveReadIntent(ctx, state); err != nil {
		return fmt.Errorf("persist Inline read intent: %w", err)
	}
	ic.tryReadProjection(ctx, state)
	return nil
}

func (ic *InlineClient) retryReadProjection(ctx context.Context, chatID int64) {
	// The source consumer and periodic scan share a lock so an old intent cannot
	// finish after a newer one. Network operations below have a short deadline.
	ic.readProjectionMu.Lock()
	defer ic.readProjectionMu.Unlock()
	if ic.readProjectionAttempts == nil {
		ic.readProjectionAttempts = make(map[int64]time.Time)
	}
	if time.Since(ic.readProjectionAttempts[chatID]) < time.Minute {
		return
	}
	state, err := ic.loadReadIntent(ctx, chatID)
	if err != nil {
		ic.UserLogin.Log.Warn().Err(err).Msg("Unable to load pending read projection")
		return
	}
	if state != nil {
		ic.tryReadProjection(ctx, state)
	}
}

func (ic *InlineClient) tryReadProjection(ctx context.Context, state *sidecar.ReadStateRecord) {
	if ic.readProjectionAttempts == nil {
		ic.readProjectionAttempts = make(map[int64]time.Time)
	}
	ic.readProjectionAttempts[state.ChatID] = time.Now()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ic.projectReadState(ctx, state); err != nil {
		ic.UserLogin.Log.Debug().Err(err).Int64("chat_id", state.ChatID).Msg("Inline read projection remains pending")
	}
}

func (ic *InlineClient) projectReadState(ctx context.Context, state *sidecar.ReadStateRecord) error {
	// Native marked-unread is not allowed to overwrite a later Matrix user choice.
	// Nor does writing a receipt clear the Matrix marked-unread account data.
	if state.MarkedUnread || state.ReadMaxID == nil || *state.ReadMaxID <= 0 {
		return nil
	}
	if ic.UserLogin.User == nil {
		return errors.New("read projection needs a double puppet")
	}
	puppet, ok := ic.UserLogin.User.DoublePuppet(ctx).(*matrix.ASIntent)
	if !ok || puppet == nil || !puppet.IsDoublePuppet() {
		return errors.New("read projection needs supported double-puppet transport")
	}
	portal, err := ic.UserLogin.Bridge.GetExistingPortalByKey(ctx, ic.portalKeyForChat(state.ChatID))
	if err != nil {
		return err
	}
	if portal == nil || portal.MXID == "" {
		return errors.New("read projection awaits room creation")
	}
	timeline, err := readTimeline(ctx, puppet.Matrix.Client, portal.MXID)
	if err != nil {
		return err
	}
	target, err := verifiedReadPrefix(ctx, timeline, portal.PortalKey, *state.ReadMaxID, ic.UserLogin.Bridge.DB.Message.GetPartByMXID)
	if err != nil || target == "" {
		return err
	}
	return writePrivateReadMarkers(ctx, puppet, portal.MXID, target)
}

func writePrivateReadMarkers(ctx context.Context, puppet *matrix.ASIntent, room id.RoomID, target id.EventID) error {
	extra := map[string]any{}
	puppet.Matrix.AddDoublePuppetValue(extra)
	// Bypass ASIntent.MarkRead: it sends a public receipt and can also clear
	// marked-unread. These private markers leave the user's unread flag alone.
	return puppet.Matrix.SetReadMarkers(ctx, room, &mautrix.ReqSetReadMarkers{
		ReadPrivate: target, FullyRead: target,
		BeeperReadPrivateExtra: extra, BeeperFullyReadExtra: extra,
	})
}

func (ic *InlineClient) verifySourceReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt, chatID, messageID int64) error {
	if msg.EventID == "" || msg.ExactMessage.MXID != msg.EventID || msg.ExactMessage.HasFakeMXID() {
		return errors.New("source read requires an explicit real Matrix event")
	}
	if ic.UserLogin.Bridge == nil || ic.UserLogin.Bridge.DB == nil {
		return errors.New("source read coverage is unavailable")
	}
	coverage, err := ic.loadHistoryCheckpoint(ctx, chatID)
	if err != nil {
		return err
	}
	if coverage < messageID {
		return errors.New("source read awaits completed history delivery")
	}
	intent, ok := ic.UserLogin.Bridge.Bot.(*matrix.ASIntent)
	if !ok || intent == nil || intent.Matrix == nil || intent.Matrix.Client == nil {
		return errors.New("source read requires supported timeline inspection")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	timeline, err := readTimeline(ctx, intent.Matrix.Client, msg.Portal.MXID)
	if err != nil {
		return fmt.Errorf("inspect Matrix source read order: %w", err)
	}
	safe, err := verifiedSourceReadReceipt(ctx, timeline, msg.Portal.PortalKey, msg.EventID, messageID, ic.UserLogin.Bridge.DB.Message.GetPartByMXID)
	if err != nil {
		return err
	}
	if !safe {
		return errors.New("source read would cross an unverified or later Matrix message")
	}
	return nil
}

// A source receipt reads every source ID up to its target. All of those messages
// must therefore already be delivered, and none may occur after the explicitly
// read Matrix event. Timestamp and source-ID order cannot prove Matrix order.
func verifiedSourceReadReceipt(ctx context.Context, timeline []*event.Event, key networkid.PortalKey, target id.EventID, readMax int64, lookup func(context.Context, id.EventID) (*database.Message, error)) (bool, error) {
	found := false
	seenSenders := make(map[id.EventID]id.UserID)
	for _, evt := range timeline {
		if evt == nil {
			return false, nil
		}
		if evt.StateKey != nil || evt.Type == event.EventReaction || evt.Type == event.EventRedaction {
			continue
		}
		mapped, err := lookup(ctx, evt.ID)
		if err != nil {
			return false, err
		}
		if mapped == nil {
			transparent, err := verifiedTimelineEdit(ctx, evt, key, seenSenders, lookup)
			if err != nil {
				return false, err
			}
			if transparent {
				continue
			}
		}
		if mapped == nil || mapped.Room != key || mapped.MXID != evt.ID || mapped.HasFakeMXID() {
			return false, nil
		}
		chatID, messageID, err := parseMessageID(mapped.ID)
		if err != nil || key.ID != networkid.PortalID(fmt.Sprint(chatID)) || messageID <= 0 {
			return false, nil
		}
		seenSenders[evt.ID] = evt.Sender
		if evt.ID == target {
			if found || messageID != readMax {
				return false, nil
			}
			found = true
		} else if found && messageID <= readMax {
			return false, nil
		}
	}
	return found, nil
}

// An authenticated plaintext replacement from the original Matrix sender does
// not introduce another source message. Its original must already have appeared
// in this inspected timeline with a real mapping for this portal. Edits are only
// transparent to traversal; they never become receipt targets themselves.
func verifiedTimelineEdit(ctx context.Context, evt *event.Event, key networkid.PortalKey, seenSenders map[id.EventID]id.UserID, lookup func(context.Context, id.EventID) (*database.Message, error)) (bool, error) {
	if evt.Type != event.EventMessage || evt.Sender == "" {
		return false, nil
	}
	if evt.Content.Parsed == nil {
		if err := evt.Content.ParseRaw(evt.Type); err != nil {
			return false, nil
		}
	}
	content := evt.Content.AsMessage()
	relation := content.OptionalGetRelatesTo()
	if content.NewContent == nil || relation == nil || relation.Type != event.RelReplace || relation.EventID == evt.ID || seenSenders[relation.EventID] != evt.Sender {
		return false, nil
	}
	mapped, err := lookup(ctx, relation.EventID)
	if err != nil {
		return false, err
	}
	if mapped == nil || mapped.Room != key || mapped.MXID != relation.EventID || mapped.HasFakeMXID() || (mapped.SenderMXID != "" && mapped.SenderMXID != evt.Sender) {
		return false, nil
	}
	chatID, messageID, err := parseMessageID(mapped.ID)
	return err == nil && messageID > 0 && key.ID == networkid.PortalID(fmt.Sprint(chatID)), nil
}

// Read an entire bounded timeline before proving a prefix. Source IDs and
// timestamps are not Matrix order; newer delivery can insert older source IDs.
func readTimeline(ctx context.Context, client *mautrix.Client, room id.RoomID) ([]*event.Event, error) {
	var reverse []*event.Event
	cursor := ""
	for page := 0; page < 20; page++ {
		response, err := client.Messages(ctx, room, cursor, "", mautrix.DirectionBackward, nil, 500)
		if err != nil {
			return nil, err
		}
		reverse = append(reverse, response.Chunk...)
		if len(reverse) > 10000 {
			return nil, errors.New("read timeline inspection limit reached")
		}
		// Matrix may return an empty intermediate page. Only an absent end
		// token proves that there is no earlier visible history to inspect.
		if response.End == "" {
			for i, j := 0, len(reverse)-1; i < j; i, j = i+1, j-1 {
				reverse[i], reverse[j] = reverse[j], reverse[i]
			}
			return reverse, nil
		}
		if response.End == cursor {
			return nil, errors.New("read timeline cursor did not advance")
		}
		cursor = response.End
	}
	return nil, errors.New("read timeline inspection limit reached")
}

func verifiedReadPrefix(ctx context.Context, timeline []*event.Event, key networkid.PortalKey, readMax int64, lookup func(context.Context, id.EventID) (*database.Message, error)) (id.EventID, error) {
	var target id.EventID
	seenSenders := make(map[id.EventID]id.UserID)
	for _, evt := range timeline {
		if evt == nil {
			return target, nil
		}
		if evt.StateKey != nil || evt.Type == event.EventReaction || evt.Type == event.EventRedaction {
			continue
		}
		// Unknown non-state events are barriers too, including polls and stickers.
		mapped, err := lookup(ctx, evt.ID)
		if err != nil {
			return "", err
		}
		if mapped == nil {
			transparent, err := verifiedTimelineEdit(ctx, evt, key, seenSenders, lookup)
			if err != nil {
				return "", err
			}
			if transparent {
				continue
			}
		}
		if mapped == nil || mapped.Room != key || mapped.HasFakeMXID() {
			break
		}
		chatID, messageID, err := parseMessageID(mapped.ID)
		if err != nil || key.ID != networkid.PortalID(fmt.Sprint(chatID)) || messageID > readMax {
			break
		}
		seenSenders[evt.ID] = evt.Sender
		target = evt.ID
	}
	return target, nil
}
