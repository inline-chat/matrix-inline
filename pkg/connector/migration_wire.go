package connector

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strings"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// Matrix state has no multi-event transaction. A persisted per-field guard
// allows our own partial effects after a crash, while refusing a third value.
func migrationValuesCompatible(current, before, after presentationValue) bool {
	return current.RoomID == before.RoomID && current.RoomID == after.RoomID &&
		(current.Kind == before.Kind || current.Kind == after.Kind) &&
		(current.Parent == before.Parent || current.Parent == after.Parent)
}
func migrationWireCompatible(current, before, after map[string]string) bool {
	if len(before) == 0 || len(current) != len(before) || len(after) != len(before) {
		return false
	}
	for k, v := range current {
		b, bok := before[k]
		a, aok := after[k]
		if !bok || !aok || (v != b && v != a) {
			return false
		}
	}
	return true
}
func migrationJSON(value any) string {
	raw, _ := json.Marshal(value)
	var canonical any
	_ = json.Unmarshal(raw, &canonical)
	raw, _ = json.Marshal(canonical)
	return string(raw)
}
func migrationLink(content *event.Content) (string, error) {
	raw, err := content.MarshalJSON()
	if err != nil {
		return "", err
	}
	var fields map[string]any
	if err = json.Unmarshal(raw, &fields); err != nil {
		return "", err
	}
	// Missing links and explicit empty tombstones have the same meaning.
	if len(fields) == 0 {
		return "{}", nil
	}
	return migrationJSON(fields), nil
}
func migrationWireSnapshot(ctx context.Context, cli *mautrix.Client, portal *bridgev2.Portal, parents []id.RoomID) (map[string]string, error) {
	state, err := cli.State(ctx, portal.MXID)
	if err != nil {
		return nil, err
	}
	values := make(map[string]string)
	for _, typ := range []event.Type{event.StateBridge, event.StateHalfShotBridge} {
		found := false
		for _, evt := range state[typ] {
			var info event.BridgeEventContent
			raw, err := evt.Content.MarshalJSON()
			if err != nil {
				return nil, err
			}
			if err = json.Unmarshal(raw, &info); err != nil {
				return nil, err
			}
			if info.Channel.ID != string(portal.ID) || info.Channel.Receiver != string(portal.Receiver) {
				continue
			}
			if found {
				return nil, errors.New("ambiguous bridge info")
			}
			found = true
			prefix := "bridge:" + typ.Type + ":"
			values[prefix+"kind"] = info.BeeperRoomTypeV2
			values[prefix+"legacy"] = info.BeeperRoomType
			values[prefix+"parent"] = ""
			if info.Network != nil {
				values[prefix+"parent"] = info.Network.ID
			}
		}
		if !found {
			return nil, errors.New("bridge info unavailable")
		}
	}
	for _, parent := range parents {
		values["parent:"+string(parent)] = "{}"
	}
	for key, evt := range state[event.StateSpaceParent] {
		value, err := migrationLink(&evt.Content)
		if err != nil {
			return nil, err
		}
		if value != "{}" {
			values["parent:"+key] = value
		}
	}
	for _, parent := range parents {
		var content map[string]any
		if err = cli.StateEvent(ctx, parent, event.StateSpaceChild, string(portal.MXID), &content); err != nil && !errors.Is(err, mautrix.MNotFound) {
			return nil, err
		}
		if content == nil {
			content = map[string]any{}
		}
		values["child:"+string(parent)] = migrationJSON(content)
	}
	return values, nil
}
func migrationBridgeValues(before map[string]string, desired presentationValue) map[string]string {
	values := maps.Clone(before)
	for key := range values {
		if !strings.HasPrefix(key, "bridge:") {
			continue
		}
		switch {
		case strings.HasSuffix(key, ":kind"):
			values[key] = string(desired.Kind)
		case strings.HasSuffix(key, ":legacy"):
			values[key] = ""
			if desired.Kind == "dm" || desired.Kind == "group_dm" {
				values[key] = "dm"
			}
		case strings.HasSuffix(key, ":parent"):
			values[key] = desired.Parent
		}
	}
	return values
}
func expectedMigrationWire(before map[string]string, desired presentationValue, parents []id.RoomID, target id.RoomID, via string) map[string]string {
	values := migrationBridgeValues(before, desired)
	for _, parent := range parents {
		values["parent:"+string(parent)] = "{}"
		values["child:"+string(parent)] = "{}"
		if parent == target {
			values["parent:"+string(parent)] = migrationJSON(&event.SpaceParentEventContent{Via: []string{via}, Canonical: true})
			values["child:"+string(parent)] = migrationJSON(&event.SpaceChildEventContent{Via: []string{via}})
		}
	}
	return values
}
func restoreMigrationLinks(ctx context.Context, w hierarchyWire, child id.RoomID, parents []id.RoomID, before map[string]string, allowed ...map[string]string) error {
	for _, parent := range parents {
		for _, link := range []struct {
			room       id.RoomID
			typ        event.Type
			key, value string
		}{
			{child, event.StateSpaceParent, string(parent), before["parent:"+string(parent)]},
			{parent, event.StateSpaceChild, string(child), before["child:"+string(parent)]},
		} {
			var content map[string]any
			if err := json.Unmarshal([]byte(link.value), &content); err != nil {
				return err
			}
			var existing map[string]any
			if err := w.get(ctx, link.room, link.typ, link.key, &existing); err != nil && !errors.Is(err, mautrix.MNotFound) {
				return err
			}
			current := "{}"
			if existing != nil {
				current = migrationJSON(existing)
			}
			field := "parent:" + string(parent)
			if link.typ == event.StateSpaceChild {
				field = "child:" + string(parent)
			}
			permitted := current == link.value
			for _, values := range allowed {
				permitted = permitted || current == values[field]
			}
			if !permitted {
				return errProjectionConflict
			}
			if current == link.value {
				continue
			}
			if err := w.put(ctx, link.room, link.typ, link.key, content); err != nil {
				return err
			}
			var got map[string]any
			if err := w.get(ctx, link.room, link.typ, link.key, &got); err != nil {
				return err
			}
			if migrationJSON(got) != link.value {
				return errors.New("rollback hierarchy readback differs")
			}
		}
	}
	return nil
}
