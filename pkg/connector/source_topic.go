package connector

import (
	"context"
	"errors"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
)

type sourceTopicIntent struct {
	Before  string `json:"before"`
	Desired string `json:"desired"`
}
type sourceTopicState struct {
	Owned     *string            `json:"owned,omitempty"`
	Pending   *sourceTopicIntent `json:"pending,omitempty"`
	UserOwned bool               `json:"user_owned,omitempty"`
}

func (ic *InlineClient) topicProjectionEligible(d sidecar.DialogRecord) bool {
	return classifyDialog(d) != roomUnknown && ic.shouldProjectDialog(d)
}

// All writes run in the existing serialized portal lane. Intent is durable
// before a topic write, and observed state is read back before ownership moves.
// Matrix offers no atomic CAS: this retains the controlled-window limitation.
func (ic *InlineClient) reconcileSourceTopic(ctx context.Context, p *bridgev2.Portal, d sidecar.DialogRecord) error {
	if !ic.topicProjectionEligible(d) {
		return nil
	}
	meta, ok := p.Metadata.(*PortalMetadata)
	if !ok || meta == nil {
		return errors.New("source topic metadata unavailable")
	}
	state := meta.InlineTopic
	if state == nil {
		state = &sourceTopicState{}
	}
	if state.UserOwned {
		return nil
	}
	desired, err := ic.replyChildTopic(ctx, d)
	if err != nil {
		if p.MXID == "" {
			p.Topic = ""
			p.TopicSet = false
		}
		return err
	}
	if desired == "" && state.Owned == nil && state.Pending == nil {
		return nil
	}

	save := func(next *sourceTopicState) error {
		previous := meta.InlineTopic
		meta.InlineTopic = next
		if err := p.Save(ctx); err != nil {
			meta.InlineTopic = previous
			return err
		}
		state = next
		return nil
	}
	// Room creation consumes the cached topic. Keep its intent pending until a
	// later existing-room read proves that initial state reached the homeserver.
	if p.MXID == "" {
		if state.Pending == nil || state.Pending.Desired != desired {
			before := p.Topic
			if state.Pending != nil {
				before = state.Pending.Before
			}
			if err := save(&sourceTopicState{Owned: state.Owned, Pending: &sourceTopicIntent{Before: before, Desired: desired}}); err != nil {
				// ExtraUpdates cannot abort pinned room creation. Expose no
				// obsolete or non-durable context to that creation request.
				// The old durable intent survives; a nonempty-before fallback
				// may conservatively retire ownership on later reconciliation.
				p.Topic = ""
				p.TopicSet = false
				return err
			}
		}
		p.Topic = state.Pending.Desired
		p.TopicSet = false
		return p.Save(ctx)
	}

	raw, ok := p.Bridge.Bot.(*matrix.ASIntent)
	if !ok || raw == nil || raw.Matrix == nil || raw.Matrix.Client == nil {
		return errors.New("source topic requires verified Matrix state transport")
	}
	read := func() (string, error) {
		var content event.TopicEventContent
		err := raw.Matrix.Client.StateEvent(ctx, p.MXID, event.StateTopic, "", &content)
		if errors.Is(err, mautrix.MNotFound) {
			return "", nil
		}
		return content.Topic, err
	}
	current, err := read()
	if err != nil {
		return err
	}
	retire := func() error {
		p.Topic = current
		p.TopicSet = true
		return save(&sourceTopicState{UserOwned: true})
	}
	// Finish one prior operation, then allow one new source desire. This is
	// bounded even if a source descriptor changes while an old intent is pending.
	for step := 0; step < 2; step++ {
		if state.Pending != nil && state.Pending.Desired != desired && current == state.Pending.Before {
			// Nothing from the prior intent reached Matrix; supersede it
			// durably rather than publish obsolete context just to retract it.
			if err = save(&sourceTopicState{Owned: state.Owned, Pending: &sourceTopicIntent{Before: current, Desired: desired}}); err != nil {
				return err
			}
		}
		if state.Pending == nil {
			if state.Owned == nil {
				if desired == "" {
					return nil
				}
				// Equality does not prove ownership of a pre-existing topic.
				if current != "" {
					return retire()
				}
			} else if current != *state.Owned {
				return retire()
			}
			if current == desired {
				return nil
			}
			if err = save(&sourceTopicState{Owned: state.Owned, Pending: &sourceTopicIntent{Before: current, Desired: desired}}); err != nil {
				return err
			}
		}
		pending := state.Pending
		if current != pending.Desired {
			if current != pending.Before {
				return retire()
			}
			// No ExtraUpdates on this inner update: never recurse into this lane.
			p.UpdateInfo(ctx, &bridgev2.ChatInfo{Topic: &pending.Desired}, nil, nil, time.Time{})
			current, err = read()
			if err != nil {
				return err
			}
			if current != pending.Desired {
				if current != pending.Before {
					return retire()
				}
				return errors.New("source topic write was not observed")
			}
		}
		next := &sourceTopicState{}
		if pending.Desired != "" {
			value := pending.Desired
			next.Owned = &value
		}
		p.Topic = current
		p.TopicSet = true
		if err = save(next); err != nil {
			return err
		}
		if current == desired {
			return nil
		}
	}
	return nil
}
