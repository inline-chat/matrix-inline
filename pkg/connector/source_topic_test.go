package connector

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"hegel.dev/go/hegel"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
)

type topicFixture struct {
	path           string
	topic          string
	writes         int
	matrixCalls    int
	failWrite      bool
	failRead       bool
	failAfterWrite bool
	server         *httptest.Server
	ic             *InlineClient
	portal         *bridgev2.Portal
}

func newTopicFixture(t *testing.T) *topicFixture {
	t.Helper()
	f := &topicFixture{path: filepath.Join(t.TempDir(), "topics.db")}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.matrixCalls++
		if strings.Contains(r.URL.Path, "/state/m.room.topic") {
			if r.Method == http.MethodGet {
				if f.failRead {
					http.Error(w, "temporary read failure", 503)
					return
				}
				json.NewEncoder(w).Encode(event.TopicEventContent{Topic: f.topic})
				return
			}
			if r.Method == http.MethodPut {
				if f.failWrite {
					http.Error(w, "temporary write failure", 503)
					return
				}
				var c event.TopicEventContent
				if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
					t.Error(err)
				}
				f.topic = c.Topic
				f.writes++
				if f.failAfterWrite {
					f.failRead = true
				}
			}
		}
		json.NewEncoder(w).Encode(map[string]string{"event_id": "$state"})
	}))
	t.Cleanup(f.server.Close)
	f.restart(t)
	return f
}
func (f *topicFixture) restart(t *testing.T) {
	t.Helper()
	f.ic = fixtureHistoryClient(t, f.path)
	f.ic.AccountID = "1"
	br := f.ic.UserLogin.Bridge
	as, err := appservice.CreateFull(appservice.CreateOpts{HomeserverDomain: "test", HomeserverURL: f.server.URL, Registration: &appservice.Registration{SenderLocalpart: "bot", AppToken: "synthetic"}})
	if err != nil {
		t.Fatal(err)
	}
	as.BotIntent().Client.DefaultHTTPRetries = 0
	br.Bot = &matrix.ASIntent{Matrix: as.BotIntent()}
	f.portal, err = br.GetPortalByKey(t.Context(), f.ic.portalKeyForChat(7))
	if err != nil {
		t.Fatal(err)
	}
	f.portal.MXID = "!child:test"
	if err = as.StateStore.SetMembership(t.Context(), f.portal.MXID, br.Bot.GetMXID(), event.MembershipJoin); err != nil {
		t.Fatal(err)
	}
	if err = f.portal.Save(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func topicChild() sidecar.DialogRecord {
	return sidecar.DialogRecord{ChatID: 7, Structure: &sidecar.DialogStructure{Kind: "reply_child", ParentChatID: dirPtr(int64(3)), ParentMessageID: dirPtr(int64(9))}}
}
func topicChannel() sidecar.DialogRecord {
	return sidecar.DialogRecord{ChatID: 7, Structure: &sidecar.DialogStructure{Kind: "channel"}}
}
func (f *topicFixture) apply(t *testing.T, d sidecar.DialogRecord) {
	t.Helper()
	info := f.ic.chatInfoForDialog(t.Context(), d, nil)
	f.portal.UpdateInfo(t.Context(), info, nil, nil, time.Time{})
}
func (f *topicFixture) pending(t *testing.T) bool {
	t.Helper()
	row, err := f.ic.UserLogin.Bridge.DB.Portal.GetByKey(t.Context(), f.portal.PortalKey)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(row.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	var meta struct {
		Topic *struct {
			Pending json.RawMessage `json:"pending"`
		} `json:"inline_topic"`
	}
	if err = json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	return meta.Topic != nil && len(meta.Topic.Pending) > 0 && string(meta.Topic.Pending) != "null"
}
func TestSourceTopicDetachAndUserChoiceAcrossRestart(t *testing.T) {
	for _, mode := range []string{"detach", "unknown", "user-choice"} {
		t.Run(mode, func(t *testing.T) {
			f := newTopicFixture(t)
			f.apply(t, topicChild())
			if !strings.Contains(f.topic, "chat 3") {
				t.Fatal("child context not written")
			}
			before := f.topic
			f.restart(t)
			d := topicChannel()
			if mode == "unknown" {
				d.Structure = nil
			}
			if mode == "user-choice" {
				f.topic = "user choice"
			}
			f.apply(t, d)
			want := ""
			if mode == "unknown" {
				want = before
			}
			if mode == "user-choice" {
				want = "user choice"
			}
			if f.topic != want {
				t.Fatalf("%s retained/replaced incorrect topic %q want %q", mode, f.topic, want)
			}
		})
	}
}
func TestSourceTopicPendingReadbackResumesWithoutDuplicateWrite(t *testing.T) {
	f := newTopicFixture(t)
	f.failAfterWrite = true
	f.apply(t, topicChild())
	if !f.pending(t) {
		t.Fatal("write without readback lost durable intent")
	}
	writes := f.writes
	f.failAfterWrite = false
	f.failRead = false
	f.restart(t)
	f.apply(t, topicChild())
	if f.pending(t) || f.writes != writes {
		t.Fatalf("readback recovery duplicated write or stayed pending: %d => %d", writes, f.writes)
	}
	f.apply(t, topicChannel())
	if f.topic != "" {
		t.Fatal("recovered ownership did not retract context")
	}
}

func TestSourceTopicPendingRetriesOnPortalEvent(t *testing.T) {
	for _, userChoice := range []bool{false, true} {
		t.Run(fmt.Sprintf("user-choice=%t", userChoice), func(t *testing.T) {
			f := newTopicFixture(t)
			d := topicChild()
			f.ic.rememberDialog(d)
			f.failWrite = true
			f.apply(t, d)
			if !f.pending(t) {
				t.Fatal("failed write lost pending intent")
			}
			f.failWrite = false
			if userChoice {
				f.topic = "user choice"
			}
			// The next existing portal event invokes the real post-handler;
			// there is no new dialog observation or manual topic reconciliation.
			f.ic.projectionPostHandler(t.Context(), f.portal)
			if f.pending(t) {
				t.Fatal("portal event did not resolve pending intent")
			}
			if userChoice {
				if f.topic != "user choice" || f.writes != 0 {
					t.Fatalf("user topic overwritten: %q, writes %d", f.topic, f.writes)
				}
			} else if !strings.Contains(f.topic, "chat 3") || f.writes != 1 {
				t.Fatalf("pending context not applied once: %q, writes %d", f.topic, f.writes)
			}
			calls := f.matrixCalls
			f.ic.projectionPostHandler(t.Context(), f.portal)
			if f.matrixCalls != calls {
				t.Fatal("completed intent caused another topic request")
			}
		})
	}
}

func TestSourceTopicPendingEventRetriesAreRateLimited(t *testing.T) {
	f := newTopicFixture(t)
	d := topicChild()
	f.ic.rememberDialog(d)
	f.failWrite = true
	f.apply(t, d)
	f.ic.projectionPostHandler(t.Context(), f.portal)
	calls := f.matrixCalls
	f.ic.projectionPostHandler(t.Context(), f.portal)
	if !f.pending(t) || f.matrixCalls != calls {
		t.Fatal("repeated portal event retried pending topic without delay")
	}
}

func TestSourceTopicIneligibleEventDoesNotDelayRetry(t *testing.T) {
	f := newTopicFixture(t)
	d := topicChild()
	f.ic.rememberDialog(d)
	f.failWrite = true
	f.apply(t, d)
	if !f.pending(t) {
		t.Fatal("failed write lost pending intent")
	}
	unknown := d
	unknown.Structure = &sidecar.DialogStructure{Kind: "unknown"}
	f.ic.rememberDialog(unknown)
	f.ic.projectionPostHandler(t.Context(), f.portal)
	f.ic.rememberDialog(d)
	f.failWrite = false
	f.ic.projectionPostHandler(t.Context(), f.portal)
	if f.pending(t) || f.writes != 1 || !strings.Contains(f.topic, "chat 3") {
		t.Fatalf("ineligible event delayed eligible retry: topic %q writes %d", f.topic, f.writes)
	}
}
func TestSourceTopicFailedWriteAndConflictStaySafe(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "conflict"}[conflict], func(t *testing.T) {
			f := newTopicFixture(t)
			f.failWrite = true
			f.apply(t, topicChild())
			if !f.pending(t) || f.writes != 0 {
				t.Fatal("failed topic send lost intent or claimed write")
			}
			f.failWrite = false
			f.restart(t)
			if conflict {
				f.topic = "new user value"
			}
			f.apply(t, topicChild())
			if conflict {
				if f.topic != "new user value" || f.writes != 0 {
					t.Fatal("overwrote user during restart")
				}
			} else if !strings.Contains(f.topic, "chat 3") || f.pending(t) {
				t.Fatal("pending write did not resume")
			}
		})
	}
}
func TestSourceTopicUnownedEqualTextIsNotAdopted(t *testing.T) {
	f := newTopicFixture(t)
	f.topic = mustReplyTopic(t, f.ic, topicChild())
	before := f.topic
	f.apply(t, topicChild())
	f.restart(t)
	f.apply(t, topicChannel())
	if f.topic != before || f.writes != 0 {
		t.Fatal("equal unowned topic was adopted and cleared")
	}
}

func TestSourceTopicIntentSaveFailurePreventsExternalWrite(t *testing.T) {
	f := newTopicFixture(t)
	db := f.ic.UserLogin.Bridge.DB
	if _, err := db.Exec(t.Context(), `CREATE TRIGGER deny_topic_intent BEFORE UPDATE ON portal WHEN NEW.metadata LIKE '%"pending"%' BEGIN SELECT RAISE(FAIL,'fixture intent failure'); END`); err != nil {
		t.Fatal(err)
	}
	f.apply(t, topicChild())
	if f.writes != 0 || f.pending(t) {
		t.Fatal("external topic write occurred before durable intent")
	}
	if _, err := db.Exec(t.Context(), `DROP TRIGGER deny_topic_intent`); err != nil {
		t.Fatal(err)
	}
	f.restart(t)
	f.apply(t, topicChild())
	if f.writes != 1 || f.pending(t) {
		t.Fatal("intent-save recovery did not apply once")
	}
}

func TestSourceTopicCompletionSaveFailureRecovers(t *testing.T) {
	f := newTopicFixture(t)
	db := f.ic.UserLogin.Bridge.DB
	if _, err := db.Exec(t.Context(), `CREATE TRIGGER deny_topic_completion BEFORE UPDATE ON portal WHEN NEW.metadata LIKE '%"owned"%' AND NEW.metadata NOT LIKE '%"pending"%' BEGIN SELECT RAISE(FAIL,'fixture completion failure'); END`); err != nil {
		t.Fatal(err)
	}
	f.apply(t, topicChild())
	if f.writes != 1 || !f.pending(t) {
		t.Fatal("completion failure lost durable pending intent")
	}
	if _, err := db.Exec(t.Context(), `DROP TRIGGER deny_topic_completion`); err != nil {
		t.Fatal(err)
	}
	f.restart(t)
	f.apply(t, topicChild())
	if f.writes != 1 || f.pending(t) {
		t.Fatal("restart repeated an already observed topic write")
	}
	f.apply(t, topicChannel())
	if f.topic != "" {
		t.Fatal("completion recovery lost topic ownership")
	}
}

func TestSourceTopicPendingUserChoiceProperty(t *testing.T) {
	f := newTopicFixture(t)
	hegel.Test(t, func(ht *hegel.T) {
		value := []rune(hegel.Draw(ht, hegel.Text()))
		if len(value) > 80 {
			value = value[:80]
		}
		userChoice := hegel.Draw(ht, hegel.Booleans())
		// Each generated case begins with a fresh logical portal state in the same
		// real database, avoiding thousands of open SQLite/HTTP fixture instances.
		f.portal.Metadata = &PortalMetadata{}
		f.portal.Topic = ""
		f.portal.TopicSet = true
		f.topic = ""
		f.writes = 0
		f.failRead = false
		f.failAfterWrite = true
		if err := f.portal.Save(t.Context()); err != nil {
			ht.Fatal(err)
		}
		f.apply(t, topicChild())
		if !f.pending(t) {
			ht.Fatal("unobserved write lost intent")
		}
		f.failAfterWrite = false
		f.failRead = false
		want := f.topic
		if userChoice {
			want = "user-" + string(value)
			f.topic = want
		}
		writes := f.writes
		f.restart(t)
		f.apply(t, topicChild())
		if f.topic != want || f.writes != writes || f.pending(t) {
			ht.Fatalf("pending replay changed user=%t topic%q=>%q writes%d=>%d", userChoice, want, f.topic, writes, f.writes)
		}
	}, hegel.WithTestCases(100), hegel.WithDatabase(t.TempDir()))
}

func TestSourceTopicPrecreationPendingTracksLatestDescriptor(t *testing.T) {
	for _, before := range []string{"", "old cached topic"} {
		for _, detach := range []bool{false, true} {
			for _, fail := range []bool{false, true} {
				t.Run(fmt.Sprintf("before=%q/detach=%t/fail=%t", before, detach, fail), func(t *testing.T) {
					f := newTopicFixture(t)
					f.portal.MXID = ""
					f.portal.Topic = before
					if err := f.portal.Save(t.Context()); err != nil {
						t.Fatal(err)
					}
					f.apply(t, topicChild())
					old := f.portal.Topic
					d := topicChild()
					d.Structure.ParentChatID = dirPtr(int64(8))
					want := mustReplyTopic(t, f.ic, d)
					if detach {
						d = topicChannel()
						want = ""
					}
					db := f.ic.UserLogin.Bridge.DB
					if fail {
						if _, err := db.Exec(t.Context(), `CREATE TRIGGER deny_precreation_topic BEFORE UPDATE ON portal BEGIN SELECT RAISE(FAIL,'fixture save failure'); END`); err != nil {
							t.Fatal(err)
						}
					}
					f.apply(t, d)
					row, err := db.Portal.GetByKey(t.Context(), f.portal.PortalKey)
					if err != nil {
						t.Fatal(err)
					}
					pending := row.Metadata.(*PortalMetadata).InlineTopic.Pending
					if pending == nil {
						t.Fatal("creation topic lost pending intent")
					}
					if fail {
						if f.portal.Topic != "" || pending.Desired != old {
							t.Fatalf("failed save exposed stale/non-durable topic: cache%q intent%+v", f.portal.Topic, pending)
						}
						if _, err = db.Exec(t.Context(), `DROP TRIGGER deny_precreation_topic`); err != nil {
							t.Fatal(err)
						}
						f.apply(t, d)
						row, err = db.Portal.GetByKey(t.Context(), f.portal.PortalKey)
						if err != nil {
							t.Fatal(err)
						}
						pending = row.Metadata.(*PortalMetadata).InlineTopic.Pending
					}
					if f.portal.Topic != want || pending.Desired != want || f.matrixCalls != 0 {
						t.Fatalf("creation used obsolete topic: cache%q want%q intent%+v Matrixcalls%d", f.portal.Topic, want, pending, f.matrixCalls)
					}
				})
			}
		}
	}
}

func TestSourceTopicUnappliedIntentSupersededBeforeWireWrite(t *testing.T) {
	f := newTopicFixture(t)
	f.failWrite = true
	f.apply(t, topicChild())
	if !f.pending(t) {
		t.Fatal("missing pending source context")
	}
	f.failWrite = false
	f.apply(t, topicChannel())
	if f.writes != 0 || f.topic != "" || f.pending(t) {
		t.Fatalf("obsolete pending context was published before detach: writes%d topic%q", f.writes, f.topic)
	}
}
