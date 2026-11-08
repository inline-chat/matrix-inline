package connector

import (
	"fmt"
	"hegel.dev/go/hegel"
	"math"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/id"
)

func seedTopicParent(t *testing.T, f *topicFixture, chat int64, eventID id.EventID) {
	t.Helper()
	key := f.ic.portalKeyForChat(chat)
	db := f.ic.UserLogin.Bridge.DB
	if err := db.Portal.Insert(t.Context(), &database.Portal{PortalKey: key, MXID: id.RoomID("!parent-" + string(eventID) + ":test"), Metadata: &PortalMetadata{}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Message.Insert(t.Context(), &database.Message{ID: makeMessageID(chat, 9), Room: key, MXID: eventID, Metadata: &MessageMetadata{}}); err != nil {
		t.Fatal(err)
	}
}
func breakTopicMessageReads(t *testing.T, f *topicFixture) {
	t.Helper()
	db := f.ic.UserLogin.Bridge.DB
	if _, err := db.Exec(t.Context(), `ALTER TABLE message RENAME TO message_hold`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(t.Context(), `CREATE VIEW message AS SELECT * FROM message_hold WHERE json_extract('not-json','$')`); err != nil {
		t.Fatal(err)
	}
}
func restoreTopicMessageReads(t *testing.T, f *topicFixture) {
	t.Helper()
	db := f.ic.UserLogin.Bridge.DB
	if _, err := db.Exec(t.Context(), `DROP VIEW message`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(t.Context(), `ALTER TABLE message_hold RENAME TO message`); err != nil {
		t.Fatal(err)
	}
}
func TestSourceTopicBacklinkDBFailurePreservesOwnedText(t *testing.T) {
	f := newTopicFixture(t)
	seedTopicParent(t, f, 3, "$root3")
	f.apply(t, topicChild())
	before := f.topic
	if !strings.Contains(before, "matrix.to") {
		t.Fatal("backlink was not projected")
	}
	calls, writes := f.matrixCalls, f.writes
	breakTopicMessageReads(t, f)
	f.apply(t, topicChild())
	if f.topic != before || f.writes != writes || f.matrixCalls != calls {
		t.Fatalf("transient DB error rewrote owned context: topic%q writes%d=>%d Matrixcalls%d=>%d", f.topic, writes, f.writes, calls, f.matrixCalls)
	}
	restoreTopicMessageReads(t, f)
	if _, err := f.ic.UserLogin.Bridge.DB.Exec(t.Context(), `DELETE FROM message WHERE id=$1`, "3/9"); err != nil {
		t.Fatal(err)
	}
	f.apply(t, topicChild())
	if strings.Contains(f.topic, "matrix.to") || !strings.Contains(f.topic, "chat 3") || f.writes != writes+1 {
		t.Fatalf("conclusive absence did not retain readable numeric context: %q writes%d", f.topic, f.writes)
	}
}
func TestSourceTopicPrecreationDBFailureExposesNoStaleCache(t *testing.T) {
	f := newTopicFixture(t)
	seedTopicParent(t, f, 3, "$root3")
	seedTopicParent(t, f, 8, "$root8")
	f.portal.MXID = ""
	if err := f.portal.Save(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.apply(t, topicChild())
	old := f.portal.Topic
	if !strings.Contains(old, "chat 3") {
		t.Fatal("old creation context not cached")
	}
	breakTopicMessageReads(t, f)
	changed := topicChild()
	changed.Structure.ParentChatID = foundationInt64(8)
	f.apply(t, changed)
	stored, err := f.ic.UserLogin.Bridge.DB.Portal.GetByKey(t.Context(), f.portal.PortalKey)
	if err != nil {
		t.Fatal(err)
	}
	pending := stored.Metadata.(*PortalMetadata).InlineTopic.Pending
	if f.portal.Topic != "" || pending == nil || pending.Desired != old || f.matrixCalls != 0 {
		t.Fatalf("lookup failure exposed non-durable context cache%q pending%+v calls%d", f.portal.Topic, pending, f.matrixCalls)
	}
	restoreTopicMessageReads(t, f)
	f.apply(t, changed)
	if !strings.Contains(f.portal.Topic, "chat 8") || !strings.Contains(f.portal.Topic, "$root8") || f.matrixCalls != 0 {
		t.Fatalf("creation did not recover latest context: %q calls%d", f.portal.Topic, f.matrixCalls)
	}
}
func TestSourceTopicChannelDescriptionFollowsOwnership(t *testing.T) {
	f := newTopicFixture(t)
	description := "source description"
	d := topicChannel()
	d.Description = &description
	f.apply(t, d)
	if f.topic != description {
		t.Fatalf("source description missing: %q", f.topic)
	}
	description = "new description"
	f.apply(t, d)
	if f.topic != description {
		t.Fatal("owned description failed to update")
	}
	writes := f.writes
	f.topic = "user choice"
	description = "later source"
	f.apply(t, d)
	if f.topic != "user choice" || f.writes != writes {
		t.Fatal("source description overwrote user topic")
	}
}

func TestSourceTopicDirectDescriptionDoesNotSeedTopic(t *testing.T) {
	f := newTopicFixture(t)
	d := topicChannel()
	description := "not a projected DM topic"
	d.Description = &description
	d.Structure.Kind = "direct"
	d.Structure.PeerUserID = foundationInt64(42)
	if err := f.ic.reconcileSourceTopic(t.Context(), f.portal, d); err != nil {
		t.Fatal(err)
	}
	if f.topic != "" || f.matrixCalls != 0 {
		t.Fatal("direct description unexpectedly became a generated topic")
	}
}

func TestSourceTopicBacklinkLookupFailureProperty(t *testing.T) {
	f := newTopicFixture(t)
	seedTopicParent(t, f, 3, "$root3")
	f.apply(t, topicChild())
	hegel.Test(t, func(ht *hegel.T) {
		number := hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, math.MaxInt64))
		failed := hegel.Draw(ht, hegel.Booleans())
		d := topicChild()
		description := fmt.Sprintf("source-%d", number)
		d.Description = &description
		before, calls := f.topic, f.matrixCalls
		if failed {
			breakTopicMessageReads(t, f)
		}
		f.apply(t, d)
		if failed {
			restoreTopicMessageReads(t, f)
		}
		if failed {
			if f.topic != before || f.matrixCalls != calls {
				ht.Fatalf("failed mapping read published topic%q=>%q calls%d=>%d", before, f.topic, calls, f.matrixCalls)
			}
		} else if !strings.HasPrefix(f.topic, description+"\n\n") || !strings.Contains(f.topic, "$root3") {
			ht.Fatalf("valid source change lost description/backlink: %q", f.topic)
		}
	}, hegel.WithTestCases(100), hegel.WithDatabase(t.TempDir()))
}
