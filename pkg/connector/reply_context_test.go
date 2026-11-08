package connector

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"hegel.dev/go/hegel"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
)

func TestSeparateChildRetainsSourceParentContext(t *testing.T) {
	parent, root := int64(3), int64(9)
	d := sidecar.DialogRecord{ChatID: 7, Structure: &sidecar.DialogStructure{Kind: "reply_child", ParentChatID: &parent, ParentMessageID: &root}}
	ic := &InlineClient{AccountID: "1", UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "1"}}}
	info := ic.chatInfoForDialog(t.Context(), d, nil)
	if info.Type == nil || *info.Type != database.RoomTypeDefault || info.ParentID != nil || !strings.Contains(mustReplyTopic(t, ic, d), "chat 3") || !strings.Contains(mustReplyTopic(t, ic, d), "message 9") {
		t.Fatalf("child identity/parent context lost: %+v", info)
	}
	d.ChatListHidden = dirPtr(true)
	if ic.shouldProjectDialog(d) {
		t.Fatal("context auto-projected a hidden child")
	}
}

func TestSeparateChildNeverSendsParentLocalReplyID(t *testing.T) {
	calls := 0
	s := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		mid := int64(11)
		writeConnectorSidecarResult(t, w, "message", sidecar.MessageMutation{State: sidecar.TransactionCompleted, MessageID: &mid})
	}))
	defer s.Close()
	ic := richClient()
	ic.Sidecar = sidecar.NewClient(s.URL)
	for _, thread := range []bool{false, true} {
		msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: testPortal("7"), Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "child message"}}}
		if thread {
			msg.ThreadRoot = &database.Message{ID: "3/9"}
		} else {
			msg.ReplyTo = &database.Message{ID: "3/9"}
		}
		if _, err := ic.HandleMatrixMessage(t.Context(), msg); err == nil || calls != 0 {
			t.Fatalf("thread/cross-chat reply flattened: %v calls%d", err, calls)
		}
	}
}

func TestSeparateChildThreadBackfillReturnsExplicitUnsupported(t *testing.T) {
	if _, err := (&InlineClient{}).FetchMessages(t.Context(), bridgev2.FetchMessagesParams{ThreadRoot: "3/9"}); err == nil {
		t.Fatal("unsupported Matrix thread reported an empty successful history")
	}
}

func TestSeparateChildBacklinkUsesPersistedReceiverMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "context.db")
	ic := fixtureHistoryClient(t, path)
	key := ic.portalKeyForChat(3)
	if err := ic.UserLogin.Bridge.DB.Portal.Insert(t.Context(), &database.Portal{PortalKey: key, MXID: "!parent:test", Metadata: &PortalMetadata{}}); err != nil {
		t.Fatal(err)
	}
	if err := ic.UserLogin.Bridge.DB.Message.Insert(t.Context(), &database.Message{ID: "3/9", MXID: "$root", Room: key, Metadata: &MessageMetadata{}}); err != nil {
		t.Fatal(err)
	}
	d := sidecar.DialogRecord{ChatID: 7, Description: stringPtr("native child"), Structure: &sidecar.DialogStructure{Kind: "reply_child", ParentChatID: dirPtr(int64(3)), ParentMessageID: dirPtr(int64(9))}}
	restarted := fixtureHistoryClient(t, path)
	info := restarted.chatInfoForDialog(t.Context(), d, nil)
	topic := mustReplyTopic(t, restarted, d)
	if !strings.Contains(topic, "native child") || !strings.Contains(topic, "!parent:test/$root") {
		t.Fatalf("restart lost persisted backlink: %+v", info.Topic)
	}
	// The same native IDs on another login cannot reuse the first receiver's
	// existing portal/message mapping or create a parent as a side effect.
	restarted.UserLogin.ID = "other-login"
	info = restarted.chatInfoForDialog(t.Context(), d, nil)
	topic = mustReplyTopic(t, restarted, d)
	if strings.Contains(topic, "matrix.to") || !strings.Contains(topic, "chat 3, message 9") {
		t.Fatalf("cross-receiver backlink: %+v", info.Topic)
	}
	if parent, err := restarted.UserLogin.Bridge.DB.Portal.GetByKey(t.Context(), restarted.portalKeyForChat(3)); err != nil || parent != nil {
		t.Fatalf("context lookup created a parent: %+v %v", parent, err)
	}
}

// The pinned framework resolves relations globally by Matrix event ID. Keep its
// actual DB lookup in this regression: constructing a MatrixMessage alone would
// miss the cross-login provenance of an otherwise identical source chat ID.
func TestSeparateChildFrameworkRejectsCrossReceiverReply(t *testing.T) {
	for _, mode := range []string{"cross-receiver", "same-room", "raw-thread", "raw-thread-no-fallback"} {
		t.Run(mode, func(t *testing.T) {
			cross := mode == "cross-receiver"
			ic := fixtureHistoryClient(t, filepath.Join(t.TempDir(), "reply.db"))
			ic.AccountID = "1"
			ic.UserLogin.Client = ic
			br := ic.UserLogin.Bridge
			portal, err := br.GetPortalByKey(t.Context(), ic.portalKeyForChat(7))
			if err != nil {
				t.Fatal(err)
			}
			portal.MXID = "!current:test"
			if err = portal.Save(t.Context()); err != nil {
				t.Fatal(err)
			}
			target := portal.PortalKey
			if cross {
				target.Receiver = "other-login"
				if err = br.DB.Portal.Insert(t.Context(), &database.Portal{PortalKey: target, MXID: "!other:test", Metadata: &PortalMetadata{}}); err != nil {
					t.Fatal(err)
				}
			}
			if err = br.DB.Message.Insert(t.Context(), &database.Message{ID: "7/9", Room: target, MXID: "$target", Metadata: &MessageMetadata{}}); err != nil {
				t.Fatal(err)
			}
			calls := 0
			server := newSidecarTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != "/rpc/send" {
					t.Errorf("unexpected RPC %s", r.URL.Path)
				}
				var request sidecar.SendTextRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if mode == "same-room" && (request.ReplyToMessageID == nil || *request.ReplyToMessageID != 9) {
					t.Errorf("reply lost: %+v", request)
				}
				// Stop after recording the intended mutation boundary; no Matrix send/ghost
				// fixture is needed to prove correct relation resolution and RPC admission.
				writeConnectorSidecarResult(t, w, "message", sidecar.MessageMutation{State: sidecar.TransactionFailed})
			}))
			defer server.Close()
			ic.Sidecar = sidecar.NewClient(server.URL).WithSessionNamespace("fixture")
			relation := &event.RelatesTo{InReplyTo: &event.InReplyTo{EventID: "$target"}}
			if strings.HasPrefix(mode, "raw-thread") {
				relation.Type, relation.EventID = event.RelThread, "$target"
				if mode == "raw-thread-no-fallback" {
					relation.InReplyTo = nil
				}
			}
			result := portal.Internal().HandleMatrixMessage(t.Context(), ic.UserLogin, nil, &event.Event{
				ID: "$out", RoomID: portal.MXID, Type: event.EventMessage,
				Content: event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: "reply", RelatesTo: relation}},
			})
			want := 1
			if mode != "same-room" {
				want = 0
			}
			if calls != want || result.Error == nil {
				t.Fatalf("mode=%s reached %d source mutations, want %d; result=%+v", mode, calls, want, result)
			}
		})
	}
}

func TestSeparateChildReplyReceiverProperty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		receiver := networkid.UserLoginID(fmt.Sprintf("login-%d", hegel.Draw(ht, hegel.Integers[int64](math.MinInt64, math.MaxInt64))))
		same := hegel.Draw(ht, hegel.Booleans())
		portal := testPortal("7")
		portal.Receiver = receiver
		target := portal.PortalKey
		if !same {
			target.Receiver += "-other"
		}
		msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: portal}, ReplyTo: &database.Message{ID: "7/9", Room: target}}
		err := validateInlineReplyTarget(msg, 7)
		if (err == nil) != same {
			ht.Fatalf("receiver check same=%t: %v", same, err)
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func mustReplyTopic(t *testing.T, ic *InlineClient, d sidecar.DialogRecord) string {
	t.Helper()
	value, err := ic.replyChildTopic(t.Context(), d)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
