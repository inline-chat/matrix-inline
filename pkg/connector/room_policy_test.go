package connector

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"hegel.dev/go/hegel"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

func policyDialog(t testing.TB, raw string) sidecar.DialogRecord {
	t.Helper()
	var d sidecar.DialogRecord
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestFoundationClassification(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		kind  database.RoomType
		known bool
	}{
		{`{"chat_id":7}`, database.RoomTypeDefault, false},
		{`{"chat_id":7,"structure":{"kind":"channel","peer_user_id":null,"space_id":null,"parent_chat_id":null,"parent_message_id":null}}`, database.RoomTypeDefault, true},
		{`{"chat_id":7,"structure":{"kind":"direct","peer_user_id":42,"space_id":null,"parent_chat_id":null,"parent_message_id":null}}`, database.RoomTypeDM, true},
		{`{"chat_id":7,"structure":{"kind":"reply_child","parent_chat_id":3,"parent_message_id":9,"peer_user_id":null,"space_id":null}}`, database.RoomTypeDefault, true},
		{`{"chat_id":7,"peer_user_id":42,"structure":{"kind":"direct","peer_user_id":42,"parent_chat_id":3,"space_id":null,"parent_message_id":null}}`, database.RoomTypeDefault, false},
	} {
		d := policyDialog(t, tc.raw)
		info := (&InlineClient{AccountID: "1", UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "1"}}}).chatInfoForDialog(t.Context(), d, nil)
		if tc.known && (info.Type == nil || *info.Type != tc.kind) {
			t.Errorf("%s: type=%v want %s", tc.raw, info.Type, tc.kind)
		}
		if !tc.known && info.Type != nil {
			t.Errorf("%s: unknown structure must preserve existing type", tc.raw)
		}
	}
}

func TestFoundationEqualityIncludesPresentation(t *testing.T) {
	base := policyDialog(t, `{"chat_id":7,"space_id":1}`)
	for _, raw := range []string{`{"chat_id":7,"space_id":2}`, `{"chat_id":7,"space_id":1,"archived":true}`, `{"chat_id":7,"space_id":1,"structure":{"kind":"channel","peer_user_id":null,"space_id":null,"parent_chat_id":null,"parent_message_id":null}}`, `{"chat_id":7,"space_id":1,"emoji":"x"}`} {
		if sameDialogRecord(base, policyDialog(t, raw)) {
			t.Errorf("presentation change ignored: %s", raw)
		}
	}
}

func TestFoundationConflictingPeerNeverDirect(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		peer := int64(hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64)))
		parent := int64(hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64)))
		raw, _ := json.Marshal(map[string]any{"chat_id": 7, "peer_user_id": peer, "parent_chat_id": parent, "structure": map[string]any{"kind": "direct", "peer_user_id": peer, "parent_chat_id": parent, "space_id": nil, "parent_message_id": nil}})
		var d sidecar.DialogRecord
		if err := json.Unmarshal(raw, &d); err != nil {
			ht.Fatal(err)
		}
		if isDMDialog(d) {
			ht.Fatal("contradictory parent and peer classified direct")
		}
	}, hegel.WithTestCases(2000), hegel.WithDatabase(t.TempDir()))
}

func TestPartialDialogPreservesEstablishedStructure(t *testing.T) {
	ic := &InlineClient{}
	known := sidecar.DialogRecord{ChatID: 7, SpaceID: foundationInt64(3), Structure: &sidecar.DialogStructure{Kind: "channel", SpaceID: foundationInt64(3)}}
	ic.rememberDialog(known)
	ic.rememberDialog(sidecar.DialogRecord{ChatID: 7, Title: stringPtr("new name")})
	got, _ := ic.cachedDialog(7)
	if classifyDialog(got) != roomChannel || got.Structure.SpaceID == nil || *got.Structure.SpaceID != 3 || got.Title == nil || *got.Title != "new name" {
		t.Fatalf("partial patch lost established structure: %+v", got)
	}
	ic.rememberDialog(sidecar.DialogRecord{ChatID: 7, Structure: &sidecar.DialogStructure{Kind: "channel"}})
	got, _ = ic.cachedDialog(7)
	if got.Structure.SpaceID != nil || got.SpaceID != nil {
		t.Fatal("complete home descriptor did not detach")
	}
}

func foundationInt64(n int64) *int64 { return &n }

func TestClassificationCoversAuthoritativeDomain(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		chat := int64(hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64)))
		peer := int64(hegel.Draw(ht, hegel.Integers[int64](1, math.MaxInt64)))
		parent := chat ^ 1
		if parent == 0 {
			parent = 2
		}
		hidden, archived := hegel.Draw(ht, hegel.Booleans()), hegel.Draw(ht, hegel.Booleans())
		kind := hegel.Draw(ht, hegel.SampledFrom([]string{"direct", "channel", "reply_child"}))
		s := &sidecar.DialogStructure{Kind: kind}
		want := roomChannel
		switch kind {
		case "direct":
			s.PeerUserID = &peer
			want = roomDirect
		case "reply_child":
			s.ParentChatID = &parent
			s.ParentMessageID = &peer
			want = roomReplyChild
		}
		if kind != "direct" && hegel.Draw(ht, hegel.Booleans()) {
			s.SpaceID = &peer
		}
		d := sidecar.DialogRecord{ChatID: chat, Structure: s, ChatListHidden: &hidden, Archived: &archived}
		if classifyDialog(d) != want {
			ht.Fatal("valid source descriptor rejected")
		}
		d.ChatID = 0
		if classifyDialog(d) != roomUnknown {
			ht.Fatal("invalid source chat accepted")
		}
		d.ChatID = chat
		d.Structure = nil
		if classifyDialog(d) != roomUnknown {
			ht.Fatal("old record absence invented structure")
		}
		d.Structure = s
		switch kind {
		case "direct":
			s.ParentChatID = &parent
		case "channel":
			s.PeerUserID = &peer
		case "reply_child":
			s.ParentChatID = nil
		}
		if classifyDialog(d) != roomUnknown {
			ht.Fatal("contradictory or incomplete structural record accepted")
		}
	}, hegel.WithTestCases(3000), hegel.WithDatabase(t.TempDir()))
}

func TestParentOnlyChildIsSourceBackedSeparateRoom(t *testing.T) {
	d := sidecar.DialogRecord{ChatID: 7, Structure: &sidecar.DialogStructure{Kind: "reply_child", ParentChatID: foundationInt64(6)}}
	if classifyDialog(d) != roomReplyChild {
		t.Fatal("valid linked child without reply anchor became unknown")
	}
}
