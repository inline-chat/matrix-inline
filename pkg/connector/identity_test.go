package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestConversationIdentityDoesNotFollowMemberCount(t *testing.T) {
	client := &InlineClient{AccountID: "1", UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "1"}}}
	peer := int64(2)
	for _, test := range []struct {
		name   string
		dialog sidecar.DialogRecord
		kind   *database.RoomType
	}{
		{"legacy unknown", sidecar.DialogRecord{ChatID: 7}, nil},
		{"direct", sidecar.DialogRecord{ChatID: 7, Kind: sidecar.ConversationDirectMessage, PeerUserID: &peer}, roomTypePtr(database.RoomTypeDM)},
		{"channel", sidecar.DialogRecord{ChatID: 7, Kind: sidecar.ConversationChat}, roomTypePtr(database.RoomTypeDefault)},
		{"private child", sidecar.DialogRecord{ChatID: 8, Kind: sidecar.ConversationChat, ParentChatID: identityInt64Ptr(7)}, roomTypePtr(database.RoomTypeDefault)},
	} {
		t.Run(test.name, func(t *testing.T) {
			info := client.chatInfoForDialog(t.Context(), test.dialog, nil)
			if (info.Type == nil) != (test.kind == nil) || (info.Type != nil && *info.Type != *test.kind) {
				t.Fatalf("classification = %v", info.Type)
			}
			if info.ParentID != nil {
				t.Fatal("child context creates a parent navigation link")
			}
		})
	}
	created := client.chatInfoForCreatedChat(t.Context(), &sidecar.CreatedChat{ChatID: 8}, []networkid.UserID{"2"})
	if created.Type == nil || *created.Type != database.RoomTypeDefault || created.Members.OtherUserID != "" {
		t.Fatal("two-person ordinary chat classified as DM")
	}
}

func roomTypePtr(value database.RoomType) *database.RoomType { return &value }

func TestAuthoritativeDetachAndStaleStructure(t *testing.T) {
	seq := int32(8)
	oldSeq := int32(7)
	original := sidecar.DialogRecord{ChatID: 7, Kind: sidecar.ConversationChat, StructureKnown: true, StructureSeq: &seq, SpaceID: identityInt64Ptr(3), ParentChatID: identityInt64Ptr(1)}
	partial := mergeDialogStructure(original, sidecar.DialogRecord{ChatID: 7})
	if partial.SpaceID == nil || partial.ParentChatID == nil || partial.Kind != sidecar.ConversationChat {
		t.Fatal("partial refresh detached source structure")
	}
	stale := mergeDialogStructure(original, sidecar.DialogRecord{ChatID: 7, Kind: sidecar.ConversationChat, StructureKnown: true, StructureSeq: &oldSeq})
	if stale.SpaceID == nil || stale.ParentChatID == nil {
		t.Fatal("stale replay detached source structure")
	}
	unversioned := mergeDialogStructure(original, sidecar.DialogRecord{ChatID: 7, Kind: sidecar.ConversationChat, StructureKnown: true})
	if unversioned.SpaceID == nil || unversioned.ParentChatID == nil {
		t.Fatal("unversioned snapshot erased versioned source structure")
	}
	next := int32(9)
	detached := mergeDialogStructure(original, sidecar.DialogRecord{ChatID: 7, Kind: sidecar.ConversationChat, StructureKnown: true, StructureSeq: &next})
	if detached.SpaceID != nil || detached.ParentChatID != nil {
		t.Fatal("canonical detach retained cached structure")
	}
}

type identityStateSend struct {
	room    id.RoomID
	typ     event.Type
	key     string
	content *event.Content
}
type identityMatrixAPI struct {
	bridgev2.MatrixAPI
	states  []identityStateSend
	failure error
}

func (api *identityMatrixAPI) GetMXID() id.UserID { return "@bridge:example.org" }
func (api *identityMatrixAPI) SendState(_ context.Context, room id.RoomID, typ event.Type, key string, content *event.Content, _ time.Time) (*mautrix.RespSendEvent, error) {
	if api.failure != nil {
		return nil, api.failure
	}
	api.states = append(api.states, identityStateSend{room, typ, key, content})
	return &mautrix.RespSendEvent{EventID: "$state"}, nil
}

type identityMatrixConnector struct {
	bridgev2.MatrixConnector
	api         *identityMatrixAPI
	topicSender id.UserID
	topic       string
}

func (matrix *identityMatrixConnector) Init(*bridgev2.Bridge)         {}
func (matrix *identityMatrixConnector) BotIntent() bridgev2.MatrixAPI { return matrix.api }
func (matrix *identityMatrixConnector) ServerName() string            { return "example.org" }
func (matrix *identityMatrixConnector) GetCapabilities() *bridgev2.MatrixCapabilities {
	return &bridgev2.MatrixCapabilities{}
}
func (matrix *identityMatrixConnector) GetStateEvent(_ context.Context, _ id.RoomID, typ event.Type, _ string) (*event.Event, error) {
	sender := matrix.topicSender
	if sender == "" {
		sender = matrix.api.GetMXID()
	}
	return &event.Event{Type: typ, Sender: sender, Content: event.Content{Parsed: &event.TopicEventContent{Topic: matrix.topic}}}, nil
}
func identityTestBridge(t *testing.T, db *database.Database) (*bridgev2.Bridge, *identityMatrixAPI) {
	t.Helper()
	api := &identityMatrixAPI{}
	bridge := bridgev2.NewBridge(db.BridgeID, db.Database, zerolog.Nop(), &bridgeconfig.BridgeConfig{SplitPortals: true}, &identityMatrixConnector{api: api}, &InlineConnector{}, func(*bridgev2.Bridge) bridgev2.CommandProcessor { return nil })
	bridge.BackgroundCtx = t.Context()
	return bridge, api
}

func TestClassificationRepairUsesFrameworkWithoutChangingRoomOrMessages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	db := openTestBridgeDatabase(t, path)
	bridge, api := identityTestBridge(t, db)
	key := networkid.PortalKey{ID: "7", Receiver: "1"}
	stored := &database.Portal{PortalKey: key, MXID: "!original:example.org", RoomType: database.RoomTypeGroupDM, Topic: "My topic", TopicSet: true, Metadata: &PortalMetadata{}}
	if err := db.Portal.Insert(t.Context(), stored); err != nil {
		t.Fatal(err)
	}
	if err := db.Ghost.Insert(t.Context(), &database.Ghost{ID: "2"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Message.Insert(t.Context(), &database.Message{ID: "7/11", Room: key, SenderID: "2", MXID: "$original", Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	portal := &bridgev2.Portal{Portal: stored, Bridge: bridge}
	ordinary := database.RoomTypeDefault
	if preserveExistingRoomClassification(portal, &bridgev2.ChatInfo{Type: &ordinary}).Type != nil {
		t.Fatal("normal refresh applied an unrequested repair")
	}
	info, err := classificationRepairInfo(t.Context(), portal)
	if err != nil {
		t.Fatal(err)
	}
	portal.UpdateInfo(t.Context(), info, nil, nil, time.Time{})
	if len(api.states) != 2 {
		t.Fatalf("bridge info events=%d", len(api.states))
	}
	for _, state := range api.states {
		content, ok := state.content.Parsed.(*event.BridgeEventContent)
		if !ok || content.BeeperRoomType != "" || content.BeeperRoomTypeV2 != "" {
			t.Fatalf("wrong repaired Beeper metadata: %#v", state.content.Parsed)
		}
	}
	db.RawDB.Close()
	db = openTestBridgeDatabase(t, path)
	restored, err := db.Portal.GetByKey(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if restored.MXID != "!original:example.org" || restored.RoomType != database.RoomTypeDefault || restored.Topic != "My topic" {
		t.Fatalf("repair changed unrelated state: %+v", restored)
	}
	meta := restored.Metadata.(*PortalMetadata)
	if meta.ClassificationRepair == nil || meta.ClassificationRepair.PreviousType != database.RoomTypeGroupDM {
		t.Fatal("missing saved previous classification")
	}
	message, err := db.Message.GetFirstPartByID(t.Context(), "1", "7/11")
	if err != nil || message == nil || message.MXID != "$original" {
		t.Fatalf("message changed: %+v %v", message, err)
	}
}

func TestChildTopicPreservesUserChoiceAndAuthoritativeDetach(t *testing.T) {
	client := &InlineClient{}
	portal := testPortal("8")
	portal.Metadata = &PortalMetadata{ChildTopicOwned: true, OwnedChildTopic: "Old parent"}
	portal.Topic = "Old parent"
	if !client.updateChildContext(t.Context(), portal, sidecar.DialogRecord{ChatID: 8, StructureKnown: true}) || portal.Topic != "" {
		t.Fatal("authoritative detach retained bridge-owned topic")
	}
	portal.Topic = "My topic"
	client.updateChildContext(t.Context(), portal, sidecar.DialogRecord{ChatID: 8, StructureKnown: true, Description: stringPtr("Source description")})
	if portal.Topic != "My topic" {
		t.Fatal("source changed user topic")
	}
	_, err := client.HandleMatrixRoomTopic(t.Context(), &bridgev2.MatrixRoomTopic{MatrixEventBase: bridgev2.MatrixEventBase[*event.TopicEventContent]{Portal: portal, Content: &event.TopicEventContent{Topic: ""}}})
	if err != nil {
		t.Fatal(err)
	}
	client.updateChildContext(t.Context(), portal, sidecar.DialogRecord{ChatID: 8, StructureKnown: true, Description: stringPtr("Source description")})
	if portal.Topic != "" {
		t.Fatal("user-cleared topic was replaced")
	}
	portal.Metadata = &PortalMetadata{ChildTopicOwned: true, OwnedChildTopic: "Known parent"}
	portal.Topic = "Known parent"
	client.updateChildContext(t.Context(), portal, sidecar.DialogRecord{ChatID: 8, StructureKnown: true, ParentChatID: identityInt64Ptr(7)})
	if portal.Topic != "Known parent" {
		t.Fatal("temporary missing parent erased context")
	}
}

func TestChildContextOnlyWritesAccessibleChildAndKeepsIndependentIDs(t *testing.T) {
	db := testBridgeDatabase(t)
	bridge, api := identityTestBridge(t, db)
	login := &database.UserLogin{ID: "1", UserMXID: "@owner:example.org", Metadata: &UserLoginMetadata{AccountID: "1"}}
	if err := db.User.Insert(t.Context(), &database.User{MXID: login.UserMXID}); err != nil {
		t.Fatal(err)
	}
	if err := db.UserLogin.Insert(t.Context(), login); err != nil {
		t.Fatal(err)
	}
	parentKey := networkid.PortalKey{ID: "7", Receiver: "1"}
	childKey := networkid.PortalKey{ID: "8", Receiver: "1"}
	for _, record := range []*database.Portal{{PortalKey: parentKey, MXID: "!parent:example.org", Name: "Parent"}, {PortalKey: childKey, MXID: "!child:example.org"}} {
		if err := db.Portal.Insert(t.Context(), record); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.UserPortal.GetOrCreate(t.Context(), login, parentKey); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/chat/participants" {
			t.Fatalf("unexpected %s", r.URL.Path)
		}
		writeConnectorSidecarResult(t, w, "chat_participants", sidecar.ChatParticipantsPage{})
	}))
	defer server.Close()
	client := &InlineClient{UserLogin: &bridgev2.UserLogin{UserLogin: login, Bridge: bridge}, AccountID: "1", Sidecar: sidecar.NewClient(server.URL), dialogs: map[int64]sidecar.DialogRecord{7: {ChatID: 7, Kind: sidecar.ConversationChat}}}
	child, err := bridge.GetExistingPortalByKey(t.Context(), childKey)
	if err != nil {
		t.Fatal(err)
	}
	if !client.updateChildContext(t.Context(), child, sidecar.DialogRecord{ChatID: 8, Kind: sidecar.ConversationChat, StructureKnown: true, ParentChatID: identityInt64Ptr(7)}) {
		t.Fatal("child context not updated")
	}
	if !strings.Contains(child.Topic, "!parent:example.org") || len(api.states) != 1 || api.states[0].room != child.MXID || child.ParentKey.ID != "" {
		t.Fatalf("context leaked or missing: %+v", api.states)
	}
	// Another account's cached portal is insufficient without that login's mapping.
	client.UserLogin.ID = "2"
	if parent, err := client.accessibleMappedParent(t.Context(), 7); err != nil || parent != nil {
		t.Fatalf("foreign mapping accepted: %+v %v", parent, err)
	}
}

func TestWorkspaceRevocationSurvivesReopenAndRetainsIndependentRoom(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	db := openTestBridgeDatabase(t, path)
	bridge, api := identityTestBridge(t, db)
	login := &database.UserLogin{ID: "1", UserMXID: "@owner:example.org", Metadata: &UserLoginMetadata{AccountID: "1", WorkspaceNavigation: map[string]*WorkspaceNavigation{"3": {SpaceID: 3, Name: "Work", Active: true, RoomID: "!space:example.org", Links: map[string]WorkspaceRoomLink{"7": {RoomID: "!chat:example.org", Attached: true}}}}}}
	if err := db.User.Insert(t.Context(), &database.User{MXID: login.UserMXID}); err != nil {
		t.Fatal(err)
	}
	if err := db.UserLogin.Insert(t.Context(), login); err != nil {
		t.Fatal(err)
	}
	key := networkid.PortalKey{ID: "7", Receiver: "1"}
	if err := db.Portal.Insert(t.Context(), &database.Portal{PortalKey: key, MXID: "!chat:example.org"}); err != nil {
		t.Fatal(err)
	}
	client := &InlineClient{AccountID: "1", UserLogin: &bridgev2.UserLogin{UserLogin: login, Bridge: bridge}}
	api.failure = errors.New("temporary Matrix outage")
	if err := client.reconcileWorkspaceNavigation(t.Context(), &sidecar.AccountStateSnapshot{RemovedSpaceIDs: []int64{3}}); err == nil {
		t.Fatal("failed navigation write hidden")
	}
	db.RawDB.Close()
	db = openTestBridgeDatabase(t, path)
	bridge, api = identityTestBridge(t, db)
	restored, err := db.UserLogin.GetByID(t.Context(), "1")
	if err != nil {
		t.Fatal(err)
	}
	client = &InlineClient{AccountID: "1", UserLogin: &bridgev2.UserLogin{UserLogin: restored, Bridge: bridge}}
	saved := client.workspaceNavigationSnapshot()["3"]
	if saved.Active || len(saved.Links) != 1 {
		t.Fatal("revocation/pending cleanup lost at restart")
	}
	client.retryWorkspaceLinks(t.Context())
	if len(api.states) != 1 || api.states[0].room != "!space:example.org" || len(api.states[0].content.Parsed.(*event.SpaceChildEventContent).Via) != 0 {
		t.Fatal("did not detach revoked navigation")
	}
	portal, err := db.Portal.GetByKey(t.Context(), key)
	if err != nil || portal == nil || portal.MXID != "!chat:example.org" {
		t.Fatal("workspace leave deleted independent room")
	}
}

func TestWorkspaceMembershipIsAccountScopedAndIncompleteRefreshRetainsStructure(t *testing.T) {
	spaces := map[string]*WorkspaceNavigation{"3": {SpaceID: 3, Active: true}}
	applyWorkspaceAccess(spaces, &sidecar.AccountStateSnapshot{}, 1)
	if !spaces["3"].Active {
		t.Fatal("incomplete refresh revoked membership")
	}
	applyWorkspaceAccess(spaces, &sidecar.AccountStateSnapshot{Spaces: []sidecar.SpaceRecord{{SpaceID: 4, Name: "Other"}}, SpaceMemberships: []sidecar.SpaceMemberRecord{{SpaceID: 4, UserID: 2}}}, 1)
	if spaces["4"] != nil {
		t.Fatal("foreign account membership created space")
	}
	applyWorkspaceAccess(spaces, &sidecar.AccountStateSnapshot{Spaces: []sidecar.SpaceRecord{{SpaceID: 5, Name: "Known"}}, SpaceMemberships: []sidecar.SpaceMemberRecord{{SpaceID: 5, UserID: 1}}}, 1)
	if spaces["5"] == nil || !spaces["5"].Active {
		t.Fatal("one incomplete workspace blocked a separately proven membership")
	}
	applyWorkspaceAccess(spaces, &sidecar.AccountStateSnapshot{SpacesComplete: true}, 1)
	if spaces["3"].Active || spaces["5"].Active {
		t.Fatal("verified workspace leave was ignored")
	}
}

func TestSourceDirectoryRejectsCachedUsersAndAmbiguousNames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rpc/users":
			writeConnectorSidecarResult(t, w, "users", sidecar.UsersPage{})
		case "/rpc/users/search":
			writeConnectorSidecarResult(t, w, "users", sidecar.UsersPage{Users: []sidecar.UserRecord{{UserID: 2, Username: stringPtr("Ada")}, {UserID: 3, Username: stringPtr("ada")}}})
		default:
			t.Fatalf("unexpected %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := &InlineClient{Sidecar: sidecar.NewClient(server.URL), users: map[int64]sidecar.UserRecord{2: {UserID: 2, Username: stringPtr("ada")}}}
	if _, err := client.ResolveIdentifier(t.Context(), "2", false); err == nil {
		t.Fatal("cached presence granted source-account lookup")
	}
	if _, err := client.ResolveIdentifier(t.Context(), "@ADA", false); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("duplicate username selected arbitrary user: %v", err)
	}
}

func TestCreateChannelPreservesWorkspaceVisibilityAndValidatesBeforeMutation(t *testing.T) {
	var requests []sidecar.CreateThreadRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rpc/chat/create-thread" {
			t.Fatalf("unexpected %s", r.URL.Path)
		}
		var request sidecar.CreateThreadRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, request)
		writeConnectorSidecarResult(t, w, "created_chat", sidecar.CreatedChat{ChatID: 7, Title: request.Title})
	}))
	defer server.Close()
	client := &InlineClient{AccountID: "1", UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "1"}}, Sidecar: sidecar.NewClient(server.URL)}
	for _, kind := range []string{"public_channel", "private_channel"} {
		response, err := client.CreateGroup(t.Context(), &bridgev2.GroupCreateParams{Type: kind, Name: &event.RoomNameEventContent{Name: "Project"}, Parent: &networkid.PortalKey{ID: client.workspacePortalID(3), Receiver: "1"}, Participants: []networkid.UserID{"2"}})
		if err != nil {
			t.Fatal(err)
		}
		if response.PortalInfo.Type == nil || *response.PortalInfo.Type != database.RoomTypeDefault || response.PortalInfo.Members.OtherUserID != "" {
			t.Fatal("created workspace channel looks like DM")
		}
	}
	if len(requests) != 2 || !requests[0].IsPublic || requests[1].IsPublic || requests[0].SpaceID == nil || *requests[0].SpaceID != 3 || requests[1].SpaceID == nil || *requests[1].SpaceID != 3 {
		t.Fatalf("wrong creation payload: %+v", requests)
	}
	if _, err := client.CreateGroup(t.Context(), &bridgev2.GroupCreateParams{Type: "thread", Name: &event.RoomNameEventContent{Name: "Child"}, Parent: &networkid.PortalKey{ID: "7"}}); err == nil {
		t.Fatal("native child creation flattened into ordinary chat")
	}
	if _, err := client.CreateGroup(t.Context(), &bridgev2.GroupCreateParams{Type: "public_channel", Name: &event.RoomNameEventContent{Name: "No workspace"}}); err == nil {
		t.Fatal("public workspace channel accepted without workspace")
	}
	if len(requests) != 2 {
		t.Fatal("invalid shape reached source mutation")
	}
}

func identityInt64Ptr(value int64) *int64 { return &value }

func TestOfflineUserClearedTopicIsNotReplaced(t *testing.T) {
	db := testBridgeDatabase(t)
	bridge, _ := identityTestBridge(t, db)
	matrix := bridge.Matrix.(*identityMatrixConnector)
	matrix.topicSender = "@owner:example.org"
	portal := testPortal("8")
	portal.MXID = "!child:example.org"
	portal.Bridge = bridge
	portal.Topic = "Old bridge context"
	portal.Metadata = &PortalMetadata{ChildTopicOwned: true, OwnedChildTopic: portal.Topic}
	client := &InlineClient{}
	client.updateChildContext(t.Context(), portal, sidecar.DialogRecord{ChatID: 8, StructureKnown: true, Description: stringPtr("New source description")})
	if portal.Topic != "" || !portal.Metadata.(*PortalMetadata).UserTopic {
		t.Fatal("offline user-cleared topic was overwritten")
	}
}
