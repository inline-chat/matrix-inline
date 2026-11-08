package connector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const (
	directoryPageSize          = 100
	directoryMaxPages          = 4
	directoryParticipantBudget = 8
	directoryRefreshTimeout    = 20 * time.Second
)

var errDirectoryNotFound = errors.New("Inline user has no eligible directory entry; lookup does not establish reachability")
var errDirectoryAmbiguous = errors.New("Inline username is ambiguous")
var errDirectoryIncomplete = errors.New("Inline directory refresh was bounded; lookup may be incomplete")

// Fresh means this user was returned as a participant by a successful live
// ChatParticipants read. False means source-backed cached discovery only.
// Neither value proves permission to create, edit, delete or manage membership.
type directoryUser struct {
	Record sidecar.UserRecord
	Fresh  bool
}

// directorySnapshot is request-local, never a global cache. Do not reuse across
// logins or persist it as permission evidence. AccountState members and Dialogs
// rows are cached even after Dialogs refreshes: its response is store-backed.
type directorySnapshot struct {
	users      map[int64]directoryUser
	spaces     map[int64]sidecar.SpaceRecord
	access     map[int64]string
	members    map[int64]map[int64]bool
	receiver   networkid.UserLoginID
	namespace  string
	incomplete bool
}

// loadDirectory performs at most four dialog pages, one account-state read and
// eight live participant reads within twenty seconds. It returns a bounded
// subset, never fills missing users from arbitrary ic.users records and never
// falls back after a source error. The sidecar client owns namespace routing.
// Hidden/deleted dialogs alone are excluded from discovery; independent
// workspace membership or live participants can still supply eligible users.
func (ic *InlineClient) loadDirectory(ctx context.Context) (*directorySnapshot, error) {
	if ic.Sidecar == nil || strings.TrimSpace(ic.Sidecar.SessionNamespace) == "" || ic.UserLogin == nil || ic.UserLogin.UserLogin == nil || ic.UserLogin.ID == "" {
		return nil, errors.New("Inline directory requires a scoped login")
	}
	ctx, cancel := context.WithTimeout(ctx, directoryRefreshTimeout)
	defer cancel()
	d := &directorySnapshot{users: map[int64]directoryUser{}, spaces: map[int64]sidecar.SpaceRecord{}, access: map[int64]string{}, members: map[int64]map[int64]bool{}, receiver: ic.UserLogin.ID, namespace: ic.Sidecar.SessionNamespace}
	var dialogs []sidecar.DialogRecord
	records := map[int64]sidecar.UserRecord{}
	addRecords := func(users []sidecar.UserRecord) {
		for _, u := range users {
			if u.UserID > 0 {
				records[u.UserID] = u
			}
		}
	}
	cursor := ""
	seen := map[string]bool{}
	for page := 0; page < directoryMaxPages; page++ {
		limit := uint32(directoryPageSize)
		result, err := ic.Sidecar.Dialogs(ctx, sidecar.DialogsRequest{Limit: &limit, Cursor: cursor, Order: sidecar.DialogsOrderStableChatID})
		if err != nil {
			return nil, fmt.Errorf("refresh Inline directory dialogs: %w", err)
		}
		dialogs = append(dialogs, result.Dialogs...)
		addRecords(result.Users)
		cursor = result.NextCursor
		if cursor == "" {
			break
		}
		if seen[cursor] {
			d.incomplete = true
			break
		}
		seen[cursor] = true
		if page == directoryMaxPages-1 {
			d.incomplete = true
		}
	}
	// Read after refresh: getChats can update workspace access and tombstones.
	state, err := ic.Sidecar.AccountState(ctx)
	if err != nil {
		return nil, fmt.Errorf("read Inline directory access: %w", err)
	}
	addRecords(state.Users)
	for _, s := range state.Spaces {
		if s.SpaceID > 0 {
			d.spaces[s.SpaceID] = s
		}
	}
	for _, a := range state.SpaceAccess {
		// Conflicting evidence is unknown, never whichever row happened to be last.
		if prev, ok := d.access[a.SpaceID]; ok && prev != a.Status {
			d.access[a.SpaceID] = "unknown"
		} else {
			d.access[a.SpaceID] = a.Status
		}
	}
	allow := func(id int64, fresh bool) {
		if u, ok := records[id]; ok && id > 0 {
			old := d.users[id]
			d.users[id] = directoryUser{Record: u, Fresh: old.Fresh || fresh}
		}
	}
	for _, m := range state.SpaceMembers {
		if m.UserID <= 0 || m.SpaceID <= 0 || d.access[m.SpaceID] != "accessible" {
			continue
		}
		if d.members[m.SpaceID] == nil {
			d.members[m.SpaceID] = map[int64]bool{}
		}
		d.members[m.SpaceID][m.UserID] = true
		allow(m.UserID, false)
	}
	deleted := map[int64]bool{}
	for _, id := range state.DeletedChatIDs {
		deleted[id] = true
	}
	sort.Slice(dialogs, func(i, j int) bool { return dialogs[i].ChatID < dialogs[j].ChatID })
	seenChats := map[int64]bool{}
	reads := 0
	for _, dialog := range dialogs {
		kind := classifyDialog(dialog)
		if seenChats[dialog.ChatID] || deleted[dialog.ChatID] || (dialog.ChatListHidden != nil && *dialog.ChatListHidden) || kind == roomUnknown {
			continue
		}
		seenChats[dialog.ChatID] = true
		if kind == roomDirect {
			allow(*dialog.Structure.PeerUserID, false)
			continue
		}
		// A known chat can retain independent access after workspace removal. Only
		// a successful live participant read establishes that bounded subset.
		if reads == directoryParticipantBudget {
			d.incomplete = true
			continue
		}
		reads++
		page, err := ic.Sidecar.ChatParticipants(ctx, sidecar.ChatParticipantsRequest{ChatID: dialog.ChatID})
		if err != nil {
			return nil, fmt.Errorf("refresh Inline directory participants for %d: %w", dialog.ChatID, err)
		}
		addRecords(page.Users)
		for _, p := range page.Participants {
			allow(p.UserID, true)
		}
	}
	return d, nil
}

func directoryPositiveID(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	id, err := strconv.ParseInt(s, 10, 64)
	return id, err == nil && id > 0
}
func directoryUsername(s string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(s), "@"))
}

func directoryNumericShape(s string) bool {
	digits := strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	return digits != "" && strings.Trim(digits, "0123456789") == ""
}

// resolve accepts an exact positive decimal ID or canonical username (optional
// @, case-insensitive). IDs are never substring matched or synthesized. A
// bounded/truncated snapshot cannot prove a username unique across the account.
func (d *directorySnapshot) resolve(identifier string) (directoryUser, error) {
	if d == nil {
		return directoryUser{}, errDirectoryNotFound
	}
	identifier = strings.TrimSpace(identifier)
	if id, ok := directoryPositiveID(identifier); ok {
		if user, found := d.users[id]; found {
			return user, nil
		}
		if d.incomplete {
			return directoryUser{}, errDirectoryIncomplete
		}
		return directoryUser{}, errDirectoryNotFound
	}
	name := directoryUsername(identifier)
	// A malformed numeric ID must not fall through to a coincidentally numeric
	// username (including zero, negative values and int64 overflow).
	if directoryNumericShape(name) {
		return directoryUser{}, errDirectoryNotFound
	}
	if name == "" || strings.ContainsAny(name, "@+ \t\r\n") {
		return directoryUser{}, errDirectoryNotFound
	}
	var found directoryUser
	count := 0
	for _, user := range d.users {
		if user.Record.Username != nil && directoryUsername(*user.Record.Username) == name {
			count++
			found = user
		}
	}
	if count > 1 {
		return directoryUser{}, errDirectoryAmbiguous
	}
	if d.incomplete {
		return directoryUser{}, errDirectoryIncomplete
	}
	if count == 1 {
		return found, nil
	}
	return directoryUser{}, errDirectoryNotFound
}

// search pages the same accessible subset in ascending numeric user ID order.
// Numeric queries match exact IDs only; text queries match username or names.
func (d *directorySnapshot) search(query string, offset, limit int) ([]directoryUser, error) {
	if offset < 0 || limit < 1 || limit > directoryPageSize {
		return nil, fmt.Errorf("Inline directory requires offset >= 0 and limit 1..%d", directoryPageSize)
	}
	result := []directoryUser{}
	if d == nil {
		return result, nil
	}
	query = directoryUsername(query)
	numericID, numeric := directoryPositiveID(query)
	if !numeric && directoryNumericShape(query) {
		return result, nil
	}
	for id, u := range d.users {
		match := query == ""
		if numeric {
			match = id == numericID
		} else if !match {
			for _, field := range []*string{u.Record.Username, u.Record.DisplayName, u.Record.FirstName, u.Record.LastName} {
				if field != nil && strings.Contains(strings.ToLower(*field), query) {
					match = true
					break
				}
			}
		}
		if match {
			result = append(result, u)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Record.UserID < result[j].Record.UserID })
	if offset >= len(result) {
		return []directoryUser{}, nil
	}
	result = result[offset:]
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

// SearchUsers and GetContactList expose at most 100 results. These framework
// responses have no freshness field; callers needing provenance use the snapshot.
func (ic *InlineClient) SearchUsers(ctx context.Context, query string) ([]*bridgev2.ResolveIdentifierResponse, error) {
	d, err := ic.loadDirectory(ctx)
	if err != nil {
		return nil, err
	}
	users, err := d.search(query, 0, directoryPageSize)
	if err != nil {
		return nil, err
	}
	results := make([]*bridgev2.ResolveIdentifierResponse, 0, len(users))
	for _, u := range users {
		results = append(results, &bridgev2.ResolveIdentifierResponse{UserID: makeUserID(strconv.FormatInt(u.Record.UserID, 10)), UserInfo: userInfoFromRecord(u.Record)})
	}
	return results, nil
}

// GetContactList serves the framework compatibility endpoint as a bounded
// accessible subset. ContactList remains unadvertised because this is not a
// complete contacts snapshot; clients should prefer the search capability.
func (ic *InlineClient) GetContactList(ctx context.Context) ([]*bridgev2.ResolveIdentifierResponse, error) {
	return ic.SearchUsers(ctx, "")
}

// validateThreadCreation is pure preflight, NOT positive authorization. The
// caller must issue CreateThread on the same scoped sidecar and propagate its
// current permission error. spaceThreadGuards checks current workspace privacy,
// public-chat access and active participants; cached Role/Creator never grant it.
// Empty/thread/channel/private_thread create private ordinary channels;
// public_thread requires a workspace and no participants. True group DMs reject.
// IDs returned here are explicit deduplicated invitees; the source adds self.
func validateThreadCreation(params *bridgev2.GroupCreateParams, receiver networkid.UserLoginID, d *directorySnapshot) (sidecar.CreateThreadRequest, []networkid.UserID, error) {
	var request sidecar.CreateThreadRequest
	fail := func(message string) (sidecar.CreateThreadRequest, []networkid.UserID, error) {
		return sidecar.CreateThreadRequest{}, nil, errors.New(message)
	}
	if params == nil || d == nil || receiver == "" || d.receiver != receiver || d.namespace == "" {
		return fail("Inline creation requires directory facts for this login")
	}
	switch params.Type {
	case "", "thread", "channel", "private_thread":
	case "public_thread":
		request.IsPublic = true
	default:
		return fail("unsupported Inline creation type: " + params.Type)
	}
	if params.Username != "" || params.Avatar != nil || params.Disappear != nil {
		return fail("Inline thread username, avatar and disappearing metadata are unsupported")
	}
	if params.Parent != nil {
		if params.Parent.Receiver != receiver || !strings.HasPrefix(string(params.Parent.ID), "space:") {
			return fail("Inline parent must be a workspace belonging to this login")
		}
		space, ok := directoryPositiveID(strings.TrimPrefix(string(params.Parent.ID), "space:"))
		if !ok || d.access[space] != "accessible" {
			return fail("Inline workspace access unavailable")
		}
		if _, ok = d.spaces[space]; !ok {
			return fail("Inline workspace metadata unavailable")
		}
		request.SpaceID = &space
	}
	if request.IsPublic && request.SpaceID == nil {
		return fail("public Inline home threads are unsupported")
	}
	if request.IsPublic && len(params.Participants) > 0 {
		return fail("public Inline workspace threads require empty participants")
	}
	if !request.IsPublic && len(params.Participants) == 0 {
		return fail("private Inline threads require participants")
	}
	ids := []networkid.UserID{}
	seen := map[int64]bool{}
	for _, participant := range params.Participants {
		id, ok := directoryPositiveID(string(participant))
		if !ok {
			return fail("Inline participants must be exact positive user IDs")
		}
		user, err := d.resolve(strconv.FormatInt(id, 10))
		if err != nil {
			return sidecar.CreateThreadRequest{}, nil, fmt.Errorf("Inline participant %q: %w", participant, err)
		}
		if request.SpaceID != nil && !d.members[*request.SpaceID][user.Record.UserID] {
			return fail("Inline participant has no workspace membership evidence")
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		request.Participants = append(request.Participants, sidecar.ChatCreateParticipant{UserID: id})
		ids = append(ids, makeUserID(strconv.FormatInt(id, 10)))
	}
	if params.Name != nil {
		request.Title = optionalString(params.Name.Name)
	}
	if params.Topic != nil {
		request.Description = optionalString(params.Topic.Topic)
	}
	return request, ids, nil
}
