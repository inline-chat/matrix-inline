package connector

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const inlineDirectoryLimit uint32 = 50

var _ bridgev2.UserSearchingNetworkAPI = (*InlineClient)(nil)

func canonicalInlineUsername(value string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(value), "@"))
}

func (ic *InlineClient) resolveInlineIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return nil, fmt.Errorf("enter an Inline user ID or canonical username")
	}
	var user sidecar.UserRecord
	if userID, err := parseInlineUserID(identifier); err == nil {
		if createChat {
			// Exact source IDs can go directly to the authoritative mutation. Requiring
			// a complete directory would wrongly reject valid users in large accounts.
			chat, err := ic.createDMWithUserID(ctx, userID)
			if err != nil {
				return nil, err
			}
			return &bridgev2.ResolveIdentifierResponse{UserID: makeUserID(strconv.FormatInt(userID, 10)), UserInfo: ic.userInfoForID(userID), Chat: chat}, nil
		}
		page, err := ic.Sidecar.Users(ctx, sidecar.UsersRequest{UserIDs: []int64{userID}})
		if err != nil {
			return nil, fmt.Errorf("Inline user lookup failed: %w", err)
		}
		for _, candidate := range page.Users {
			if candidate.UserID == userID {
				user = candidate
				break
			}
		}
		if user.UserID == 0 {
			return nil, fmt.Errorf("Inline user %d is not available to this account", userID)
		}
	} else {
		username := canonicalInlineUsername(identifier)
		if strings.ContainsAny(username, " @+:/\\\t\r\n") || utf8.RuneCountInString(username) > 100 {
			return nil, fmt.Errorf("use an Inline user ID or username; phone and email lookup are not supported")
		}
		limit := inlineDirectoryLimit
		page, err := ic.Sidecar.SearchUsers(ctx, sidecar.SearchUsersRequest{Query: username, Limit: &limit})
		if err != nil {
			return nil, fmt.Errorf("Inline username lookup failed: %w", err)
		}
		matches := make(map[int64]sidecar.UserRecord)
		for _, candidate := range page.Users {
			if candidate.UserID > 0 && candidate.Username != nil && canonicalInlineUsername(*candidate.Username) == username {
				matches[candidate.UserID] = candidate
			}
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("no accessible Inline user has username @%s", username)
		}
		if len(matches) > 1 {
			return nil, fmt.Errorf("Inline username @%s is ambiguous; use a numeric user ID", username)
		}
		for _, candidate := range matches {
			user = candidate
		}
	}
	ic.cacheUsers([]sidecar.UserRecord{user})
	response := &bridgev2.ResolveIdentifierResponse{UserID: makeUserID(strconv.FormatInt(user.UserID, 10)), UserInfo: userInfoFromRecord(user)}
	if createChat {
		chat, err := ic.createDMWithUserID(ctx, user.UserID)
		if err != nil {
			return nil, err
		}
		response.Chat = chat
	}
	return response, nil
}

func (ic *InlineClient) SearchUsers(ctx context.Context, query string) ([]*bridgev2.ResolveIdentifierResponse, error) {
	query = strings.TrimSpace(query)
	if query == "" || utf8.RuneCountInString(query) > 100 {
		return nil, fmt.Errorf("Inline people search requires 1–100 characters")
	}
	limit := inlineDirectoryLimit
	page, err := ic.Sidecar.SearchUsers(ctx, sidecar.SearchUsersRequest{Query: query, Limit: &limit})
	if err != nil {
		return nil, fmt.Errorf("Inline people search failed: %w", err)
	}
	// Only source-account results are exposed. The shared ghost cache is never a directory.
	unique := make(map[int64]sidecar.UserRecord)
	for _, user := range page.Users {
		if user.UserID > 0 {
			unique[user.UserID] = user
		}
	}
	ids := make([]int64, 0, len(unique))
	for userID := range unique {
		ids = append(ids, userID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) > int(inlineDirectoryLimit) {
		ids = ids[:inlineDirectoryLimit]
	}
	result := make([]*bridgev2.ResolveIdentifierResponse, 0, len(ids))
	for _, userID := range ids {
		user := unique[userID]
		ic.cacheUsers([]sidecar.UserRecord{user})
		result = append(result, &bridgev2.ResolveIdentifierResponse{UserID: networkid.UserID(strconv.FormatInt(userID, 10)), UserInfo: userInfoFromRecord(user)})
	}
	return result, nil
}
