package connector

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/inline-chat/matrix-inline/pkg/sidecar"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func inlineGroupCreationCapabilities() map[string]bridgev2.GroupTypeCapabilities {
	capability := bridgev2.GroupTypeCapabilities{
		TypeDescription: "Private Inline chat",
		Name:            bridgev2.GroupFieldCapability{Allowed: true, Required: true, MinLength: 1, MaxLength: 256},
		Topic:           bridgev2.GroupFieldCapability{Allowed: true, MaxLength: 4096},
		Participants:    bridgev2.GroupFieldCapability{Allowed: true},
		Parent:          bridgev2.GroupFieldCapability{Allowed: true},
	}
	public := capability
	public.TypeDescription = "Public Inline workspace channel"
	public.Parent.Required = true
	private := capability
	private.TypeDescription = "Private Inline workspace channel"
	private.Parent.Required = true
	return map[string]bridgev2.GroupTypeCapabilities{"thread": capability, "private_channel": private, "public_channel": public}
}

func (ic *InlineClient) createInlineGroup(ctx context.Context, params *bridgev2.GroupCreateParams) (*bridgev2.CreateChatResponse, error) {
	request, participantIDs, err := ic.inlineGroupRequest(params)
	if err != nil {
		return nil, err
	}
	chat, err := ic.Sidecar.CreateThread(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("Inline sidecar create chat failed: %w", err)
	}
	if chat == nil || chat.ChatID <= 0 {
		return nil, fmt.Errorf("Inline returned no created chat")
	}
	ic.rememberDialog(sidecar.DialogRecord{ChatID: chat.ChatID, Kind: sidecar.ConversationChat, StructureKnown: true, Title: chat.Title, Description: request.Description, SpaceID: request.SpaceID, IsPublic: &request.IsPublic})
	return ic.createChatResponse(ctx, chat, ic.chatInfoForCreatedChat(ctx, chat, participantIDs)), nil
}

func (ic *InlineClient) inlineGroupRequest(params *bridgev2.GroupCreateParams) (sidecar.CreateThreadRequest, []networkid.UserID, error) {
	request := sidecar.CreateThreadRequest{}
	if params == nil {
		return request, nil, fmt.Errorf("missing Inline group create params")
	}
	kind := strings.TrimSpace(params.Type)
	switch kind {
	case "", "thread", "private_channel":
	case "public_channel":
		request.IsPublic = true
	default:
		return request, nil, fmt.Errorf("unsupported Inline creation type %q; native child conversations and group DMs cannot be created from Matrix", kind)
	}
	if params.Username != "" || params.Avatar != nil || params.Disappear != nil {
		return request, nil, fmt.Errorf("Inline chat creation does not support usernames, avatars or disappearing timers")
	}
	if params.Name == nil || strings.TrimSpace(params.Name.Name) == "" || utf8.RuneCountInString(params.Name.Name) > 256 {
		return request, nil, fmt.Errorf("Inline chat name must contain 1–256 characters")
	}
	request.Title = optionalString(strings.TrimSpace(params.Name.Name))
	if params.Topic != nil {
		if utf8.RuneCountInString(params.Topic.Topic) > 4096 {
			return request, nil, fmt.Errorf("Inline chat description exceeds 4096 characters")
		}
		request.Description = &params.Topic.Topic
	}
	if params.Parent != nil {
		spaceID, err := ic.workspaceIDFromKey(*params.Parent)
		if err != nil {
			return request, nil, err
		}
		request.SpaceID = &spaceID
	}
	if (kind == "public_channel" || kind == "private_channel") && request.SpaceID == nil {
		return request, nil, fmt.Errorf("Inline workspace channels require an Inline workspace parent")
	}
	ids := make([]networkid.UserID, 0, len(params.Participants))
	seen := make(map[int64]bool)
	for _, participant := range params.Participants {
		userID, err := parseInlineUserID(string(participant))
		if err != nil {
			return request, nil, fmt.Errorf("invalid Inline participant %q: %w", participant, err)
		}
		if seen[userID] {
			continue
		}
		seen[userID] = true
		request.Participants = append(request.Participants, sidecar.ChatCreateParticipant{UserID: userID})
		ids = append(ids, makeUserID(strconv.FormatInt(userID, 10)))
	}
	// Workspace/participant authorization remains with the source mutation; cached
	// roles or a local directory are never used to grant or deny source permission.
	return request, ids, nil
}
