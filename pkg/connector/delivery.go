package connector

import (
	"context"
	"errors"
	"fmt"
)

// verifyMessageDelivery requires a durable projection, rather than trusting
// queue admission or an ignored event for a room that has not been created.
func (ic *InlineClient) verifyMessageDelivery(ctx context.Context, chatID, messageID int64) error {
	key := ic.portalKeyForChat(chatID)
	parts, err := ic.UserLogin.Bridge.DB.Message.GetAllPartsByID(ctx, key.Receiver, makeMessageID(chatID, messageID))
	if err != nil {
		return fmt.Errorf("read delivered message mapping: %w", err)
	}
	if len(parts) == 0 {
		return errors.New("message delivery has no durable Matrix mapping")
	}
	for _, part := range parts {
		if part.Room != key || part.MXID == "" || part.HasFakeMXID() {
			return errors.New("message delivery has no real same-portal Matrix mapping")
		}
	}
	return nil
}
