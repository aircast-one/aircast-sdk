package room

import (
	"context"

	"log/slog"

	"github.com/pavliha/aircast-sdk/pkg/message"
)

// RoomManager manages subscriptions and routes messages to room subscribers
type RoomManager struct {
	driver Driver
	logger *slog.Logger
}

// NewRoomManager creates a new RoomManager with the specified driver
func NewRoomManager(driver Driver, logger *slog.Logger) *RoomManager {
	return &RoomManager{
		driver: driver,
		logger: logger.With("component", "room_manager"),
	}
}

// NewRoomManagerWithMemory creates a new RoomManager with an in-memory driver
func NewRoomManagerWithMemory(logger *slog.Logger) *RoomManager {
	driver := NewMemoryDriver(logger)
	return NewRoomManager(driver, logger)
}

// Subscribe adds a subscriber to a room and returns a channel for receiving messages
func (rm *RoomManager) Subscribe(roomID message.RoomID, subscriberID string, bufferSize int) (<-chan message.GenericMessage, error) {
	return rm.driver.Subscribe(roomID, subscriberID, bufferSize)
}

// Unsubscribe removes a subscriber from a room
func (rm *RoomManager) Unsubscribe(roomID message.RoomID, subscriberID string) {
	rm.driver.Unsubscribe(roomID, subscriberID)
}

// SubscriberCount returns the number of subscribers for a room
func (rm *RoomManager) SubscriberCount(roomID message.RoomID) int {
	return rm.driver.SubscriberCount(roomID)
}

// ListenAndRoute listens to client messages and routes them to room subscribers
func (rm *RoomManager) ListenAndRoute(ctx context.Context, client message.Client) {
	msgCh := client.ReadMessage()

	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				rm.logger.Debug("Message channel closed, stopping routing")
				return
			}

			// Extract room ID and publish to subscribers
			roomID := rm.extractRoomID(msg)
			if roomID != "" {
				rm.driver.Publish(roomID, msg)
			}

		case <-ctx.Done():
			rm.logger.Debug("Context cancelled, stopping routing")
			return
		}
	}
}

// extractRoomID extracts the room ID from a message using the RoomMessage interface
func (rm *RoomManager) extractRoomID(msg any) message.RoomID {
	if roomMsg, ok := msg.(message.RoomMessage); ok {
		return roomMsg.GetRoomID()
	}
	return ""
}

// CloseAll closes all subscriber channels and clears all rooms
func (rm *RoomManager) CloseAll() {
	rm.driver.CloseAll()
}
