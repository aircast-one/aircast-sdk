package room

import (
	"context"
	"fmt"
	"sync"

	"github.com/pavliha/aircast-sdk/pkg/message"
	"github.com/sirupsen/logrus"
)

// RoomManager manages subscriptions and routes messages to room subscribers
type RoomManager struct {
	rooms      map[message.RoomID]*roomSubscribers
	roomsMutex sync.RWMutex
	logger     *logrus.Entry
}

type roomSubscribers struct {
	subscribers map[string]chan message.GenericMessage
	subMutex    sync.RWMutex
}

// NewRoomManager creates a new RoomManager
func NewRoomManager(logger *logrus.Entry) *RoomManager {
	return &RoomManager{
		rooms:  make(map[message.RoomID]*roomSubscribers),
		logger: logger.WithField("component", "room_manager"),
	}
}

// Subscribe adds a subscriber to a room and returns a channel for receiving messages
func (rm *RoomManager) Subscribe(roomID message.RoomID, subscriberID string, bufferSize int) (<-chan message.GenericMessage, error) {
	rm.roomsMutex.Lock()
	defer rm.roomsMutex.Unlock()

	// Get or create room
	room, exists := rm.rooms[roomID]
	if !exists {
		room = &roomSubscribers{
			subscribers: make(map[string]chan message.GenericMessage),
		}
		rm.rooms[roomID] = room
	}

	room.subMutex.Lock()
	defer room.subMutex.Unlock()

	// Check if already subscribed
	if _, exists := room.subscribers[subscriberID]; exists {
		return nil, fmt.Errorf("subscriber %s already subscribed to room %s", subscriberID, roomID)
	}

	// Create channel
	ch := make(chan message.GenericMessage, bufferSize)
	room.subscribers[subscriberID] = ch

	rm.logger.WithFields(logrus.Fields{
		"room":        roomID,
		"subscriber":  subscriberID,
		"buffer_size": bufferSize,
		"total_subs":  len(room.subscribers),
	}).Debug("Subscriber added to room")

	return ch, nil
}

// Unsubscribe removes a subscriber from a room
func (rm *RoomManager) Unsubscribe(roomID message.RoomID, subscriberID string) {
	rm.roomsMutex.RLock()
	room, exists := rm.rooms[roomID]
	rm.roomsMutex.RUnlock()

	if !exists {
		return
	}

	room.subMutex.Lock()
	defer room.subMutex.Unlock()

	ch, exists := room.subscribers[subscriberID]
	if exists {
		close(ch)
		delete(room.subscribers, subscriberID)

		rm.logger.WithFields(logrus.Fields{
			"room":       roomID,
			"subscriber": subscriberID,
			"total_subs": len(room.subscribers),
		}).Debug("Subscriber removed from room")
	}
}

// SubscriberCount returns the number of subscribers for a room
func (rm *RoomManager) SubscriberCount(roomID message.RoomID) int {
	rm.roomsMutex.RLock()
	room, exists := rm.rooms[roomID]
	rm.roomsMutex.RUnlock()

	if !exists {
		return 0
	}

	room.subMutex.RLock()
	defer room.subMutex.RUnlock()
	return len(room.subscribers)
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
				rm.publish(roomID, msg)
			}

		case <-ctx.Done():
			rm.logger.Debug("Context cancelled, stopping routing")
			return
		}
	}
}

// extractRoomID extracts the room ID from a message
func (rm *RoomManager) extractRoomID(msg any) message.RoomID {
	switch m := msg.(type) {
	case message.RequestMessage:
		return m.RoomID
	case message.ResponseMessage:
		return m.RoomID
	case message.ErrorMessage:
		return m.RoomID
	case message.EventMessage:
		return m.RoomID
	}
	return ""
}

// publish sends a message to all subscribers of a room
func (rm *RoomManager) publish(roomID message.RoomID, msg message.GenericMessage) {
	rm.roomsMutex.RLock()
	room, exists := rm.rooms[roomID]
	rm.roomsMutex.RUnlock()

	if !exists {
		return
	}

	room.subMutex.RLock()
	defer room.subMutex.RUnlock()

	for subID, ch := range room.subscribers {
		select {
		case ch <- msg:
			// Message sent successfully
		default:
			rm.logger.WithFields(logrus.Fields{
				"room":       roomID,
				"subscriber": subID,
			}).Warn("Subscriber channel full, dropping message")
		}
	}
}

// CloseAll closes all subscriber channels and clears all rooms
func (rm *RoomManager) CloseAll() {
	rm.roomsMutex.Lock()
	defer rm.roomsMutex.Unlock()

	for roomID, room := range rm.rooms {
		room.subMutex.Lock()
		for subID, ch := range room.subscribers {
			close(ch)
			rm.logger.WithFields(logrus.Fields{
				"room":       roomID,
				"subscriber": subID,
			}).Debug("Closed subscriber channel")
		}
		room.subscribers = make(map[string]chan message.GenericMessage)
		room.subMutex.Unlock()
	}

	rm.rooms = make(map[message.RoomID]*roomSubscribers)
}
