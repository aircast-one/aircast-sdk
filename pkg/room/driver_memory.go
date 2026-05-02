package room

import (
	"fmt"
	"sync"

	"log/slog"

	"github.com/pavliha/aircast-sdk/pkg/message"
)

// MemoryDriver is an in-memory implementation of the Driver interface
type MemoryDriver struct {
	rooms      map[message.RoomID]*roomSubscribers
	roomsMutex sync.RWMutex
	logger     *slog.Logger
}

type roomSubscribers struct {
	subscribers map[string]chan message.GenericMessage
	subMutex    sync.RWMutex
}

// NewMemoryDriver creates a new in-memory driver
func NewMemoryDriver(logger *slog.Logger) *MemoryDriver {
	return &MemoryDriver{
		rooms:  make(map[message.RoomID]*roomSubscribers),
		logger: logger.With("driver", "memory"),
	}
}

// Subscribe adds a subscriber to a room and returns a channel for receiving messages
func (d *MemoryDriver) Subscribe(roomID message.RoomID, subscriberID string, bufferSize int) (<-chan message.GenericMessage, error) {
	d.roomsMutex.Lock()
	defer d.roomsMutex.Unlock()

	// Get or create room
	room, exists := d.rooms[roomID]
	if !exists {
		room = &roomSubscribers{
			subscribers: make(map[string]chan message.GenericMessage),
		}
		d.rooms[roomID] = room
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

	d.logger.Debug("Subscriber added to room",
		"room", roomID,
		"subscriber", subscriberID,
		"buffer_size", bufferSize,
		"total_subs", len(room.subscribers),
	)

	return ch, nil
}

// Unsubscribe removes a subscriber from a room
func (d *MemoryDriver) Unsubscribe(roomID message.RoomID, subscriberID string) {
	d.roomsMutex.RLock()
	room, exists := d.rooms[roomID]
	d.roomsMutex.RUnlock()

	if !exists {
		return
	}

	room.subMutex.Lock()
	defer room.subMutex.Unlock()

	ch, exists := room.subscribers[subscriberID]
	if exists {
		close(ch)
		delete(room.subscribers, subscriberID)

		d.logger.Debug("Subscriber removed from room",
			"room", roomID,
			"subscriber", subscriberID,
			"total_subs", len(room.subscribers),
		)
	}
}

// Publish sends a message to all subscribers of a room
func (d *MemoryDriver) Publish(roomID message.RoomID, msg message.GenericMessage) {
	d.roomsMutex.RLock()
	room, exists := d.rooms[roomID]
	d.roomsMutex.RUnlock()

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
			d.logger.Warn("Subscriber channel full, dropping message",
				"room", roomID,
				"subscriber", subID,
			)
		}
	}
}

// SubscriberCount returns the number of subscribers for a room
func (d *MemoryDriver) SubscriberCount(roomID message.RoomID) int {
	d.roomsMutex.RLock()
	room, exists := d.rooms[roomID]
	d.roomsMutex.RUnlock()

	if !exists {
		return 0
	}

	room.subMutex.RLock()
	defer room.subMutex.RUnlock()
	return len(room.subscribers)
}

// CloseAll closes all subscriber channels and clears all rooms
func (d *MemoryDriver) CloseAll() {
	d.roomsMutex.Lock()
	defer d.roomsMutex.Unlock()

	for roomID, room := range d.rooms {
		room.subMutex.Lock()
		for subID, ch := range room.subscribers {
			close(ch)
			d.logger.Debug("Closed subscriber channel",
				"room", roomID,
				"subscriber", subID,
			)
		}
		room.subscribers = make(map[string]chan message.GenericMessage)
		room.subMutex.Unlock()
	}

	d.rooms = make(map[message.RoomID]*roomSubscribers)
}
