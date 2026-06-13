package room

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/pavliha/aircast-sdk/pkg/message"
	"github.com/redis/go-redis/v9"
)

// RedisDriver is a Redis-backed implementation of the Driver interface
type RedisDriver struct {
	client     *redis.Client
	rooms      map[message.RoomID]*redisRoom
	roomsMutex sync.RWMutex
	logger     *slog.Logger
	ctx        context.Context
	cancel     context.CancelFunc
}

type redisRoom struct {
	subscribers map[string]chan message.GenericMessage
	subMutex    sync.RWMutex
	pubsub      *redis.PubSub
	cancel      context.CancelFunc
}

// NewRedisDriver creates a new Redis-backed driver
func NewRedisDriver(client *redis.Client, logger *slog.Logger) *RedisDriver {
	ctx, cancel := context.WithCancel(context.Background())
	return &RedisDriver{
		client: client,
		rooms:  make(map[message.RoomID]*redisRoom),
		logger: logger.With("driver", "redis"),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Subscribe adds a subscriber to a room and returns a channel for receiving messages
func (d *RedisDriver) Subscribe(roomID message.RoomID, subscriberID string, bufferSize int) (<-chan message.GenericMessage, error) {
	d.roomsMutex.Lock()
	defer d.roomsMutex.Unlock()

	// Get or create room
	room, exists := d.rooms[roomID]
	if !exists {
		room = &redisRoom{
			subscribers: make(map[string]chan message.GenericMessage),
		}
		d.rooms[roomID] = room

		// Start Redis pub/sub listener for this room
		if err := d.startRoomListener(roomID, room); err != nil {
			return nil, fmt.Errorf("failed to start room listener: %w", err)
		}
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
func (d *RedisDriver) Unsubscribe(roomID message.RoomID, subscriberID string) {
	d.roomsMutex.Lock()
	defer d.roomsMutex.Unlock()

	room, exists := d.rooms[roomID]
	if !exists {
		return
	}

	room.subMutex.Lock()
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

	// If no more subscribers, clean up the room
	subscriberCount := len(room.subscribers)
	room.subMutex.Unlock()

	if subscriberCount == 0 {
		d.cleanupRoom(roomID, room)
	}
}

// Publish sends a message to all subscribers of a room via Redis pub/sub
func (d *RedisDriver) Publish(roomID message.RoomID, msg message.GenericMessage) {
	// Serialize message to JSON
	data, err := json.Marshal(msg)
	if err != nil {
		d.logger.Error("Failed to marshal message", "error", err)
		return
	}

	// Publish to Redis
	channel := d.roomChannel(roomID)
	if err := d.client.Publish(d.ctx, channel, data).Err(); err != nil {
		d.logger.Error("Failed to publish message to Redis", "error", err, "room", roomID)
	}
}

// SubscriberCount returns the number of local subscribers for a room
func (d *RedisDriver) SubscriberCount(roomID message.RoomID) int {
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
func (d *RedisDriver) CloseAll() {
	d.roomsMutex.Lock()
	defer d.roomsMutex.Unlock()

	for roomID, room := range d.rooms {
		d.cleanupRoomNoLock(roomID, room)
	}

	d.rooms = make(map[message.RoomID]*redisRoom)
	d.cancel()
}

// startRoomListener starts a Redis pub/sub listener for a room
func (d *RedisDriver) startRoomListener(roomID message.RoomID, room *redisRoom) error {
	channel := d.roomChannel(roomID)
	pubsub := d.client.Subscribe(d.ctx, channel)

	// Wait for confirmation that subscription is created
	if _, err := pubsub.Receive(d.ctx); err != nil {
		_ = pubsub.Close() // Ignore close error during error path
		return fmt.Errorf("failed to subscribe to Redis channel: %w", err)
	}

	room.pubsub = pubsub

	// Create a cancelable context for this room's listener
	ctx, cancel := context.WithCancel(d.ctx)
	room.cancel = cancel

	// Start listening in a goroutine
	go d.listenToRoom(ctx, roomID, room)

	d.logger.Debug("Started Redis listener for room", "room", roomID)
	return nil
}

// listenToRoom listens to Redis pub/sub and forwards messages to local subscribers
func (d *RedisDriver) listenToRoom(ctx context.Context, roomID message.RoomID, room *redisRoom) {
	ch := room.pubsub.Channel()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				d.logger.Debug("Redis channel closed", "room", roomID)
				return
			}

			// Deserialize message
			var genericMsg message.GenericMessage
			if err := json.Unmarshal([]byte(msg.Payload), &genericMsg); err != nil {
				d.logger.Error("Failed to unmarshal message from Redis", "error", err)
				continue
			}

			// Forward to local subscribers
			d.forwardToSubscribers(roomID, room, genericMsg)

		case <-ctx.Done():
			d.logger.Debug("Room listener context cancelled", "room", roomID)
			return
		}
	}
}

// forwardToSubscribers forwards a message to all local subscribers of a room
func (d *RedisDriver) forwardToSubscribers(roomID message.RoomID, room *redisRoom, msg message.GenericMessage) {
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

// cleanupRoom cleans up a room when it has no more subscribers
func (d *RedisDriver) cleanupRoom(roomID message.RoomID, room *redisRoom) {
	if room.cancel != nil {
		room.cancel()
	}
	if room.pubsub != nil {
		_ = room.pubsub.Close() // Ignore close error during cleanup
	}
	delete(d.rooms, roomID)

	d.logger.Debug("Cleaned up room", "room", roomID)
}

// cleanupRoomNoLock cleans up a room without acquiring the rooms mutex (caller must hold lock)
func (d *RedisDriver) cleanupRoomNoLock(roomID message.RoomID, room *redisRoom) {
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

	if room.cancel != nil {
		room.cancel()
	}
	if room.pubsub != nil {
		_ = room.pubsub.Close() // Ignore close error during cleanup
	}
}

// roomChannel returns the Redis channel name for a room
func (d *RedisDriver) roomChannel(roomID message.RoomID) string {
	return fmt.Sprintf("room:%s", roomID)
}
