package room

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/pavliha/aircast-sdk/pkg/message"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRedisDriver(t *testing.T) (*RedisDriver, *miniredis.Miniredis) {
	// Create in-memory Redis server
	mr := miniredis.RunT(t)

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Create driver
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	driver := NewRedisDriver(client, logger)

	return driver, mr
}

func TestRedisDriver_Subscribe(t *testing.T) {
	driver, _ := setupRedisDriver(t)
	defer driver.CloseAll()

	roomID := message.RoomID("test-room")

	ch, err := driver.Subscribe(roomID, "sub-1", 10)
	require.NoError(t, err)
	require.NotNil(t, ch)

	assert.Equal(t, 1, driver.SubscriberCount(roomID))
}

func TestRedisDriver_Subscribe_Duplicate(t *testing.T) {
	driver, _ := setupRedisDriver(t)
	defer driver.CloseAll()

	roomID := message.RoomID("test-room")

	_, err := driver.Subscribe(roomID, "sub-1", 10)
	require.NoError(t, err)

	// Attempt duplicate subscription
	_, err = driver.Subscribe(roomID, "sub-1", 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already subscribed")
}

func TestRedisDriver_Unsubscribe(t *testing.T) {
	driver, _ := setupRedisDriver(t)
	defer driver.CloseAll()

	roomID := message.RoomID("test-room")

	ch, err := driver.Subscribe(roomID, "sub-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 1, driver.SubscriberCount(roomID))

	driver.Unsubscribe(roomID, "sub-1")

	// Wait a bit for cleanup
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, 0, driver.SubscriberCount(roomID))

	// Channel should be closed
	_, open := <-ch
	assert.False(t, open)
}

func TestRedisDriver_Unsubscribe_NonExistent(t *testing.T) {
	driver, _ := setupRedisDriver(t)
	defer driver.CloseAll()

	// Should not panic
	driver.Unsubscribe("non-existent-room", "non-existent-sub")
}

func TestRedisDriver_Publish(t *testing.T) {
	driver, _ := setupRedisDriver(t)
	defer driver.CloseAll()

	roomID := message.RoomID("test-room")

	ch1, _ := driver.Subscribe(roomID, "sub-1", 10)
	ch2, _ := driver.Subscribe(roomID, "sub-2", 10)

	// Give the Redis listeners time to start
	time.Sleep(100 * time.Millisecond)

	msg := message.EventMessage{
		Action:  "test.event",
		Source:  message.SystemDevice,
		RoomID:  roomID,
		Payload: map[string]any{"data": "test"},
	}

	driver.Publish(roomID, msg)

	// Both subscribers should receive the message
	timeout := time.After(1 * time.Second)

	select {
	case received := <-ch1:
		assert.NotNil(t, received)
		// Message is received as GenericMessage, check the action
		if event, ok := received.(map[string]any); ok {
			assert.Equal(t, "test.event", event["action"])
		}
	case <-timeout:
		t.Fatal("Subscriber 1 did not receive message")
	}

	select {
	case received := <-ch2:
		assert.NotNil(t, received)
		if event, ok := received.(map[string]any); ok {
			assert.Equal(t, "test.event", event["action"])
		}
	case <-timeout:
		t.Fatal("Subscriber 2 did not receive message")
	}
}

func TestRedisDriver_Publish_NonExistentRoom(t *testing.T) {
	driver, _ := setupRedisDriver(t)
	defer driver.CloseAll()

	msg := message.EventMessage{
		Action: "test.event",
		Source: message.SystemDevice,
		RoomID: "non-existent",
	}

	// Should not panic
	driver.Publish("non-existent", msg)
}

func TestRedisDriver_SubscriberCount(t *testing.T) {
	driver, _ := setupRedisDriver(t)
	defer driver.CloseAll()

	roomID := message.RoomID("test-room")

	assert.Equal(t, 0, driver.SubscriberCount(roomID))

	_, err := driver.Subscribe(roomID, "sub-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 1, driver.SubscriberCount(roomID))

	_, err = driver.Subscribe(roomID, "sub-2", 10)
	require.NoError(t, err)
	assert.Equal(t, 2, driver.SubscriberCount(roomID))

	driver.Unsubscribe(roomID, "sub-1")

	// Wait for cleanup
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, driver.SubscriberCount(roomID))

	driver.Unsubscribe(roomID, "sub-2")

	// Wait for cleanup
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, driver.SubscriberCount(roomID))
}

func TestRedisDriver_CloseAll(t *testing.T) {
	driver, _ := setupRedisDriver(t)

	room1 := message.RoomID("room-1")
	room2 := message.RoomID("room-2")

	ch1, _ := driver.Subscribe(room1, "sub-1", 10)
	ch2, _ := driver.Subscribe(room2, "sub-2", 10)

	assert.Equal(t, 1, driver.SubscriberCount(room1))
	assert.Equal(t, 1, driver.SubscriberCount(room2))

	driver.CloseAll()

	// All channels should be closed
	_, open1 := <-ch1
	assert.False(t, open1)

	_, open2 := <-ch2
	assert.False(t, open2)

	// All subscriber counts should be zero
	assert.Equal(t, 0, driver.SubscriberCount(room1))
	assert.Equal(t, 0, driver.SubscriberCount(room2))
}

func TestRedisDriver_MultipleRooms(t *testing.T) {
	driver, _ := setupRedisDriver(t)
	defer driver.CloseAll()

	room1 := message.RoomID("room-1")
	room2 := message.RoomID("room-2")

	ch1, _ := driver.Subscribe(room1, "sub-1", 10)
	ch2, _ := driver.Subscribe(room2, "sub-2", 10)

	// Give listeners time to start
	time.Sleep(100 * time.Millisecond)

	msg1 := message.EventMessage{
		Action: "room1.event",
		Source: message.SystemDevice,
		RoomID: room1,
	}

	driver.Publish(room1, msg1)

	// Only room1 subscriber should receive
	select {
	case received := <-ch1:
		assert.NotNil(t, received)
		if event, ok := received.(map[string]any); ok {
			assert.Equal(t, "room1.event", event["action"])
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Room 1 subscriber did not receive message")
	}

	// Room 2 should NOT receive
	select {
	case <-ch2:
		t.Fatal("Room 2 subscriber should not receive room 1 message")
	case <-time.After(200 * time.Millisecond):
		// Expected - no message
	}
}

func TestRedisDriver_CrossInstanceCommunication(t *testing.T) {
	// This tests the distributed nature of Redis driver
	mr := miniredis.RunT(t)

	// Create two separate drivers (simulating two app instances)
	client1 := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	client2 := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	driver1 := NewRedisDriver(client1, logger)
	driver2 := NewRedisDriver(client2, logger)

	defer driver1.CloseAll()
	defer driver2.CloseAll()

	roomID := message.RoomID("shared-room")

	// Subscribe from driver1
	ch1, _ := driver1.Subscribe(roomID, "sub-1", 10)

	// Subscribe from driver2
	ch2, _ := driver2.Subscribe(roomID, "sub-2", 10)

	// Give listeners time to start
	time.Sleep(100 * time.Millisecond)

	// Publish from driver1
	msg := message.EventMessage{
		Action:  "cross.instance",
		Source:  message.SystemDevice,
		RoomID:  roomID,
		Payload: map[string]any{"test": "data"},
	}

	driver1.Publish(roomID, msg)

	// Both subscribers should receive (even though they're on different drivers)
	timeout := time.After(1 * time.Second)

	select {
	case received := <-ch1:
		assert.NotNil(t, received)
	case <-timeout:
		t.Fatal("Driver 1 subscriber did not receive message")
	}

	select {
	case received := <-ch2:
		assert.NotNil(t, received)
	case <-timeout:
		t.Fatal("Driver 2 subscriber did not receive message")
	}
}

func TestRedisDriver_AutoCleanupOnLastUnsubscribe(t *testing.T) {
	driver, _ := setupRedisDriver(t)
	defer driver.CloseAll()

	roomID := message.RoomID("test-room")

	// Subscribe two subscribers
	_, err := driver.Subscribe(roomID, "sub-1", 10)
	require.NoError(t, err)
	_, err = driver.Subscribe(roomID, "sub-2", 10)
	require.NoError(t, err)

	assert.Equal(t, 2, driver.SubscriberCount(roomID))

	// Unsubscribe first - room should still exist
	driver.Unsubscribe(roomID, "sub-1")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, driver.SubscriberCount(roomID))

	// Unsubscribe last - room should be cleaned up
	driver.Unsubscribe(roomID, "sub-2")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, driver.SubscriberCount(roomID))

	// Verify room is completely removed from driver's internal state
	driver.roomsMutex.RLock()
	_, exists := driver.rooms[roomID]
	driver.roomsMutex.RUnlock()
	assert.False(t, exists, "Room should be removed from internal state")
}
