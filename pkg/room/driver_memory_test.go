package room

import (
	"testing"
	"time"

	"github.com/pavliha/aircast-sdk/pkg/message"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryDriver_Subscribe(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	driver := NewMemoryDriver(logger)
	roomID := message.RoomID("test-room")

	ch, err := driver.Subscribe(roomID, "sub-1", 10)
	require.NoError(t, err)
	require.NotNil(t, ch)

	assert.Equal(t, 1, driver.SubscriberCount(roomID))
}

func TestMemoryDriver_Subscribe_Duplicate(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	driver := NewMemoryDriver(logger)
	roomID := message.RoomID("test-room")

	_, err := driver.Subscribe(roomID, "sub-1", 10)
	require.NoError(t, err)

	// Attempt duplicate subscription
	_, err = driver.Subscribe(roomID, "sub-1", 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already subscribed")
}

func TestMemoryDriver_Unsubscribe(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	driver := NewMemoryDriver(logger)
	roomID := message.RoomID("test-room")

	ch, err := driver.Subscribe(roomID, "sub-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 1, driver.SubscriberCount(roomID))

	driver.Unsubscribe(roomID, "sub-1")
	assert.Equal(t, 0, driver.SubscriberCount(roomID))

	// Channel should be closed
	_, open := <-ch
	assert.False(t, open)
}

func TestMemoryDriver_Unsubscribe_NonExistent(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	driver := NewMemoryDriver(logger)

	// Should not panic
	driver.Unsubscribe("non-existent-room", "non-existent-sub")
}

func TestMemoryDriver_Publish(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	driver := NewMemoryDriver(logger)
	roomID := message.RoomID("test-room")

	ch1, _ := driver.Subscribe(roomID, "sub-1", 10)
	ch2, _ := driver.Subscribe(roomID, "sub-2", 10)

	msg := message.EventMessage{
		Action:  "test.event",
		Source:  message.SystemDevice,
		RoomID:  roomID,
		Payload: map[string]any{"data": "test"},
	}

	driver.Publish(roomID, msg)

	// Both subscribers should receive the message
	timeout := time.After(100 * time.Millisecond)

	select {
	case received := <-ch1:
		event := received.(message.EventMessage)
		assert.Equal(t, "test.event", event.Action)
	case <-timeout:
		t.Fatal("Subscriber 1 did not receive message")
	}

	select {
	case received := <-ch2:
		event := received.(message.EventMessage)
		assert.Equal(t, "test.event", event.Action)
	case <-timeout:
		t.Fatal("Subscriber 2 did not receive message")
	}
}

func TestMemoryDriver_Publish_NonExistentRoom(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	driver := NewMemoryDriver(logger)

	msg := message.EventMessage{
		Action: "test.event",
		Source: message.SystemDevice,
		RoomID: "non-existent",
	}

	// Should not panic
	driver.Publish("non-existent", msg)
}

func TestMemoryDriver_Publish_FullChannel(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.WarnLevel) // Enable warnings to test the log

	driver := NewMemoryDriver(logger)
	roomID := message.RoomID("test-room")

	// Subscribe with buffer size 1
	ch, _ := driver.Subscribe(roomID, "sub-1", 1)

	msg := message.EventMessage{
		Action: "test.event",
		Source: message.SystemDevice,
		RoomID: roomID,
	}

	// Fill the buffer
	driver.Publish(roomID, msg)

	// This should trigger the "channel full" warning
	driver.Publish(roomID, msg)

	// Drain the channel
	<-ch
}

func TestMemoryDriver_SubscriberCount(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	driver := NewMemoryDriver(logger)
	roomID := message.RoomID("test-room")

	assert.Equal(t, 0, driver.SubscriberCount(roomID))

	_, err := driver.Subscribe(roomID, "sub-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 1, driver.SubscriberCount(roomID))

	_, err = driver.Subscribe(roomID, "sub-2", 10)
	require.NoError(t, err)
	assert.Equal(t, 2, driver.SubscriberCount(roomID))

	driver.Unsubscribe(roomID, "sub-1")
	assert.Equal(t, 1, driver.SubscriberCount(roomID))

	driver.Unsubscribe(roomID, "sub-2")
	assert.Equal(t, 0, driver.SubscriberCount(roomID))
}

func TestMemoryDriver_CloseAll(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	driver := NewMemoryDriver(logger)
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

func TestMemoryDriver_MultipleRooms(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	driver := NewMemoryDriver(logger)
	room1 := message.RoomID("room-1")
	room2 := message.RoomID("room-2")

	ch1, _ := driver.Subscribe(room1, "sub-1", 10)
	ch2, _ := driver.Subscribe(room2, "sub-2", 10)

	msg1 := message.EventMessage{
		Action: "room1.event",
		Source: message.SystemDevice,
		RoomID: room1,
	}

	driver.Publish(room1, msg1)

	// Only room1 subscriber should receive
	select {
	case received := <-ch1:
		event := received.(message.EventMessage)
		assert.Equal(t, "room1.event", event.Action)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Room 1 subscriber did not receive message")
	}

	// Room 2 should NOT receive
	select {
	case <-ch2:
		t.Fatal("Room 2 subscriber should not receive room 1 message")
	case <-time.After(50 * time.Millisecond):
		// Expected - no message
	}
}
