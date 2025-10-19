package room

import (
	"context"
	"testing"
	"time"

	"github.com/pavliha/aircast-sdk/pkg/message"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoomManager_Subscribe_SingleSubscriber(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel) // Suppress logs in tests

	// Create room manager
	rm := NewRoomManager(logger)

	roomID := message.RoomID("test-room-1")

	// Subscribe to the room
	msgCh, err := rm.Subscribe(roomID, "subscriber-1", 10)
	require.NoError(t, err)
	require.NotNil(t, msgCh)

	assert.Equal(t, 1, rm.SubscriberCount(roomID))

	// Unsubscribe
	rm.Unsubscribe(roomID, "subscriber-1")
	assert.Equal(t, 0, rm.SubscriberCount(roomID))
}

func TestRoomManager_Subscribe_MultipleSubscribers(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	rm := NewRoomManager(logger)
	roomID := message.RoomID("test-room-2")

	// Multiple subscribers
	msgCh1, err := rm.Subscribe(roomID, "subscriber-1", 10)
	require.NoError(t, err)

	msgCh2, err := rm.Subscribe(roomID, "subscriber-2", 10)
	require.NoError(t, err)

	msgCh3, err := rm.Subscribe(roomID, "subscriber-3", 10)
	require.NoError(t, err)

	assert.Equal(t, 3, rm.SubscriberCount(roomID))

	// Publish a message
	eventMsg := message.EventMessage{
		Action:  "broadcast.event",
		Source:  message.SystemDevice,
		RoomID:  roomID,
		Payload: map[string]any{"broadcast": "data"},
	}

	rm.publish(roomID, eventMsg)

	// All three subscribers should receive the message
	timeout := time.After(100 * time.Millisecond)

	select {
	case msg := <-msgCh1:
		event := msg.(message.EventMessage)
		assert.Equal(t, "broadcast.event", event.Action)
	case <-timeout:
		t.Fatal("Subscriber 1 did not receive message")
	}

	select {
	case msg := <-msgCh2:
		event := msg.(message.EventMessage)
		assert.Equal(t, "broadcast.event", event.Action)
	case <-timeout:
		t.Fatal("Subscriber 2 did not receive message")
	}

	select {
	case msg := <-msgCh3:
		event := msg.(message.EventMessage)
		assert.Equal(t, "broadcast.event", event.Action)
	case <-timeout:
		t.Fatal("Subscriber 3 did not receive message")
	}

	// Unsubscribe one
	rm.Unsubscribe(roomID, "subscriber-2")
	assert.Equal(t, 2, rm.SubscriberCount(roomID))
}

func TestRoomManager_Subscribe_DuplicateSubscriber(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	rm := NewRoomManager(logger)
	roomID := message.RoomID("test-room-3")

	// First subscription succeeds
	_, err := rm.Subscribe(roomID, "subscriber-1", 10)
	require.NoError(t, err)

	// Duplicate subscription fails
	_, err = rm.Subscribe(roomID, "subscriber-1", 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already subscribed")
}

func TestRoomManager_CloseAll(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	rm := NewRoomManager(logger)
	roomID := message.RoomID("test-room-4")

	// Subscribe
	msgCh, err := rm.Subscribe(roomID, "subscriber-1", 10)
	require.NoError(t, err)

	assert.Equal(t, 1, rm.SubscriberCount(roomID))

	// Close all
	rm.CloseAll()

	// Subscriber channel should be closed
	_, open := <-msgCh
	assert.False(t, open, "Subscriber channel should be closed")
	assert.Equal(t, 0, rm.SubscriberCount(roomID))
}

func TestRoomManager_DifferentRooms(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	rm := NewRoomManager(logger)

	room1ID := message.RoomID("room-1")
	room2ID := message.RoomID("room-2")

	// Subscribe to both rooms
	msgCh1, _ := rm.Subscribe(room1ID, "sub-1", 10)
	msgCh2, _ := rm.Subscribe(room2ID, "sub-2", 10)

	// Publish to room-1
	eventMsg := message.EventMessage{
		Action:  "event.room1",
		Source:  message.SystemDevice,
		RoomID:  room1ID,
		Payload: map[string]any{},
	}
	rm.publish(room1ID, eventMsg)

	// Only room-1 subscriber should receive it
	select {
	case msg := <-msgCh1:
		event := msg.(message.EventMessage)
		assert.Equal(t, "event.room1", event.Action)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Room 1 subscriber did not receive message")
	}

	// Room 2 should NOT receive it
	select {
	case <-msgCh2:
		t.Fatal("Room 2 subscriber should not receive room-1 message")
	case <-time.After(50 * time.Millisecond):
		// Expected - no message
	}
}

func TestRoomManager_ListenAndRoute(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel)

	// Create a mock client that implements message.Client interface
	mockClient := &mockClientForRouting{
		msgCh: make(chan any, 10),
	}

	// Create room manager
	rm := NewRoomManager(logger)

	roomID := message.RoomID("test-room-routing")

	// Subscribe to the room
	msgCh, err := rm.Subscribe(roomID, "subscriber-1", 10)
	require.NoError(t, err)

	// Start room manager routing
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rm.ListenAndRoute(ctx, mockClient)

	// Simulate receiving a message for this room
	eventMsg := message.EventMessage{
		Action:  "test.event",
		Source:  message.SystemDevice,
		RoomID:  roomID,
		Payload: map[string]any{"data": "test"},
	}
	mockClient.msgCh <- eventMsg

	// Verify subscriber receives the message
	select {
	case receivedMsg := <-msgCh:
		event, ok := receivedMsg.(message.EventMessage)
		require.True(t, ok, "Expected EventMessage")
		assert.Equal(t, "test.event", event.Action)
		assert.Equal(t, roomID, event.RoomID)
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

// mockClientForRouting is a minimal mock client for testing routing
type mockClientForRouting struct {
	msgCh chan any
}

func (m *mockClientForRouting) ReadMessage() <-chan any {
	return m.msgCh
}

// Stub other Client interface methods
func (m *mockClientForRouting) Listen(ctx context.Context) error            { return nil }
func (m *mockClientForRouting) Send(msg any) error                          { return nil }
func (m *mockClientForRouting) Close() error                                { return nil }
func (m *mockClientForRouting) IsClosed() bool                              { return false }
func (m *mockClientForRouting) GetSource() message.MessageSource            { return message.SystemDevice }
func (m *mockClientForRouting) RegisterWill(will message.WillMessage) error { return nil }
func (m *mockClientForRouting) ClearWill() error                            { return nil }
func (m *mockClientForRouting) SendRawJSON(jsonBytes []byte) error          { return nil }
