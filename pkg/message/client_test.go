package message

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockConnection is a mock implementation of the Connection interface
type MockConnection struct {
	mock.Mock
	msgCh      chan []byte
	closed     bool
	closeMutex sync.Mutex
}

func NewMockConnection() *MockConnection {
	return &MockConnection{
		msgCh: make(chan []byte, 15000), // Increased to support high-volume tests
	}
}

func (m *MockConnection) SendMessage(message []byte) error {
	args := m.Called(message)
	return args.Error(0)
}

func (m *MockConnection) ReadMessage() <-chan []byte {
	m.Called()
	return m.msgCh
}

func (m *MockConnection) Close() error {
	m.closeMutex.Lock()
	defer m.closeMutex.Unlock()

	if !m.closed {
		m.closed = true
		close(m.msgCh)
	}
	args := m.Called()
	return args.Error(0)
}

func (m *MockConnection) IsClosed() bool {
	m.closeMutex.Lock()
	defer m.closeMutex.Unlock()

	args := m.Called()
	return args.Bool(0)
}

func TestNewClient(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	conn := NewMockConnection()
	config := ClientConfig{
		Source: SystemDevice,
	}

	client := NewClient(logger, conn, config)

	assert.NotNil(t, client)
	assert.False(t, client.IsClosed())
}

func TestClient_Listen(t *testing.T) {
	t.Run("processes valid messages", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()
		conn.On("ReadMessage").Return()
		conn.On("Close").Return(nil)

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start listening in a goroutine
		go func() {
			_ = client.Listen(ctx)
		}()

		// Create and send a valid request message
		reqMsg := map[string]any{
			"type":        TypeRequest,
			"action":      "test_action",
			"source":      SystemDevice,
			"destination": DestinationAPI,
			"request_id":  "req-123",
		}

		msgBytes, _ := json.Marshal(reqMsg)
		conn.msgCh <- msgBytes

		// Wait for message to be processed
		select {
		case msg := <-client.ReadMessage():
			req, ok := msg.(*RequestMessage)
			require.True(t, ok)
			assert.Equal(t, "test_action", req.Action)
			assert.Equal(t, "req-123", req.RequestID)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for message")
		}

		cancel()
	})

	t.Run("handles invalid messages", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()
		conn.On("ReadMessage").Return()
		conn.On("Close").Return(nil)

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start listening
		go func() {
			_ = client.Listen(ctx)
		}()

		// Send invalid JSON
		conn.msgCh <- []byte("invalid json")

		// Send valid JSON but invalid message structure
		invalidMsg := map[string]any{
			"invalid": "structure",
		}
		msgBytes, _ := json.Marshal(invalidMsg)
		conn.msgCh <- msgBytes

		// Should continue listening without crashing
		time.Sleep(100 * time.Millisecond)

		cancel()
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()
		conn.On("ReadMessage").Return()
		conn.On("Close").Return(nil)

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error)
		go func() {
			done <- client.Listen(ctx)
		}()

		// Cancel context
		cancel()

		// Should return without error
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for Listen to return")
		}
	})
}

func TestClient_Send(t *testing.T) {
	t.Run("sends request message", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		req := RequestMessage{
			Action:    "test_action",
			Source:    SystemDevice,
			RequestID: "req-123",
			Payload:   map[string]string{"key": "value"},
		}

		conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
			var envelope map[string]any
			_ = json.Unmarshal(data, &envelope)
			return envelope["type"] == TypeRequest && envelope["action"] == "test_action"
		})).Return(nil)

		err := client.Send(req)
		assert.NoError(t, err)
		conn.AssertExpectations(t)
	})

	t.Run("sends response message", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()

		config := ClientConfig{
			Source: SystemAPI,
		}
		client := NewClient(logger, conn, config)

		resp := ResponseMessage{
			Action:  "test_action",
			Source:  SystemAPI,
			ReplyTo: "req-123",
			Payload: map[string]string{"status": "success"},
		}

		conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
			var envelope map[string]any
			_ = json.Unmarshal(data, &envelope)
			return envelope["type"] == TypeResponse && envelope["reply_to"] == "req-123"
		})).Return(nil)

		err := client.Send(resp)
		assert.NoError(t, err)
		conn.AssertExpectations(t)
	})

	t.Run("sends error message", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		errMsg := ErrorMessage{
			Action:  "test_action",
			Source:  SystemDevice,
			ReplyTo: "req-123",
			Error: ErrorResponse{
				Code:    "TEST_ERROR",
				Message: "Test error message",
			},
		}

		conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
			var envelope map[string]any
			_ = json.Unmarshal(data, &envelope)
			return envelope["type"] == TypeError
		})).Return(nil)

		err := client.Send(errMsg)
		assert.NoError(t, err)
		conn.AssertExpectations(t)
	})

	t.Run("sends event message", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		event := EventMessage{
			Action:  "test_event",
			Source:  SystemDevice,
			Payload: map[string]string{"event": "data"},
		}

		conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
			var envelope map[string]any
			_ = json.Unmarshal(data, &envelope)
			return envelope["type"] == TypeEvent && envelope["action"] == "test_event"
		})).Return(nil)

		err := client.Send(event)
		assert.NoError(t, err)
		conn.AssertExpectations(t)
	})

	t.Run("adds channel ID when provided", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		req := RequestMessage{
			Action:    "test_action",
			Source:    SystemDevice,
			RequestID: "req-123",
			RoomID:    "channel-123",
		}

		conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
			var envelope map[string]any
			_ = json.Unmarshal(data, &envelope)
			return envelope["room_id"] == "channel-123"
		})).Return(nil)

		err := client.Send(req)
		assert.NoError(t, err)
		conn.AssertExpectations(t)
	})

	t.Run("returns error when client is closed", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()
		conn.On("Close").Return(nil)
		conn.On("IsClosed").Return(true)

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)
		_ = client.Close()

		req := RequestMessage{
			Action:    "test_action",
			Source:    SystemDevice,
			RequestID: "req-123",
		}

		err := client.Send(req)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "closed")
	})

	t.Run("returns error for unsupported message type", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		unsupportedMsg := struct {
			Field string
		}{
			Field: "value",
		}

		err := client.Send(unsupportedMsg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})
}

func TestClient_Close(t *testing.T) {
	t.Run("closes connection and channels", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()
		conn.On("Close").Return(nil)

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		err := client.Close()
		assert.NoError(t, err)
		assert.True(t, client.IsClosed())
		conn.AssertExpectations(t)
	})

	t.Run("handles multiple close calls", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()
		conn.On("Close").Return(nil).Once()

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		// First close
		err := client.Close()
		assert.NoError(t, err)

		// Second close should not call conn.Close again
		err = client.Close()
		assert.NoError(t, err)

		conn.AssertExpectations(t)
	})

	t.Run("handles close error", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()
		expectedErr := errors.New("close error")
		conn.On("Close").Return(expectedErr)

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		err := client.Close()
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})
}

func TestClient_IsClosed(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	conn := NewMockConnection()

	config := ClientConfig{
		Source: SystemDevice,
	}
	client := NewClient(logger, conn, config)

	// Initially not closed
	assert.False(t, client.IsClosed())

	// Close the client
	conn.On("Close").Return(nil)
	_ = client.Close()

	// Now should be closed
	assert.True(t, client.IsClosed())
}

func TestClient_ReadMessage(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	conn := NewMockConnection()

	config := ClientConfig{
		Source: SystemDevice,
	}
	client := NewClient(logger, conn, config)

	msgCh := client.ReadMessage()
	assert.NotNil(t, msgCh)

	// Channel should be readable
	select {
	case <-msgCh:
		// Should not receive anything yet
		t.Fatal("unexpected message received")
	default:
		// Expected
	}
}

func TestClient_RegisterWill(t *testing.T) {
	t.Run("registers will message successfully", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		will := WillMessage{
			Action:      "device.status",
			Payload:     map[string]any{"status": "offline", "reason": "connection_lost"},
			Destination: DestinationBroadcast,
		}

		conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
			var envelope map[string]any
			_ = json.Unmarshal(data, &envelope)
			return envelope["type"] == TypeWill && envelope["action"] == "device.status"
		})).Return(nil)

		err := client.RegisterWill(will)
		assert.NoError(t, err)
		conn.AssertExpectations(t)
	})

	t.Run("returns error when client is closed", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		conn := NewMockConnection()
		conn.On("Close").Return(nil)

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)
		_ = client.Close()

		will := WillMessage{
			Action:      "device.status",
			Destination: DestinationBroadcast,
		}

		err := client.RegisterWill(will)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "closed")
	})
}

func TestClient_ClearWill(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	conn := NewMockConnection()

	config := ClientConfig{
		Source: SystemDevice,
	}
	client := NewClient(logger, conn, config)

	// Expect an empty will message to be sent
	conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
		var envelope map[string]any
		_ = json.Unmarshal(data, &envelope)
		return envelope["type"] == TypeWill && envelope["action"] == ""
	})).Return(nil)

	err := client.ClearWill()
	assert.NoError(t, err)
	conn.AssertExpectations(t)
}

func TestClient_SourceToDestination(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	conn := NewMockConnection()

	config := ClientConfig{
		Source: SystemAPI,
	}
	client := NewClient(logger, conn, config).(*client)

	t.Run("maps web source correctly", func(t *testing.T) {
		conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
			var envelope map[string]any
			_ = json.Unmarshal(data, &envelope)
			return envelope["destination"] == DestinationWeb
		})).Return(nil).Once()

		resp := ResponseMessage{
			Action:      "test",
			Source:      SystemAPI,
			Destination: SourceToDestination(SystemWeb),
			RoomID:      "ch-1",
			ReplyTo:     "req-1",
			Payload:     map[string]string{"result": "ok"},
		}
		err := client.Send(resp)
		assert.NoError(t, err)
	})

	t.Run("maps device source correctly", func(t *testing.T) {
		conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
			var envelope map[string]any
			_ = json.Unmarshal(data, &envelope)
			return envelope["destination"] == DestinationDevice
		})).Return(nil).Once()

		resp := ResponseMessage{
			Action:      "test",
			Source:      SystemAPI,
			Destination: SourceToDestination(SystemDevice),
			RoomID:      "ch-2",
			ReplyTo:     "req-2",
			Payload:     map[string]string{"result": "ok"},
		}
		err := client.Send(resp)
		assert.NoError(t, err)
	})

	t.Run("maps api source correctly", func(t *testing.T) {
		conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
			var envelope map[string]any
			_ = json.Unmarshal(data, &envelope)
			return envelope["destination"] == DestinationAPI
		})).Return(nil).Once()

		resp := ResponseMessage{
			Action:      "test",
			Source:      SystemAPI,
			Destination: SourceToDestination(SystemAPI),
			RoomID:      "ch-3",
			ReplyTo:     "req-3",
			Payload:     map[string]string{"result": "ok"},
		}
		err := client.Send(resp)
		assert.NoError(t, err)
	})

	t.Run("handles unknown source", func(t *testing.T) {
		conn.On("SendMessage", mock.MatchedBy(func(data []byte) bool {
			var envelope map[string]any
			_ = json.Unmarshal(data, &envelope)
			// Unknown sources are passed through as-is
			return envelope["destination"] == "unknown"
		})).Return(nil).Once()

		resp := ResponseMessage{
			Action:      "test",
			Source:      SystemAPI,
			Destination: SourceToDestination(MessageSource("unknown")),
			RoomID:      "ch-4",
			ReplyTo:     "req-4",
			Payload:     map[string]string{"result": "ok"},
		}
		err := client.Send(resp)
		assert.NoError(t, err)
	})

	conn.AssertExpectations(t)
}

// ConcurrentMockConnection captures all sent messages for verification
// Used for testing concurrent sends with the buffer pool
type ConcurrentMockConnection struct {
	messages [][]byte
	mu       sync.Mutex
	closed   bool
}

func NewConcurrentMockConnection() *ConcurrentMockConnection {
	return &ConcurrentMockConnection{
		messages: make([][]byte, 0),
	}
}

func (c *ConcurrentMockConnection) SendMessage(message []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Make a copy of the message to verify it wasn't corrupted
	msgCopy := make([]byte, len(message))
	copy(msgCopy, message)
	c.messages = append(c.messages, msgCopy)
	return nil
}

func (c *ConcurrentMockConnection) ReadMessage() <-chan []byte {
	return make(chan []byte)
}

func (c *ConcurrentMockConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *ConcurrentMockConnection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *ConcurrentMockConnection) GetMessages() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([][]byte, len(c.messages))
	copy(result, c.messages)
	return result
}

// TestClient_Send_ConcurrentBufferPoolRace tests that concurrent Send calls
// don't corrupt each other's data due to buffer pool reuse.
// Run with: go test -race -run TestClient_Send_ConcurrentBufferPoolRace
func TestClient_Send_ConcurrentBufferPoolRace(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	conn := NewConcurrentMockConnection()

	config := ClientConfig{
		Source: SystemDevice,
	}
	client := NewClient(logger, conn, config)

	const numGoroutines = 100
	const messagesPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch multiple goroutines sending messages concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				// Create a unique message for this goroutine/iteration
				req := RequestMessage{
					Action:    "concurrent_test",
					Source:    SystemDevice,
					RequestID: "req-" + string(rune('A'+goroutineID)) + "-" + string(rune('0'+j%10)),
					Payload: map[string]any{
						"goroutine_id": goroutineID,
						"message_id":   j,
						"unique_data":  "data_" + string(rune('A'+goroutineID)) + "_" + string(rune('0'+j%10)),
					},
				}
				err := client.Send(req)
				if err != nil {
					t.Errorf("Send failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages were sent
	messages := conn.GetMessages()
	expectedCount := numGoroutines * messagesPerGoroutine
	require.Equal(t, expectedCount, len(messages), "Expected %d messages, got %d", expectedCount, len(messages))

	// Verify each message is valid JSON and not corrupted
	for i, msg := range messages {
		var envelope map[string]any
		err := json.Unmarshal(msg, &envelope)
		require.NoError(t, err, "Message %d is not valid JSON: %s", i, string(msg))

		// Verify the message has expected structure
		require.Equal(t, TypeRequest, envelope["type"], "Message %d has wrong type", i)
		require.Equal(t, "concurrent_test", envelope["action"], "Message %d has wrong action", i)

		// Verify payload is intact
		payload, ok := envelope["payload"].(map[string]any)
		require.True(t, ok, "Message %d payload is not a map", i)
		require.Contains(t, payload, "goroutine_id", "Message %d missing goroutine_id", i)
		require.Contains(t, payload, "message_id", "Message %d missing message_id", i)
		require.Contains(t, payload, "unique_data", "Message %d missing unique_data", i)
	}
}

// TestClient_RegisterWill_ConcurrentBufferPoolRace tests concurrent RegisterWill calls
func TestClient_RegisterWill_ConcurrentBufferPoolRace(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	conn := NewConcurrentMockConnection()

	config := ClientConfig{
		Source: SystemDevice,
	}
	client := NewClient(logger, conn, config)

	const numGoroutines = 50
	const messagesPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				will := WillMessage{
					Action:      "device.status",
					Destination: DestinationBroadcast,
					Payload: map[string]any{
						"goroutine_id": goroutineID,
						"message_id":   j,
						"status":       "offline",
					},
				}
				err := client.RegisterWill(will)
				if err != nil {
					t.Errorf("RegisterWill failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages were sent and are valid
	messages := conn.GetMessages()
	expectedCount := numGoroutines * messagesPerGoroutine
	require.Equal(t, expectedCount, len(messages), "Expected %d messages, got %d", expectedCount, len(messages))

	for i, msg := range messages {
		var envelope map[string]any
		err := json.Unmarshal(msg, &envelope)
		require.NoError(t, err, "Message %d is not valid JSON: %s", i, string(msg))
		require.Equal(t, TypeWill, envelope["type"], "Message %d has wrong type", i)
	}
}

// TestClient_Send_MixedConcurrentBufferPoolRace tests mixed Send and RegisterWill
// to ensure the buffer pool handles different message types correctly
func TestClient_Send_MixedConcurrentBufferPoolRace(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	conn := NewConcurrentMockConnection()

	config := ClientConfig{
		Source: SystemDevice,
	}
	client := NewClient(logger, conn, config)

	const numGoroutines = 50
	const messagesPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // Half Send, half RegisterWill

	// Send goroutines
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				req := RequestMessage{
					Action:    "mixed_test",
					Source:    SystemDevice,
					RequestID: "req-mixed-" + string(rune('A'+goroutineID)),
					Payload: map[string]any{
						"type":         "request",
						"goroutine_id": goroutineID,
						"message_id":   j,
					},
				}
				_ = client.Send(req)
			}
		}(i)
	}

	// RegisterWill goroutines
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				will := WillMessage{
					Action:      "device.will",
					Destination: DestinationBroadcast,
					Payload: map[string]any{
						"type":         "will",
						"goroutine_id": goroutineID,
						"message_id":   j,
					},
				}
				_ = client.RegisterWill(will)
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages were sent and are valid
	messages := conn.GetMessages()
	expectedCount := numGoroutines * messagesPerGoroutine * 2
	require.Equal(t, expectedCount, len(messages), "Expected %d messages, got %d", expectedCount, len(messages))

	requestCount := 0
	willCount := 0

	for i, msg := range messages {
		var envelope map[string]any
		err := json.Unmarshal(msg, &envelope)
		require.NoError(t, err, "Message %d is not valid JSON: %s", i, string(msg))

		msgType := envelope["type"].(string)
		switch msgType {
		case TypeRequest:
			requestCount++
		case TypeWill:
			willCount++
		default:
			t.Errorf("Unexpected message type: %s", msgType)
		}
	}

	expectedEach := numGoroutines * messagesPerGoroutine
	require.Equal(t, expectedEach, requestCount, "Expected %d request messages", expectedEach)
	require.Equal(t, expectedEach, willCount, "Expected %d will messages", expectedEach)
}
