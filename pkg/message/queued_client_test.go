package message

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockClient is a mock implementation of the Client interface
type MockClient struct {
	mock.Mock
	closed     bool
	closedLock sync.RWMutex
}

func (m *MockClient) Listen(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockClient) SendMessageToChannel(id ChannelID, msg any) error {
	args := m.Called(id, msg)
	return args.Error(0)
}

func (m *MockClient) SendBroadcastMessage(msg any) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockClient) Send(msg any, sessionId *ChannelID) error {
	args := m.Called(msg, sessionId)
	return args.Error(0)
}

func (m *MockClient) Close() error {
	m.closedLock.Lock()
	m.closed = true
	m.closedLock.Unlock()
	args := m.Called()
	return args.Error(0)
}

func (m *MockClient) IsClosed() bool {
	m.closedLock.RLock()
	defer m.closedLock.RUnlock()
	return m.closed
}

func (m *MockClient) ReadMessage() <-chan any {
	args := m.Called()
	if ch := args.Get(0); ch != nil {
		return ch.(chan any)
	}
	return nil
}

func (m *MockClient) SendResponse(req *RequestMessage, payload any) error {
	args := m.Called(req, payload)
	return args.Error(0)
}

func (m *MockClient) SendErrorToChannel(req *RequestMessage, payload ErrorResponse) error {
	args := m.Called(req, payload)
	return args.Error(0)
}

func (m *MockClient) SendEventToChannel(action MessageAction, payload any, sessionID ChannelID) error {
	args := m.Called(action, payload, sessionID)
	return args.Error(0)
}

func (m *MockClient) RegisterWill(will WillMessage) error {
	args := m.Called(will)
	return args.Error(0)
}

func (m *MockClient) ClearWill() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClient) SetClosed(closed bool) {
	m.closedLock.Lock()
	m.closed = closed
	m.closedLock.Unlock()
}

// Helper function to create a properly configured mock client
func createMockClient() *MockClient {
	mockClient := new(MockClient)
	mockClient.On("Close").Return(nil).Maybe() // Allow Close to be called
	return mockClient
}

func TestQueuedClient_QueueMessagesWhenDisconnected(t *testing.T) {
	// Setup
	mockClient := createMockClient()
	logger := log.WithField("test", "QueuedClient")
	config := &QueueConfig{
		MaxQueueSize:  10,
		MaxMessageAge: 30 * time.Second,
		FlushInterval: 100 * time.Millisecond,
		MaxRetries:    3,
		Source:        SystemDevice,
	}

	// Create QueuedClient
	qc := NewQueuedClient(mockClient, logger, config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	// Simulate disconnection
	mockClient.SetClosed(true)
	connectionError := errors.New("client connection is closed")

	// Setup mock to return connection error
	channelID := ChannelID("test-channel")
	mockClient.On("Send", mock.Anything, &channelID).Return(connectionError)

	// Send a message while disconnected
	msg := EventMessage{
		Action:    "test.event",
		Payload:   map[string]any{"data": "test"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}

	err := qc.Send(msg, &channelID)

	// Should return error for non-critical message
	assert.Error(t, err)
	assert.Equal(t, 1, qc.GetQueueSize(), "Message should be queued")
}

func TestQueuedClient_CriticalMessageNoError(t *testing.T) {
	// Setup
	mockClient := createMockClient()
	logger := log.WithField("test", "QueuedClient")
	config := DefaultQueueConfig()
	config.FlushInterval = 100 * time.Millisecond
	// Configure to treat "critical.event" as critical
	config.CriticalMessageActions = []string{"critical.event"}

	// Create QueuedClient
	qc := NewQueuedClient(mockClient, logger, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	// Simulate disconnection
	mockClient.SetClosed(true)
	connectionError := errors.New("client connection is closed")

	// Setup mock to return connection error
	channelID := ChannelID("test-channel")
	mockClient.On("Send", mock.Anything, &channelID).Return(connectionError)

	// Send a critical message while disconnected
	msg := EventMessage{
		Action:    "critical.event",
		Payload:   map[string]any{"data": "test"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}

	err := qc.Send(msg, &channelID)

	// Should NOT return error for critical message
	assert.NoError(t, err, "Critical message should not return error")
	assert.Equal(t, 1, qc.GetQueueSize(), "Message should be queued")

	// Verify it's marked as critical
	qc.queueMutex.Lock()
	assert.True(t, qc.queue[0].Critical, "Message should be marked as critical")
	qc.queueMutex.Unlock()
}

func TestQueuedClient_FlushOnReconnection(t *testing.T) {
	// Setup
	mockClient := createMockClient()
	logger := log.WithField("test", "QueuedClient")
	config := DefaultQueueConfig()
	config.FlushInterval = 10 * time.Millisecond // Short interval for testing
	// Configure to treat all test.event* as critical
	config.CriticalMessageActions = []string{"test.event*"}

	// Create QueuedClient
	qc := NewQueuedClient(mockClient, logger, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	// Simulate disconnection
	mockClient.SetClosed(true)
	connectionError := errors.New("client connection is closed")

	channelID := ChannelID("test-channel")

	// Setup mock: first 2 calls fail, subsequent succeed
	callCount := 0
	mockClient.On("Send", mock.Anything, &channelID).Return(connectionError).Times(2)
	mockClient.On("Send", mock.Anything, &channelID).Return(nil).Run(func(args mock.Arguments) {
		callCount++
	})

	// Send messages while disconnected
	msg1 := EventMessage{
		Action:    "test.event1",
		Payload:   map[string]any{"data": "test1"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}
	msg2 := EventMessage{
		Action:    "test.event2",
		Payload:   map[string]any{"data": "test2"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}

	err := qc.Send(msg1, &channelID)
	require.NoError(t, err)
	err = qc.Send(msg2, &channelID)
	require.NoError(t, err)

	assert.Equal(t, 2, qc.GetQueueSize(), "Both messages should be queued")

	// Simulate reconnection
	mockClient.SetClosed(false)

	// Manually trigger flush and wait for completion
	qc.FlushQueueSync()

	// Verify queue is empty
	assert.True(t, qc.WaitForQueueEmpty(100*time.Millisecond), "Queue should be empty after flush")

	// Verify that messages were successfully sent after reconnection
	assert.GreaterOrEqual(t, callCount, 2, "Both queued messages should have been sent")
}

func TestQueuedClient_MessageExpiration(t *testing.T) {
	// Setup
	mockClient := createMockClient()
	logger := log.WithField("test", "QueuedClient")
	config := DefaultQueueConfig()
	config.MaxMessageAge = 1 * time.Nanosecond // Very short expiration for testing
	config.FlushInterval = 1 * time.Hour       // Disable auto-flush
	// Configure to treat all test.event* as critical
	config.CriticalMessageActions = []string{"test.event*"}

	// Create QueuedClient
	qc := NewQueuedClient(mockClient, logger, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	// Simulate disconnection
	mockClient.SetClosed(true)
	connectionError := errors.New("client connection is closed")

	channelID := ChannelID("test-channel")
	mockClient.On("Send", mock.Anything, &channelID).Return(connectionError).Once()

	// Send a message
	msg := EventMessage{
		Action:    "test.event",
		Payload:   map[string]any{"data": "test"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}

	err := qc.Send(msg, &channelID)
	require.NoError(t, err)
	assert.Equal(t, 1, qc.GetQueueSize(), "Message should be queued")

	// Manually set message timestamp to be expired
	qc.queueMutex.Lock()
	if len(qc.queue) > 0 {
		qc.queue[0].Timestamp = time.Now().Add(-1 * time.Hour) // Set to 1 hour ago
	}
	qc.queueMutex.Unlock()

	// Simulate reconnection
	mockClient.SetClosed(false)
	mockClient.On("Send", mock.Anything, &channelID).Return(nil).Maybe()

	// Trigger flush
	qc.FlushQueueSync()

	// Message should have been dropped due to age
	assert.Equal(t, 0, qc.GetQueueSize(), "Expired message should be removed")
}

func TestQueuedClient_QueueSizeLimit(t *testing.T) {
	// Setup
	mockClient := createMockClient()
	logger := log.WithField("test", "QueuedClient")
	config := DefaultQueueConfig()
	config.MaxQueueSize = 3
	config.FlushInterval = 1 * time.Second // Long interval to prevent auto-flush

	// Create QueuedClient
	qc := NewQueuedClient(mockClient, logger, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	// Simulate disconnection
	mockClient.SetClosed(true)
	connectionError := errors.New("client connection is closed")

	channelID := ChannelID("test-channel")
	mockClient.On("Send", mock.Anything, &channelID).Return(connectionError)

	// Send more messages than queue size
	for i := 0; i < 5; i++ {
		msg := EventMessage{
			Action:    MessageAction("test.event" + string(rune(i))),
			Payload:   map[string]any{"index": i},
			Source:    SystemDevice,
			ChannelID: channelID,
		}
		_ = qc.Send(msg, &channelID)
	}

	// Queue should not exceed max size
	assert.Equal(t, 3, qc.GetQueueSize(), "Queue should not exceed max size")

	// Check that oldest messages were dropped (first 2 messages)
	qc.queueMutex.Lock()
	firstMsg := qc.queue[0].Message.(EventMessage)
	qc.queueMutex.Unlock()

	assert.Equal(t, MessageAction("test.event"+string(rune(2))), firstMsg.Action,
		"Oldest messages should have been dropped")
}

func TestQueuedClient_CriticalMessagePriority(t *testing.T) {
	// Setup
	mockClient := createMockClient()
	logger := log.WithField("test", "QueuedClient")
	config := DefaultQueueConfig()
	config.MaxQueueSize = 2
	config.FlushInterval = 1 * time.Second
	// Configure to treat "critical.event" as critical
	config.CriticalMessageActions = []string{"critical.event"}

	// Create QueuedClient
	qc := NewQueuedClient(mockClient, logger, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	// Simulate disconnection
	mockClient.SetClosed(true)
	connectionError := errors.New("client connection is closed")

	channelID := ChannelID("test-channel")
	mockClient.On("Send", mock.Anything, &channelID).Return(connectionError)

	// Send normal message
	normalMsg := EventMessage{
		Action:    "normal.event",
		Payload:   map[string]any{"type": "normal"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}
	_ = qc.Send(normalMsg, &channelID)

	// Send critical message
	criticalMsg := EventMessage{
		Action:    "critical.event",
		Payload:   map[string]any{"type": "critical"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}
	_ = qc.Send(criticalMsg, &channelID)

	// Try to add another normal message (should drop the first normal message)
	normalMsg2 := EventMessage{
		Action:    "normal.event2",
		Payload:   map[string]any{"type": "normal2"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}
	_ = qc.Send(normalMsg2, &channelID)

	assert.Equal(t, 2, qc.GetQueueSize(), "Queue should be at max size")

	// Verify critical message is still in queue
	qc.queueMutex.Lock()
	hasCritical := false
	for _, msg := range qc.queue {
		if msg.Critical {
			hasCritical = true
			break
		}
	}
	qc.queueMutex.Unlock()

	assert.True(t, hasCritical, "Critical message should be preserved in queue")
}

func TestQueuedClient_MaxRetries(t *testing.T) {
	// Setup
	mockClient := createMockClient()
	logger := log.WithField("test", "QueuedClient")
	config := DefaultQueueConfig()
	config.MaxRetries = 2
	config.FlushInterval = 1 * time.Hour // Disable auto-flush

	// Create QueuedClient
	qc := NewQueuedClient(mockClient, logger, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	// Start with disconnected state
	mockClient.SetClosed(true)
	connectionError := errors.New("client connection is closed")

	channelID := ChannelID("test-channel")

	// First call fails (initial send)
	mockClient.On("Send", mock.Anything, &channelID).Return(connectionError).Once()

	// Send message while disconnected
	msg := EventMessage{
		Action:    "test.event",
		Payload:   map[string]any{"data": "test"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}
	_ = qc.Send(msg, &channelID)

	// Simulate reconnection
	mockClient.SetClosed(false)

	// Setup to always fail for retry attempts
	sendError := errors.New("send failed")
	mockClient.On("Send", mock.Anything, &channelID).Return(sendError)

	// Manually trigger flushes to simulate retries
	for i := 0; i <= config.MaxRetries; i++ {
		qc.FlushQueueSync()
	}

	// After max retries, message should be dropped
	assert.Equal(t, 0, qc.GetQueueSize(), "Message should be dropped after max retries")
}

func TestQueuedClient_GetQueueStats(t *testing.T) {
	// Setup
	mockClient := createMockClient()
	logger := log.WithField("test", "QueuedClient")
	config := DefaultQueueConfig()
	// Configure to treat "critical.event" as critical
	config.CriticalMessageActions = []string{"critical.event"}

	// Create QueuedClient
	qc := NewQueuedClient(mockClient, logger, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	// Simulate disconnection
	mockClient.SetClosed(true)
	connectionError := errors.New("client connection is closed")

	channelID := ChannelID("test-channel")
	mockClient.On("Send", mock.Anything, &channelID).Return(connectionError)

	// Send mixed messages
	normalMsg := EventMessage{
		Action:    "normal.event",
		Payload:   map[string]any{"type": "normal"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}
	_ = qc.Send(normalMsg, &channelID)

	criticalMsg := EventMessage{
		Action:    "critical.event",
		Payload:   map[string]any{"type": "critical"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}
	_ = qc.Send(criticalMsg, &channelID)

	// Get stats
	stats := qc.GetQueueStats()

	assert.Equal(t, 2, stats["total"], "Should have 2 messages total")
	assert.Equal(t, 1, stats["critical"], "Should have 1 critical message")
	assert.Equal(t, 1, stats["normal"], "Should have 1 normal message")
	assert.NotNil(t, stats["oldest_age"], "Should have oldest age")
}

func TestQueuedClient_SendEventToChannel(t *testing.T) {
	// Setup
	mockClient := createMockClient()
	logger := log.WithField("test", "QueuedClient")
	config := DefaultQueueConfig()

	// Create QueuedClient
	qc := NewQueuedClient(mockClient, logger, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	channelID := ChannelID("test-channel")
	action := MessageAction("test.action")
	payload := map[string]any{"data": "test"}

	// Setup mock for SendEventToChannel (QueuedClient delegates to underlying client)
	mockClient.On("SendEventToChannel", action, payload, channelID).Return(nil)

	// Send event
	err := qc.SendEventToChannel(action, payload, channelID)

	require.NoError(t, err)
	mockClient.AssertCalled(t, "SendEventToChannel", action, payload, channelID)
}

func TestQueuedClient_ConcurrentAccess(t *testing.T) {
	// Setup
	mockClient := createMockClient()
	logger := log.WithField("test", "QueuedClient")
	config := DefaultQueueConfig()
	config.FlushInterval = 1 * time.Hour // Disable auto-flush

	// Create QueuedClient
	qc := NewQueuedClient(mockClient, logger, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	// Simulate disconnection
	mockClient.SetClosed(true)
	connectionError := errors.New("client connection is closed")
	// Use Times(10) instead of Maybe() to limit error returns to exactly the disconnected sends
	mockClient.On("Send", mock.Anything, mock.Anything).Return(connectionError).Times(10)

	// Concurrent sends
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			channelID := ChannelID("channel")
			msg := EventMessage{
				Action:    MessageAction("test.event"),
				Payload:   map[string]any{"index": index},
				Source:    SystemDevice,
				ChannelID: channelID,
			}
			_ = qc.Send(msg, &channelID)
		}(i)
	}

	wg.Wait()

	// All messages should be queued
	assert.Equal(t, 10, qc.GetQueueSize(), "All messages should be queued")

	// Test concurrent flush and send - setup successful sends for reconnection
	mockClient.SetClosed(false)
	// Expect up to 11 successful sends (10 queued + 1 concurrent new message)
	mockClient.On("Send", mock.Anything, mock.Anything).Return(nil).Times(11)

	// Trigger concurrent operations
	var flushWg sync.WaitGroup
	flushWg.Add(2)

	go func() {
		defer flushWg.Done()
		// Flush multiple times to ensure all messages are processed
		for i := 0; i < 3; i++ {
			qc.FlushQueueSync()
		}
	}()

	go func() {
		defer flushWg.Done()
		channelID := ChannelID("channel")
		msg := EventMessage{
			Action:    "concurrent.event",
			Payload:   map[string]any{"concurrent": true},
			Source:    SystemDevice,
			ChannelID: channelID,
		}
		_ = qc.Send(msg, &channelID)
	}()

	// Wait for operations to complete
	flushWg.Wait()

	// Give a final flush to ensure everything is processed
	qc.FlushQueueSync()

	// Wait for queue to empty with timeout
	assert.True(t, qc.WaitForQueueEmpty(500*time.Millisecond), "Queue should be empty after flush")
}

// Benchmark tests

func BenchmarkQueuedClient_Send(b *testing.B) {
	mockClient := createMockClient()
	logger := log.New()
	logger.SetLevel(log.WarnLevel)
	logEntry := log.NewEntry(logger)

	config := DefaultQueueConfig()
	qc := NewQueuedClient(mockClient, logEntry, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	channelID := ChannelID("bench-channel")
	mockClient.On("Send", mock.Anything, &channelID).Return(nil)

	msg := EventMessage{
		Action:    "bench.event",
		Payload:   map[string]any{"data": "benchmark"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = qc.Send(msg, &channelID)
	}
}

func BenchmarkQueuedClient_QueueAndFlush(b *testing.B) {
	mockClient := createMockClient()
	logger := log.New()
	logger.SetLevel(log.WarnLevel)
	logEntry := log.NewEntry(logger)

	config := DefaultQueueConfig()
	config.FlushInterval = 1 * time.Hour // Disable auto-flush
	qc := NewQueuedClient(mockClient, logEntry, &config).(*QueuedClient)
	defer func() {
		_ = qc.Close()
	}()

	channelID := ChannelID("bench-channel")

	// Start disconnected
	mockClient.SetClosed(true)
	mockClient.On("Send", mock.Anything, &channelID).Return(errors.New("disconnected")).Times(b.N)

	msg := EventMessage{
		Action:    "bench.event",
		Payload:   map[string]any{"data": "benchmark"},
		Source:    SystemDevice,
		ChannelID: channelID,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = qc.Send(msg, &channelID)
	}

	// Now benchmark the flush
	mockClient.SetClosed(false)
	mockClient.On("Send", mock.Anything, &channelID).Return(nil)

	b.StartTimer()
	qc.flushQueue()
	b.StopTimer()
}
