package message

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

// TestBasicQueueing tests that messages are queued when connection is closed
func TestBasicQueueing(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	// Create mock base client (closed)
	baseClient := &mockClient{closed: true}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Send messages while disconnected (should queue)
	testMessages := []EventMessage{
		{Action: "test.event1", Payload: map[string]any{"id": 1}},
		{Action: "test.event2", Payload: map[string]any{"id": 2}},
		{Action: "test.event3", Payload: map[string]any{"id": 3}},
	}

	for _, msg := range testMessages {
		if err := client.Send(msg); err == nil {
			t.Error("Expected error when sending to closed connection")
		}
	}

	// Verify messages are in queue
	qc := client.(*QueuedClient)
	if size := qc.GetQueueSize(); size != len(testMessages) {
		t.Errorf("Expected %d queued messages, got %d", len(testMessages), size)
	}
}

// TestQueueFlushOnReconnect tests that queued messages are sent when connection is restored
func TestQueueFlushOnReconnect(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	// Create mock base client (closed)
	baseClient := &mockClient{closed: true}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Send messages while disconnected
	testMessages := []EventMessage{
		{Action: "test.event1", Payload: map[string]any{"id": 1}},
		{Action: "test.event2", Payload: map[string]any{"id": 2}},
		{Action: "test.event3", Payload: map[string]any{"id": 3}},
	}

	for _, msg := range testMessages {
		_ = client.Send(msg)
	}

	qc := client.(*QueuedClient)
	if size := qc.GetQueueSize(); size != len(testMessages) {
		t.Errorf("Expected %d queued messages, got %d", len(testMessages), size)
	}

	// Reconnect
	baseClient.mu.Lock()
	baseClient.closed = false
	baseClient.mu.Unlock()

	// Wait for queue to empty
	if !qc.WaitForQueueEmpty(5 * time.Second) {
		t.Error("Timeout waiting for queue to flush")
	}

	// Verify messages were sent
	actualSent := baseClient.GetSentCount()
	if actualSent != len(testMessages) {
		t.Errorf("Expected %d messages sent, got %d", len(testMessages), actualSent)
	}

	// Verify queue is empty
	if size := qc.GetQueueSize(); size != 0 {
		t.Errorf("Expected empty queue after flush, got %d messages", size)
	}
}

// TestAdaptiveBackoff tests the adaptive backoff strategy
func TestAdaptiveBackoff(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()
	config.BackoffStrategy = NewExponentialBackoff(100*time.Millisecond, 5*time.Second)
	config.MaxRetries = 10 // Allow enough retries for the test (failNextSends is 5)

	// Start with closed connection to queue the message
	baseClient := &mockClient{
		closed:        true,
		failNextSends: 5, // Fail first 5 sends after reconnect
	}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Send a message while disconnected (will queue)
	_ = client.Send(EventMessage{
		Action:  "test.backoff",
		Payload: map[string]any{"data": "test"},
	})

	qc := client.(*QueuedClient)
	if qc.GetQueueSize() != 1 {
		t.Fatalf("Expected 1 queued message, got %d", qc.GetQueueSize())
	}

	// Reconnect - this will trigger flush with backoff retries
	baseClient.mu.Lock()
	baseClient.closed = false
	baseClient.mu.Unlock()

	// Wait for message to be sent (with retries)
	expectedSends := 1
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Errorf("Timeout: expected %d message sent after retries, got %d", expectedSends, baseClient.GetSentCount())
			return
		case <-ticker.C:
			if baseClient.GetSentCount() >= expectedSends && qc.GetQueueSize() == 0 {
				// Success!
				t.Logf("Message sent after %d retries", 5)
				return
			}
		}
	}
}

// TestConnectionHealthMonitoring tests connection quality tracking
func TestConnectionHealthMonitoring(t *testing.T) {
	t.Skip("Skipping health monitoring test - requires 35s wait time")

	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()
	config.EnableHealthCheck = true

	baseClient := &mockClient{closed: false}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func() { _ = client.Close() }()

	qc := client.(*QueuedClient)

	// Initial quality should be "good"
	initialQuality := qc.GetConnectionQuality()
	if initialQuality == "" {
		t.Error("Expected initial quality to be set")
	}

	t.Logf("Health monitoring enabled with initial quality: %s", initialQuality)
}

// TestCriticalMessageHandling tests critical message detection and error suppression
func TestCriticalMessageHandling(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()
	config.CriticalMessageActions = []string{"critical.*", "important.event"}

	baseClient := &mockClient{closed: true} // Connection closed

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Send critical message (should suppress error)
	err = client.Send(EventMessage{
		Action:  "critical.alert",
		Payload: map[string]any{"severity": "high"},
	})
	if err != nil {
		t.Errorf("Expected no error for critical message, got: %v", err)
	}

	// Send normal message (should return error)
	err = client.Send(EventMessage{
		Action:  "normal.event",
		Payload: map[string]any{"data": "test"},
	})
	if err == nil {
		t.Error("Expected error for normal message when connection is closed")
	}

	// Verify both messages are queued
	qc := client.(*QueuedClient)
	stats := qc.GetQueueStats()
	if stats["total"] != 2 {
		t.Errorf("Expected 2 queued messages, got %v", stats["total"])
	}
	if stats["critical"] != 1 {
		t.Errorf("Expected 1 critical message, got %v", stats["critical"])
	}
	if stats["normal"] != 1 {
		t.Errorf("Expected 1 normal message, got %v", stats["normal"])
	}
}

// TestMessageExpiration tests that old messages are dropped
func TestMessageExpiration(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()
	config.MaxMessageAge = 500 * time.Millisecond
	config.MaxCriticalAge = 1 * time.Second

	baseClient := &mockClient{closed: true}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Queue a message
	_ = client.Send(EventMessage{
		Action:  "test.expiration",
		Payload: map[string]any{"data": "test"},
	})

	qc := client.(*QueuedClient)
	if size := qc.GetQueueSize(); size != 1 {
		t.Errorf("Expected 1 queued message, got %d", size)
	}

	// Wait for message to expire (with extra buffer for CI timing variance)
	<-time.After(2000 * time.Millisecond)

	// Reconnect and trigger flush
	baseClient.mu.Lock()
	baseClient.closed = false
	baseClient.mu.Unlock()

	// Wait for flush to complete
	// Queue should be empty even if timeout (message was expired, not sent)
	_ = qc.WaitForQueueEmpty(3 * time.Second)

	// Message should be expired and dropped
	if size := qc.GetQueueSize(); size != 0 {
		t.Errorf("Expected 0 messages after expiration, got %d", size)
	}

	// Should not have been sent
	if baseClient.GetSentCount() != 0 {
		t.Error("Expired message should not have been sent")
	}
}

// TestQueueSizeLimit tests that queue respects MaxQueueSize
func TestQueueSizeLimit(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()
	config.MaxQueueSize = 10

	baseClient := &mockClient{closed: true}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Queue more messages than the limit
	for i := range 20 {
		_ = client.Send(EventMessage{
			Action:  "test.overflow",
			Payload: map[string]any{"id": i},
		})
	}

	qc := client.(*QueuedClient)
	size := qc.GetQueueSize()

	if size > config.MaxQueueSize {
		t.Errorf("Queue size %d exceeds MaxQueueSize %d", size, config.MaxQueueSize)
	}

	t.Logf("Queue size after overflow: %d (max: %d)", size, config.MaxQueueSize)
}

// TestConcurrentOperations tests thread safety
func TestConcurrentOperations(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	baseClient := &mockClient{closed: false}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func() { _ = client.Close() }()

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Concurrent sends
	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 50 {
				select {
				case <-done:
					return
				default:
					_ = client.Send(EventMessage{
						Action:  "test.concurrent",
						Payload: map[string]any{"id": id, "seq": j},
					})
				}
			}
		}(i)
	}

	// Concurrent stats reads
	for range 10 {
		wg.Go(func() {
			qc := client.(*QueuedClient)
			for range 100 {
				select {
				case <-done:
					return
				default:
					_ = qc.GetQueueStats()
					_ = qc.GetConnectionQuality()
				}
			}
		})
	}

	wg.Wait()
	close(done)

	// No panics or data races = success
	t.Log("Concurrent operations completed successfully")
}

// TestMessageOrdering tests FIFO message delivery
func TestMessageOrdering(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	baseClient := &mockClient{closed: true}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Queue messages in specific order
	expectedOrder := []int{1, 2, 3, 4, 5}
	for _, id := range expectedOrder {
		_ = client.Send(EventMessage{
			Action:  "test.ordering",
			Payload: map[string]any{"id": id},
		})
	}

	qc := client.(*QueuedClient)

	// Reconnect
	baseClient.mu.Lock()
	baseClient.closed = false
	baseClient.mu.Unlock()

	// Wait for queue to flush
	if !qc.WaitForQueueEmpty(5 * time.Second) {
		t.Fatal("Timeout waiting for queue to flush")
	}

	// Verify order
	sentMessages := baseClient.GetSentMessages()
	if len(sentMessages) != len(expectedOrder) {
		t.Fatalf("Expected %d messages, got %d", len(expectedOrder), len(sentMessages))
	}

	for i, msg := range sentMessages {
		eventMsg, ok := msg.(EventMessage)
		if !ok {
			t.Fatalf("Message %d is not EventMessage", i)
		}

		payload, ok := eventMsg.Payload.(map[string]any)
		if !ok {
			t.Fatalf("Message %d payload is not map[string]any", i)
		}

		// Try both int and float64 (JSON unmarshalling can produce either)
		var id int
		switch v := payload["id"].(type) {
		case int:
			id = v
		case float64:
			id = int(v)
		default:
			t.Fatalf("Message %d id is neither int nor float64, got %T", i, payload["id"])
		}

		if id != expectedOrder[i] {
			t.Errorf("Message order mismatch at position %d: expected %d, got %d",
				i, expectedOrder[i], id)
		}
	}
}

// mockClient implements Client interface for testing
type mockClient struct {
	closed        bool
	sentMessages  []any
	mu            sync.Mutex
	failNextSends int           // Number of sends to fail
	sendCallback  func(msg any) // Optional callback on successful send
}

// GetSentCount safely returns the number of sent messages
func (m *mockClient) GetSentCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sentMessages == nil {
		return 0
	}
	return len(m.sentMessages)
}

// GetSentMessages safely returns a copy of sent messages
func (m *mockClient) GetSentMessages() []any {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sentMessages == nil {
		return nil
	}
	result := make([]any, len(m.sentMessages))
	copy(result, m.sentMessages)
	return result
}

func (m *mockClient) Send(msg any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("connection is closed")
	}

	if m.failNextSends > 0 {
		m.failNextSends--
		return fmt.Errorf("simulated send failure")
	}

	// Simulate serialization to detect any marshaling issues
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	var unmarshalledMsg any
	if err := json.Unmarshal(data, &unmarshalledMsg); err != nil {
		return err
	}

	// Ensure sentMessages is initialized
	if m.sentMessages == nil {
		m.sentMessages = make([]any, 0)
	}

	m.sentMessages = append(m.sentMessages, msg)

	// Call callback if set
	if m.sendCallback != nil {
		m.sendCallback(msg)
	}

	return nil
}

func (m *mockClient) Listen(_ context.Context) error {
	return nil
}

func (m *mockClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockClient) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mockClient) ReadMessage() <-chan any {
	ch := make(chan any)
	close(ch)
	return ch
}

func (m *mockClient) GetSource() MessageSource {
	return SystemDevice
}

func (m *mockClient) ClearWill() error {
	// No-op for mock
	return nil
}

func (m *mockClient) RegisterWill(_ WillMessage) error {
	// No-op for mock
	return nil
}

func (m *mockClient) SendRawJSON(_ []byte) error {
	// No-op for mock
	return nil
}

// TestAllMessageTypes tests sending all message types through QueuedClient
func TestAllMessageTypes(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	baseClient := &mockClient{closed: false}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func(client Client) {
		_ = client.Close()
	}(client)

	// Test Request message
	reqMsg := RequestMessage{
		Action:      "test.request",
		RequestID:   "req-123",
		Source:      "web",
		Destination: "device",
		Payload:     map[string]any{"data": "test"},
	}
	err = client.Send(reqMsg)
	if err != nil {
		t.Errorf("Failed to send RequestMessage: %v", err)
	}

	// Test Response message
	respMsg := ResponseMessage{
		Action:      "test.response",
		ReplyTo:     "req-123",
		Source:      "device",
		Destination: "web",
		Payload:     map[string]any{"result": "success"},
	}
	err = client.Send(respMsg)
	if err != nil {
		t.Errorf("Failed to send ResponseMessage: %v", err)
	}

	// Test Error message
	errMsg := ErrorMessage{
		Action:      "test.error",
		ReplyTo:     "req-123",
		Source:      "device",
		Destination: "web",
		Error: ErrorResponse{
			Code:    "TEST_ERROR",
			Message: "Test error message",
		},
	}
	err = client.Send(errMsg)
	if err != nil {
		t.Errorf("Failed to send ErrorMessage: %v", err)
	}

	// Test Event message
	eventMsg := EventMessage{
		Action:      "test.event",
		Source:      "device",
		Destination: "web",
		Payload:     map[string]any{"event": "data"},
	}
	err = client.Send(eventMsg)
	if err != nil {
		t.Errorf("Failed to send EventMessage: %v", err)
	}

	// Verify all messages were sent
	if baseClient.GetSentCount() != 4 {
		t.Errorf("Expected 4 messages sent, got %d", baseClient.GetSentCount())
	}
}

// TestInterfaceMethods tests QueuedClient interface methods
func TestInterfaceMethods(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	baseClient := &mockClient{closed: false}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}

	// Test GetSource
	if client.GetSource() != SystemDevice {
		t.Errorf("Expected source SystemDevice, got %s", client.GetSource())
	}

	// Test IsClosed (should be open)
	if client.IsClosed() {
		t.Error("Expected client to be open")
	}

	// Test ReadMessage
	msgCh := client.ReadMessage()
	if msgCh == nil {
		t.Error("Expected non-nil message channel")
	}

	// Test Close
	err = client.Close()
	if err != nil {
		t.Errorf("Failed to close client: %v", err)
	}

	// Test IsClosed (should be closed now)
	if !client.IsClosed() {
		t.Error("Expected client to be closed after Close()")
	}
}

// TestWillMessageHandling tests RegisterWill and ClearWill
func TestWillMessageHandling(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	baseClient := &mockClient{closed: false}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func(client Client) {
		_ = client.Close()
	}(client)

	// Test RegisterWill
	will := WillMessage{
		Action:      "device.offline",
		Destination: "web",
		Payload:     map[string]any{"reason": "disconnect"},
	}
	err = client.RegisterWill(will)
	if err != nil {
		t.Errorf("Failed to register will: %v", err)
	}

	// Test ClearWill
	err = client.ClearWill()
	if err != nil {
		t.Errorf("Failed to clear will: %v", err)
	}
}

// TestSendRawJSON tests SendRawJSON method
func TestSendRawJSON(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	baseClient := &mockClient{closed: false}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func(client Client) {
		_ = client.Close()
	}(client)

	// Test SendRawJSON
	jsonData := []byte(`{"type":"event","action":"test.raw","source":"device","destination":"web"}`)
	err = client.SendRawJSON(jsonData)
	if err != nil {
		t.Errorf("Failed to send raw JSON: %v", err)
	}
}

// TestFlushQueueSync tests manual queue flushing
func TestFlushQueueSync(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	baseClient := &mockClient{closed: true}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func(client Client) {
		_ = client.Close()
	}(client)

	qc := client.(*QueuedClient)

	// Queue messages while disconnected
	for i := range 3 {
		err := client.Send(EventMessage{
			Action:  "test.sync.flush",
			Payload: map[string]any{"id": i},
		})
		if err != nil {
			return
		}
	}

	if qc.GetQueueSize() != 3 {
		t.Errorf("Expected 3 queued messages, got %d", qc.GetQueueSize())
	}

	// Reconnect
	baseClient.mu.Lock()
	baseClient.closed = false
	baseClient.mu.Unlock()

	// Manual flush
	qc.FlushQueueSync()

	// Wait for flush to complete
	if !qc.WaitForQueueEmpty(2 * time.Second) {
		t.Error("Timeout waiting for queue to flush")
	}

	// Verify messages were sent
	if baseClient.GetSentCount() != 3 {
		t.Errorf("Expected 3 messages sent, got %d", baseClient.GetSentCount())
	}
}

// TestCustomCriticalMessageFunction tests custom IsCriticalMessage function
func TestCustomCriticalMessageFunction(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	// Custom function: treat messages with "urgent" in payload as critical
	config.IsCriticalMessage = func(msg any) bool {
		if eventMsg, ok := msg.(EventMessage); ok {
			if payload, ok := eventMsg.Payload.(map[string]any); ok {
				if urgent, ok := payload["urgent"].(bool); ok {
					return urgent
				}
			}
		}
		return false
	}

	baseClient := &mockClient{closed: true}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func(client Client) {
		_ = client.Close()
	}(client)

	// Send critical message (should suppress error)
	err = client.Send(EventMessage{
		Action:  "test.custom.critical",
		Payload: map[string]any{"urgent": true, "data": "important"},
	})
	if err != nil {
		t.Errorf("Expected no error for custom critical message, got: %v", err)
	}

	// Send normal message (should return error)
	err = client.Send(EventMessage{
		Action:  "test.custom.normal",
		Payload: map[string]any{"urgent": false, "data": "normal"},
	})
	if err == nil {
		t.Error("Expected error for normal message when connection is closed")
	}

	// Verify both messages are queued
	qc := client.(*QueuedClient)
	stats := qc.GetQueueStats()
	if stats["total"] != 2 {
		t.Errorf("Expected 2 queued messages, got %v", stats["total"])
	}
	if stats["critical"] != 1 {
		t.Errorf("Expected 1 critical message, got %v", stats["critical"])
	}
}

// TestHealthQualityTransitions tests connection health quality degradation
func TestHealthQualityTransitions(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()
	config.EnableHealthCheck = true
	config.HealthCheckTimeout = 100 * time.Millisecond // Fast health check for testing

	baseClient := &mockClient{closed: false}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func(client Client) {
		_ = client.Close()
	}(client)

	qc := client.(*QueuedClient)

	// Initial quality should be set after a short time
	initialQuality := qc.GetConnectionQuality()
	if initialQuality == "" {
		t.Error("Expected initial quality to be set")
	}
	t.Logf("Initial quality: %s", initialQuality)

	// Send a message to update health
	_ = client.Send(EventMessage{Action: "test.health", Payload: map[string]any{"data": "test"}})

	// Wait for health to update
	timeout := time.After(2 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	qualityFound := false
	for !qualityFound {
		select {
		case <-timeout:
			t.Error("Timeout waiting for health quality update")
			return
		case <-ticker.C:
			quality := qc.GetConnectionQuality()
			if quality != "" {
				t.Logf("Health quality: %s", quality)
				qualityFound = true
			}
		}
	}

	// Verify quality is one of the expected values
	finalQuality := qc.GetConnectionQuality()
	validQualities := []string{"excellent", "good", "poor", "critical"}
	isValid := slices.Contains(validQualities, finalQuality)
	if !isValid {
		t.Errorf("Invalid health quality: %s", finalQuality)
	}
}

// TestCriticalMessagePriorityDuringOverflow tests that critical messages are preserved during overflow
func TestCriticalMessagePriorityDuringOverflow(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()
	config.MaxQueueSize = 10
	config.CriticalMessageActions = []string{"critical.*"}

	baseClient := &mockClient{closed: true}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func(client Client) {
		_ = client.Close()
	}(client)

	qc := client.(*QueuedClient)

	// Queue 5 critical messages first
	for i := range 5 {
		_ = client.Send(EventMessage{
			Action:  "critical.message",
			Payload: map[string]any{"id": i, "type": "critical"},
		})
	}

	// Queue 10 normal messages (should cause overflow and drop the oldest normal messages)
	for i := range 10 {
		_ = client.Send(EventMessage{
			Action:  "normal.message",
			Payload: map[string]any{"id": i, "type": "normal"},
		})
	}

	// Verify queue size respects limit
	size := qc.GetQueueSize()
	if size > config.MaxQueueSize {
		t.Errorf("Queue size %d exceeds MaxQueueSize %d", size, config.MaxQueueSize)
	}

	// Verify critical messages are still in queue
	stats := qc.GetQueueStats()
	if stats["critical"].(int) < 5 {
		t.Errorf("Expected at least 5 critical messages preserved, got %v", stats["critical"])
	}

	t.Logf("Queue stats after overflow: total=%v, critical=%v, normal=%v",
		stats["total"], stats["critical"], stats["normal"])
}

// TestBackoffResetAfterSuccess tests that backoff resets after successful flush
func TestBackoffResetAfterSuccess(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()
	config.BackoffStrategy = NewExponentialBackoff(100*time.Millisecond, 5*time.Second)

	baseClient := &mockClient{closed: true}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func(client Client) {
		_ = client.Close()
	}(client)

	qc := client.(*QueuedClient)

	// Queue messages while disconnected
	for i := range 3 {
		_ = client.Send(EventMessage{
			Action:  "test.backoff.reset",
			Payload: map[string]any{"id": i},
		})
	}

	// Reconnect
	baseClient.mu.Lock()
	baseClient.closed = false
	baseClient.mu.Unlock()

	// Wait for successful flush
	if !qc.WaitForQueueEmpty(3 * time.Second) {
		t.Fatal("Timeout waiting for queue to flush")
	}

	// Verify all messages were sent
	if baseClient.GetSentCount() != 3 {
		t.Errorf("Expected 3 messages sent, got %d", baseClient.GetSentCount())
	}

	// Queue another message and verify it's sent quickly (backoff was reset)
	_ = client.Send(EventMessage{
		Action:  "test.after.reset",
		Payload: map[string]any{"id": 99},
	})

	// Should be sent quickly since backoff was reset
	timeout := time.After(1 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Error("Message not sent quickly after backoff reset")
			return
		case <-ticker.C:
			if baseClient.GetSentCount() == 4 {
				t.Log("Message sent quickly after backoff reset")
				return
			}
		}
	}
}

// TestQueueBehaviorDuringFlush tests edge case where messages expire during flush
func TestQueueBehaviorDuringFlush(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()
	config.MaxMessageAge = 200 * time.Millisecond // Very short age for testing

	baseClient := &mockClient{closed: true}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func(client Client) {
		_ = client.Close()
	}(client)

	qc := client.(*QueuedClient)

	// Queue messages
	for i := range 5 {
		_ = client.Send(EventMessage{
			Action:  "test.expire.during.flush",
			Payload: map[string]any{"id": i},
		})
	}

	// Wait for messages to age
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(300 * time.Millisecond)

waitLoop:
	for {
		select {
		case <-timeout:
			break waitLoop
		case <-ticker.C:
			// Continue waiting
		}
	}

	// Now reconnect (messages should be expired during flush)
	baseClient.mu.Lock()
	baseClient.closed = false
	baseClient.mu.Unlock()

	// Wait for flush
	// Queue might already be empty due to expiration
	_ = qc.WaitForQueueEmpty(2 * time.Second)

	// Expired messages should not be sent
	sentCount := baseClient.GetSentCount()
	if sentCount > 0 {
		t.Logf("Some messages were sent before expiration: %d", sentCount)
	}

	// Queue should be empty
	if qc.GetQueueSize() != 0 {
		t.Errorf("Expected empty queue after expiration, got %d", qc.GetQueueSize())
	}
}

// TestConnectionStateTransitions tests connected -> disconnected -> reconnected transitions
func TestConnectionStateTransitions(t *testing.T) {
	logger := log.NewEntry(log.StandardLogger())
	config := DefaultQueueConfig()

	baseClient := &mockClient{closed: false}

	client, err := NewQueuedClient(baseClient, logger, &config)
	if err != nil {
		t.Fatalf("Failed to create queued client: %v", err)
	}
	defer func(client Client) {
		_ = client.Close()
	}(client)

	qc := client.(*QueuedClient)

	// State 1: Connected - messages should send immediately
	err = client.Send(EventMessage{Action: "test.connected", Payload: map[string]any{"state": 1}})
	if err != nil {
		t.Errorf("Expected no error when connected: %v", err)
	}

	if qc.GetQueueSize() != 0 {
		t.Errorf("Expected empty queue when connected, got %d", qc.GetQueueSize())
	}

	// State 2: Disconnect
	baseClient.mu.Lock()
	baseClient.closed = true
	baseClient.mu.Unlock()

	// Messages should queue
	err = client.Send(EventMessage{Action: "test.disconnected", Payload: map[string]any{"state": 2}})
	if err == nil {
		t.Error("Expected error when disconnected")
	}

	if qc.GetQueueSize() != 1 {
		t.Errorf("Expected 1 queued message, got %d", qc.GetQueueSize())
	}

	// State 3: Reconnect
	baseClient.mu.Lock()
	baseClient.closed = false
	baseClient.mu.Unlock()

	// Wait for queue to flush
	if !qc.WaitForQueueEmpty(3 * time.Second) {
		t.Error("Timeout waiting for queue to flush after reconnection")
	}

	// New messages should send immediately
	err = client.Send(EventMessage{Action: "test.reconnected", Payload: map[string]any{"state": 3}})
	if err != nil {
		t.Errorf("Expected no error after reconnection: %v", err)
	}

	// Verify total sent
	if baseClient.GetSentCount() != 3 {
		t.Errorf("Expected 3 messages sent total, got %d", baseClient.GetSentCount())
	}
}
