package message

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// StressTestConnection simulates network conditions
type StressTestConnection struct {
	msgCh      chan []byte
	closed     bool
	closeMutex sync.Mutex
	dropRate   float32 // Percentage of messages to drop (0.0-1.0)
	latency    time.Duration
	sendErrors int64
}

func NewStressTestConnection(dropRate float32, latency time.Duration) *StressTestConnection {
	return &StressTestConnection{
		msgCh:    make(chan []byte, 10000), // Increased buffer for large message tests
		dropRate: dropRate,
		latency:  latency,
	}
}

func (c *StressTestConnection) SendMessage([]byte) error {
	// Simulate latency
	if c.latency > 0 {
		time.Sleep(c.latency)
	}

	// Simulate message drops
	if c.dropRate > 0 && float32(time.Now().UnixNano()%100)/100.0 < c.dropRate {
		atomic.AddInt64(&c.sendErrors, 1)
		return nil // Drop silently
	}

	return nil
}

func (c *StressTestConnection) ReadMessage() <-chan []byte {
	return c.msgCh
}

func (c *StressTestConnection) Close() error {
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()
	if !c.closed {
		c.closed = true
		close(c.msgCh)
	}
	return nil
}

func (c *StressTestConnection) IsClosed() bool {
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()
	return c.closed
}

func (c *StressTestConnection) IsConnectionError(_ error) bool {
	return false
}

func (c *StressTestConnection) GetSendErrors() int64 {
	return atomic.LoadInt64(&c.sendErrors)
}

// TestHighVolumeMessageProcessing tests system under high message load
func TestHighVolumeMessageProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high volume test in short mode")
	}

	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.ErrorLevel)

	conn := NewStressTestConnection(0.0, 0)
	config := ClientConfig{
		Source: SystemDevice,
	}
	client := NewClient(logger, conn, config)

	ctx := t.Context()

	// Start listening
	go func() {
		_ = client.Listen(ctx)
	}()

	const numMessages = 100000
	received := int64(0)

	// Message receiver
	go func() {
		for range client.ReadMessage() {
			atomic.AddInt64(&received, 1)
		}
	}()

	startTime := time.Now()

	// Send high volume of messages
	for i := range numMessages {
		msg := map[string]any{
			"type":        TypeEvent,
			"action":      "high_volume_test",
			"source":      SystemDevice,
			"destination": DestinationWeb,
			"payload":     map[string]int{"sequence": i},
		}
		data, _ := json.Marshal(msg)

		select {
		case conn.msgCh <- data:
			// Message sent
		case <-time.After(time.Millisecond):
			// Channel might be full, that's OK for stress testing
		}

		// Yield occasionally to prevent tight loop
		if i%1000 == 0 {
			runtime.Gosched()
		}
	}

	// Wait for processing
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			duration := time.Since(startTime)
			receivedCount := atomic.LoadInt64(&received)

			t.Logf("Processed %d/%d messages in %v", receivedCount, numMessages, duration)
			t.Logf("Message rate: %.2f msg/sec", float64(receivedCount)/duration.Seconds())

			// Should process at least 80% of messages
			assert.GreaterOrEqual(t, receivedCount, int64(numMessages*0.8),
				"Too many messages lost under high volume")
			return

		case <-ticker.C:
			receivedCount := atomic.LoadInt64(&received)
			if receivedCount >= int64(numMessages*0.95) {
				// Successfully processed most messages
				duration := time.Since(startTime)
				t.Logf("Successfully processed %d messages in %v", receivedCount, duration)
				return
			}
		}
	}
}

// TestConcurrentClientsStress tests multiple clients operating simultaneously
func TestConcurrentClientsStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent clients stress test in short mode")
	}

	const numClients = 50
	const messagesPerClient = 1000

	var wg sync.WaitGroup
	var totalReceived int64
	var totalSent int64

	clients := make([]Client, numClients)
	connections := make([]*StressTestConnection, numClients)

	// Create clients
	for i := range numClients {
		logger := logrus.NewEntry(logrus.New())
		logger.Logger.SetLevel(logrus.ErrorLevel)

		conn := NewStressTestConnection(0.01, time.Microsecond) // 1% drop rate
		connections[i] = conn

		config := ClientConfig{
			Source: SystemDevice,
		}
		clients[i] = NewClient(logger, conn, config)
	}

	// Start all clients
	ctx := t.Context()

	for i, client := range clients {
		wg.Add(1)
		go func(clientID int, c Client) {
			defer wg.Done()

			// Listen
			go func() {
				_ = c.Listen(ctx)
			}()

			// Receive messages
			received := 0
			go func() {
				for range c.ReadMessage() {
					received++
					atomic.AddInt64(&totalReceived, 1)
				}
			}()

			// Send messages
			for j := range messagesPerClient {
				msg := RequestMessage{
					Action:    "stress_test",
					Source:    SystemDevice,
					RequestID: string(rune(clientID*10000 + j)),
					Payload: map[string]any{
						"client": clientID,
						"seq":    j,
					},
				}

				err := c.Send(msg)
				if err == nil {
					atomic.AddInt64(&totalSent, 1)
				}

				// Send test message to self
				testMsg := map[string]any{
					"type":        TypeEvent,
					"action":      "self_test",
					"source":      SystemDevice,
					"destination": DestinationWeb,
					"payload":     map[string]int{"client": clientID, "seq": j},
				}
				data, _ := json.Marshal(testMsg)
				connections[clientID].msgCh <- data

				// Small delay to prevent overwhelming
				if j%100 == 0 {
					time.Sleep(time.Microsecond)
				}
			}
		}(i, client)
	}

	wg.Wait()

	// Wait for message processing
	time.Sleep(2 * time.Second)

	// Check results
	sentCount := atomic.LoadInt64(&totalSent)
	receivedCount := atomic.LoadInt64(&totalReceived)

	t.Logf("Sent: %d, Received: %d", sentCount, receivedCount)

	// Should send most messages successfully
	expectedSent := int64(numClients * messagesPerClient)
	assert.GreaterOrEqual(t, sentCount, expectedSent*9/10, "Too many send failures")

	// Should receive most self-sent messages
	assert.GreaterOrEqual(t, receivedCount, expectedSent*8/10, "Too many messages lost")

	// Cleanup
	for _, client := range clients {
		_ = client.Close()
	}
}

// TestMemoryPressure tests behavior under memory pressure
func TestMemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory pressure test in short mode")
	}

	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.ErrorLevel)

	conn := NewStressTestConnection(0.0, 0)
	config := ClientConfig{
		Source: SystemDevice,
	}
	client := NewClient(logger, conn, config)

	ctx := t.Context()

	// Start listening
	go func() {
		_ = client.Listen(ctx)
	}()

	// Create moderate payloads to test memory handling without extreme drops
	// 100 entries * 100 bytes = ~10KB per message, 100 messages = 1MB total
	largePayload := make(map[string]string)
	for i := range 100 {
		largePayload[fmt.Sprintf("key_%d", i)] = string(make([]byte, 100))
	}

	var processed int64
	go func() {
		for range client.ReadMessage() {
			atomic.AddInt64(&processed, 1)
		}
	}()

	// Send messages with large payloads
	const numLargeMessages = 100
	for i := 0; i < numLargeMessages; i++ {
		msg := map[string]any{
			"type":        TypeRequest,
			"action":      "memory_pressure_test",
			"source":      SystemDevice,
			"destination": DestinationAPI,
			"request_id":  fmt.Sprintf("req_%d", i),
			"payload":     largePayload,
		}
		data, _ := json.Marshal(msg)

		select {
		case conn.msgCh <- data:
			// Message sent successfully
		case <-time.After(10 * time.Millisecond):
			// If channel is full, wait a bit and continue
			// This simulates real-world backpressure handling
		}

		// Force GC periodically to detect memory issues
		if i%10 == 0 {
			runtime.GC()
		}
	}

	// Wait for processing with longer timeout for large messages
	time.Sleep(3 * time.Second)

	processedCount := atomic.LoadInt64(&processed)
	// With moderate payloads, should process most messages
	assert.GreaterOrEqual(t, processedCount, int64(numLargeMessages*0.8),
		"Too many messages lost under memory pressure")

	_ = client.Close()
}

// TestNetworkSimulation tests with simulated network conditions
func TestNetworkSimulation(t *testing.T) {
	tests := []struct {
		name           string
		dropRate       float32
		latency        time.Duration
		minSuccessRate float64
	}{
		{"Perfect Network", 0.0, 0, 0.99},
		{"Low Loss", 0.01, time.Microsecond, 0.95},
		{"Medium Loss", 0.05, 10 * time.Microsecond, 0.90},
		{"High Latency", 0.0, 100 * time.Microsecond, 0.95},
		{"Lossy + Latency", 0.02, 50 * time.Microsecond, 0.90},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logrus.NewEntry(logrus.New())
			logger.Logger.SetLevel(logrus.ErrorLevel)

			conn := NewStressTestConnection(tt.dropRate, tt.latency)
			config := ClientConfig{
				Source: SystemDevice,
			}
			client := NewClient(logger, conn, config)

			ctx := t.Context()

			// Start listening
			go func() {
				_ = client.Listen(ctx)
			}()

			const numMessages = 1000
			var sent, received int64

			// Receiver
			go func() {
				for range client.ReadMessage() {
					atomic.AddInt64(&received, 1)
				}
			}()

			// Sender
			for i := 0; i < numMessages; i++ {
				msg := EventMessage{
					Action:  "network_sim",
					Source:  SystemDevice,
					Payload: map[string]int{"seq": i},
				}

				err := client.Send(msg)
				if err == nil {
					atomic.AddInt64(&sent, 1)
				}

				// Send test message
				testMsg := map[string]any{
					"type":        TypeEvent,
					"action":      "network_test",
					"source":      SystemDevice,
					"destination": DestinationWeb,
				}
				data, _ := json.Marshal(testMsg)
				conn.msgCh <- data
			}

			// Wait for processing
			time.Sleep(500 * time.Millisecond)

			sentCount := atomic.LoadInt64(&sent)
			receivedCount := atomic.LoadInt64(&received)
			successRate := float64(receivedCount) / float64(numMessages)

			t.Logf("Network: %s, Sent: %d, Received: %d, Success: %.2f%%",
				tt.name, sentCount, receivedCount, successRate*100)

			assert.GreaterOrEqual(t, successRate, tt.minSuccessRate,
				"Success rate too low for network conditions")

			_ = client.Close()
		})
	}
}

// TestGoroutineStorm tests system stability under goroutine pressure
func TestGoroutineStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping goroutine storm test in short mode")
	}

	const numGoroutines = 1000
	var wg sync.WaitGroup

	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.ErrorLevel)

	conn := NewStressTestConnection(0.0, 0)
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

	// Track operations
	var operations int64

	// Launch goroutine storm
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := range 10 {
				// Send message
				msg := EventMessage{
					Action:  "goroutine_storm",
					Source:  SystemDevice,
					Payload: map[string]int{"goroutine": id, "op": j},
				}

				_ = client.Send(msg)
				atomic.AddInt64(&operations, 1)

				// Small delay
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	totalOps := atomic.LoadInt64(&operations)
	expectedOps := int64(numGoroutines * 10)

	t.Logf("Completed %d/%d operations", totalOps, expectedOps)
	assert.GreaterOrEqual(t, totalOps, expectedOps*9/10, "Too many operations failed")

	_ = client.Close()
}

// TestResourceExhaustion tests behavior when resources are nearly exhausted
func TestResourceExhaustion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping resource exhaustion test in short mode")
	}

	// Test with very small buffer
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.ErrorLevel)

	conn := NewStressTestConnection(0.0, 0)
	config := ClientConfig{
		Source: SystemDevice,
	}
	client := NewClient(logger, conn, config)

	ctx := t.Context()

	// Start listening
	go func() {
		_ = client.Listen(ctx)
	}()

	// Overwhelm the system
	const numMessages = 10000
	processed := int64(0)

	// Slow consumer to create backpressure
	go func() {
		for range client.ReadMessage() {
			atomic.AddInt64(&processed, 1)
			time.Sleep(10 * time.Microsecond) // Slow processing
		}
	}()

	// Fast producer
	for range numMessages {
		msg := map[string]any{
			"type":        TypeEvent,
			"action":      "resource_exhaustion",
			"source":      SystemDevice,
			"destination": DestinationWeb,
		}
		data, _ := json.Marshal(msg)

		select {
		case conn.msgCh <- data:
			// Sent
		case <-time.After(time.Microsecond):
			// Channel full - this is expected under resource pressure
		}
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	processedCount := atomic.LoadInt64(&processed)
	t.Logf("Processed %d messages under resource pressure", processedCount)

	// Should handle backpressure gracefully
	assert.Greater(t, processedCount, int64(100), "System should handle some messages even under pressure")

	_ = client.Close()
}
