package message

import (
	"context"
	"encoding/json"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestGoroutineLeaks verifies that no goroutines are leaked
func TestGoroutineLeaks(t *testing.T) {
	// Get initial goroutine count
	runtime.GC()
	initialGoroutines := runtime.NumGoroutine()

	t.Run("Listen goroutine cleanup", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			logger := logrus.NewEntry(logrus.New())
			logger.Logger.SetLevel(logrus.ErrorLevel)
			conn := NewMockConnection()
			conn.On("ReadMessage").Return()
			conn.On("Close").Return(nil)

			config := ClientConfig{
				Source: SystemDevice,
			}
			client := NewClient(logger, conn, config)

			ctx, cancel := context.WithCancel(context.Background())

			// Start listening
			done := make(chan error)
			go func() {
				done <- client.Listen(ctx)
			}()

			// Let it run briefly
			time.Sleep(10 * time.Millisecond)

			// Cancel and wait for cleanup
			cancel()
			select {
			case <-done:
				// Good, Listen returned
			case <-time.After(time.Second):
				t.Fatal("Listen did not return after context cancellation")
			}

			_ = client.Close()
		}

		// Allow goroutines to fully terminate
		time.Sleep(100 * time.Millisecond)
		runtime.GC()

		// Check goroutine count
		currentGoroutines := runtime.NumGoroutine()
		if currentGoroutines > initialGoroutines+2 { // Allow small variance
			t.Errorf("Goroutine leak detected: initial=%d, current=%d", initialGoroutines, currentGoroutines)
		}
	})

	t.Run("Multiple client cleanup", func(t *testing.T) {
		var clients []Client
		for i := 0; i < 20; i++ {
			logger := logrus.NewEntry(logrus.New())
			logger.Logger.SetLevel(logrus.ErrorLevel)
			conn := NewMockConnection()
			conn.On("Close").Return(nil)

			config := ClientConfig{
				Source: SystemDevice,
			}
			client := NewClient(logger, conn, config)
			clients = append(clients, client)
		}

		// Close all clients
		for _, client := range clients {
			_ = client.Close()
		}

		// Allow cleanup
		time.Sleep(100 * time.Millisecond)
		runtime.GC()

		// Check goroutine count
		currentGoroutines := runtime.NumGoroutine()
		if currentGoroutines > initialGoroutines+2 {
			t.Errorf("Goroutine leak after closing clients: initial=%d, current=%d", initialGoroutines, currentGoroutines)
		}
	})
}

// TestMessageReliability ensures no messages are lost or stuck
func TestMessageReliability(t *testing.T) {
	t.Run("No message loss under load", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		logger.Logger.SetLevel(logrus.ErrorLevel)
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

		// Track messages
		const numMessages = 1000
		received := make(map[string]bool)
		var mu sync.Mutex

		// Start receiver
		go func() {
			for msg := range client.ReadMessage() {
				if req, ok := msg.(RequestMessage); ok {
					mu.Lock()
					received[req.RequestID] = true
					mu.Unlock()
				}
			}
		}()

		// Send messages
		for i := 0; i < numMessages; i++ {
			msg := map[string]any{
				"type":        TypeRequest,
				"action":      "test_action",
				"source":      SystemDevice,
				"destination": DestinationAPI,
				"request_id":  string(rune(i)),
			}
			data, _ := json.Marshal(msg)
			conn.msgCh <- data
		}

		// Wait for processing
		time.Sleep(500 * time.Millisecond)

		// Verify all messages received
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, numMessages, len(received), "Some messages were lost")
	})

	t.Run("Channel buffer overflow handling", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		logger.Logger.SetLevel(logrus.ErrorLevel)
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

		// Send more messages than buffer size (10,000)
		const numMessages = 12000
		for i := 0; i < numMessages; i++ {
			msg := map[string]any{
				"type":        TypeRequest,
				"action":      "overflow_test",
				"source":      SystemDevice,
				"destination": DestinationAPI,
				"request_id":  string(rune(i)),
			}
			data, _ := json.Marshal(msg)

			select {
			case conn.msgCh <- data:
				// Message sent
			default:
				// Channel full, this simulates backpressure
			}
		}

		// Drain messages
		received := 0
		timeout := time.After(time.Second)
		for {
			select {
			case <-client.ReadMessage():
				received++
			case <-timeout:
				// Check we received at least most of the buffer size (10,000)
				assert.GreaterOrEqual(t, received, 9500, "Should receive most messages despite overflow")
				return
			}
		}
	})

	t.Run("No stuck messages", func(t *testing.T) {
		logger := logrus.NewEntry(logrus.New())
		logger.Logger.SetLevel(logrus.ErrorLevel)
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

		// Send a message
		msg := map[string]any{
			"type":        TypeRequest,
			"action":      "test_stuck",
			"source":      SystemDevice,
			"destination": DestinationAPI,
			"request_id":  "req-stuck",
		}
		data, _ := json.Marshal(msg)
		conn.msgCh <- data

		// Message should be received within reasonable time
		select {
		case receivedMsg := <-client.ReadMessage():
			req, ok := receivedMsg.(RequestMessage)
			require.True(t, ok)
			assert.Equal(t, "req-stuck", req.RequestID)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Message stuck in pipeline")
		}
	})
}

// TestConcurrentSendReliability ensures thread safety
func TestConcurrentSendReliability(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.ErrorLevel)

	// Track sent messages
	var sentCount int64
	var errorCount int64

	conn := &MockConnection{}
	conn.On("SendMessage", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		atomic.AddInt64(&sentCount, 1)
	})
	conn.On("IsClosed").Return(false)

	config := ClientConfig{
		Source: SystemDevice,
	}
	client := NewClient(logger, conn, config)

	// Launch concurrent senders
	const numGoroutines = 100
	const messagesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := RequestMessage{
					Action:    "concurrent_test",
					Source:    SystemDevice,
					RequestID: string(rune(id*1000 + j)),
					Payload:   map[string]int{"id": id, "seq": j},
				}

				err := client.Send(msg, nil)
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages were sent
	expectedMessages := int64(numGoroutines * messagesPerGoroutine)
	assert.Equal(t, expectedMessages, atomic.LoadInt64(&sentCount), "Not all messages were sent")
	assert.Equal(t, int64(0), atomic.LoadInt64(&errorCount), "Some sends failed")
}

// TestClientCloseRaceCondition tests for race conditions during close
func TestClientCloseRaceCondition(t *testing.T) {
	for i := 0; i < 100; i++ {
		logger := logrus.NewEntry(logrus.New())
		logger.Logger.SetLevel(logrus.ErrorLevel)
		conn := NewMockConnection()
		conn.On("SendMessage", mock.Anything).Return(nil).Maybe()
		conn.On("Close").Return(nil)
		conn.On("IsClosed").Return(false).Maybe()
		conn.On("ReadMessage").Return()

		config := ClientConfig{
			Source: SystemDevice,
		}
		client := NewClient(logger, conn, config)

		ctx, cancel := context.WithCancel(context.Background())

		// Start listening
		go func() {
			_ = client.Listen(ctx)
		}()

		// Concurrent operations
		var wg sync.WaitGroup

		// Sender
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				msg := EventMessage{
					Action:  "race_test",
					Source:  SystemDevice,
					Payload: map[string]string{"test": "data"},
				}
				_ = client.Send(msg, nil)
				time.Sleep(time.Microsecond)
			}
		}()

		// Closer
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(5 * time.Microsecond)
			_ = client.Close()
		}()

		// Context canceller
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(3 * time.Microsecond)
			cancel()
		}()

		wg.Wait()

		// Verify client is closed
		assert.True(t, client.IsClosed())
	}
}

// TestMessageChannelDeadlock tests for potential deadlocks
func TestMessageChannelDeadlock(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.ErrorLevel)
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

	// Fill the message channel
	for i := 0; i < 600; i++ { // More than buffer size
		msg := map[string]any{
			"type":        TypeRequest,
			"action":      "deadlock_test",
			"source":      SystemDevice,
			"destination": DestinationAPI,
			"request_id":  string(rune(i)),
		}
		data, _ := json.Marshal(msg)

		select {
		case conn.msgCh <- data:
			// Sent
		case <-time.After(10 * time.Millisecond):
			// Timeout is OK, we're testing deadlock prevention
			// Use labeled continue to exit select, not break
			continue
		}
	}

	// Try to close - should not deadlock
	done := make(chan bool)
	go func() {
		_ = client.Close()
		done <- true
	}()

	select {
	case <-done:
		// Good, close completed
	case <-time.After(time.Second):
		t.Fatal("Close operation deadlocked")
	}
}

// TestMemoryLeaks checks for memory leaks during continuous operation
func TestMemoryLeaks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.ErrorLevel)
	conn := NewMockConnection()
	conn.On("SendMessage", mock.Anything).Return(nil)
	conn.On("IsClosed").Return(false)
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

	// Get initial memory stats
	var initialMem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&initialMem)

	// Run continuous operations
	const iterations = 10000
	for i := 0; i < iterations; i++ {
		// Send message
		msg := RequestMessage{
			Action:    "memory_test",
			Source:    SystemDevice,
			RequestID: string(rune(i)),
			Payload: map[string]any{
				"index": i,
				"data":  "test data that should not leak",
			},
		}
		_ = client.Send(msg, nil)

		// Receive message
		testMsg := map[string]any{
			"type":        TypeRequest,
			"action":      "memory_test",
			"source":      SystemDevice,
			"destination": DestinationAPI,
			"request_id":  string(rune(i)),
		}
		data, _ := json.Marshal(testMsg)
		conn.msgCh <- data

		select {
		case <-client.ReadMessage():
			// Message received
		case <-time.After(10 * time.Millisecond):
			// Timeout, continue
		}

		// Periodic GC to detect leaks
		if i%1000 == 0 {
			runtime.GC()
		}
	}

	// Final memory check
	runtime.GC()
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	// Calculate memory growth
	memGrowth := finalMem.HeapAlloc - initialMem.HeapAlloc
	memGrowthMB := float64(memGrowth) / (1024 * 1024)

	// Allow up to 10MB growth for legitimate caching/buffering
	if memGrowthMB > 10 {
		t.Errorf("Excessive memory growth detected: %.2f MB", memGrowthMB)
	}

	_ = client.Close()
}

// BenchmarkMessageReliability benchmarks message processing reliability
func BenchmarkMessageReliability(b *testing.B) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.ErrorLevel)
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

	// Prepare message
	msg := map[string]any{
		"type":        TypeRequest,
		"action":      "bench_reliability",
		"source":      SystemDevice,
		"destination": DestinationAPI,
		"request_id":  "req-bench",
	}
	data, _ := json.Marshal(msg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Send message
		conn.msgCh <- data

		// Verify receipt
		select {
		case <-client.ReadMessage():
			// Success
		case <-time.After(time.Millisecond):
			b.Fatal("Message not received")
		}
	}
}
