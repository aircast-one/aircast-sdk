package message

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// Backoff attempt caps
const (
	maxFailedAttempts       = 10 // Cap failed flush attempts to avoid unbounded backoff
	maxDisconnectedAttempts = 5  // Cap disconnected backoff to keep retry interval reasonable
)

// QueuedMessage represents a message waiting to be sent
type QueuedMessage struct {
	Type      string    `json:"type"`
	Message   any       `json:"message"`
	Timestamp time.Time `json:"timestamp"`
	Retries   int       `json:"retries"`
	Critical  bool      `json:"critical"`
}

// QueueConfig configures the in-memory message queue behavior
type QueueConfig struct {
	// Queue behavior
	MaxQueueSize           int                // Maximum number of messages to queue (default: 1000)
	MaxMessageAge          time.Duration      // Maximum age of queued messages (default: 30s)
	MaxCriticalAge         time.Duration      // Maximum age for critical messages (default: 60s)
	MaxRetries             int                // Maximum retries for normal messages (default: 3)
	MaxCriticalRetries     int                // Maximum retries for critical messages (default: 10)
	Source                 MessageSource      // Message source (default: SystemDevice)
	CriticalMessageActions []string           // List of message actions to treat as critical (supports prefix matching with "*")
	IsCriticalMessage      func(msg any) bool // Optional function to determine if a message is critical (takes precedence over CriticalMessageActions)

	// Advanced features
	BackoffStrategy    BackoffStrategy // Backoff strategy (default: exponential 1s-30s)
	EnableHealthCheck  bool            // Enable connection health monitoring (default: true)
	HealthCheckTimeout time.Duration   // Ping timeout for health checks (default: 5s)
}

// DefaultQueueConfig returns sensible defaults for in-memory queue
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		MaxQueueSize:       1000,
		MaxMessageAge:      30 * time.Second,
		MaxCriticalAge:     60 * time.Second,
		MaxRetries:         3,
		MaxCriticalRetries: 10,
		Source:             SystemDevice,
		BackoffStrategy:    NewExponentialBackoff(1*time.Second, 30*time.Second),
		EnableHealthCheck:  true,
		HealthCheckTimeout: 5 * time.Second,
	}
}

// QueuedClient wraps a Client with in-memory message queuing, adaptive backoff, and health monitoring
type QueuedClient struct {
	client Client
	logger *log.Entry
	config QueueConfig
	source MessageSource

	// In-memory queue (not persisted across restarts)
	queue      []QueuedMessage
	queueMutex sync.Mutex

	// Connection state tracking with backoff
	lastConnected     bool
	consecutiveErrors int
	stateMutex        sync.RWMutex

	// Connection health metrics
	lastSuccessfulSend time.Time
	connectionQuality  string // "excellent", "good", "poor", "critical"
	healthMutex        sync.RWMutex

	// Control channels
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewQueuedClient creates a new client with in-memory message queuing, adaptive backoff, and health monitoring
func NewQueuedClient(client Client, logger *log.Entry, config *QueueConfig) (Client, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	// Ensure backoff strategy is set
	if config.BackoffStrategy == nil {
		config.BackoffStrategy = NewExponentialBackoff(1*time.Second, 30*time.Second)
	}

	// Use source from config
	source := config.Source
	if source == "" {
		source = SystemDevice
	}

	ctx, cancel := context.WithCancel(context.Background())

	qc := &QueuedClient{
		client:             client,
		logger:             logger.WithField("component", "QueuedClient"),
		config:             *config,
		source:             source,
		queue:              make([]QueuedMessage, 0, config.MaxQueueSize),
		lastConnected:      !client.IsClosed(),
		consecutiveErrors:  0,
		lastSuccessfulSend: time.Now(),
		connectionQuality:  "good",
		ctx:                ctx,
		cancel:             cancel,
	}

	// Start the adaptive queue processor
	qc.wg.Add(1)
	go qc.processQueueAdaptive()

	// Start health monitor if enabled
	if config.EnableHealthCheck {
		qc.wg.Add(1)
		go qc.monitorConnectionHealth()
	}

	return qc, nil
}

// processQueueAdaptive uses adaptive backoff based on connection health
func (qc *QueuedClient) processQueueAdaptive() {
	defer qc.wg.Done()

	attempt := 0
	var timer *time.Timer

	for {
		// Calculate next flush delay using backoff
		delay := qc.config.BackoffStrategy.NextDelay(attempt)

		// When connected, cap delay to MaxMessageAge so queued messages
		// are flushed before they expire. Without this cap, high backoff
		// (e.g. 30s) causes all messages to expire (MaxMessageAge=10s)
		// before they can be sent, creating a death spiral.
		if !qc.client.IsClosed() && delay > qc.config.MaxMessageAge {
			delay = qc.config.MaxMessageAge
		}

		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}

		select {
		case <-qc.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return

		case <-timer.C:
			// Check connection state change
			connected := !qc.client.IsClosed()
			qc.stateMutex.Lock()
			wasConnected := qc.lastConnected
			qc.lastConnected = connected
			qc.stateMutex.Unlock()

			// If we just reconnected, flush immediately and reset backoff
			if connected && !wasConnected {
				qc.logger.Info("Connection restored, flushing message queue")
				attempt = 0 // Reset backoff
				result := qc.flushQueue()
				if !result.ok {
					attempt++
				}
			} else if connected {
				// Regular flush attempt
				result := qc.flushQueue()
				if result.ok {
					// Queue is drained — reset backoff immediately so fresh
					// messages are flushed within MaxMessageAge and don't expire.
					if result.remaining == 0 {
						attempt = 0
					} else if attempt > 0 {
						attempt--
					}
				} else {
					// Failed flush, increase backoff
					attempt++
					if attempt > maxFailedAttempts {
						attempt = maxFailedAttempts
					}
				}
			} else {
				// Disconnected, moderate backoff
				if attempt < maxDisconnectedAttempts {
					attempt++
				}
			}

			// Log current backoff state
			if attempt > 0 {
				nextDelay := qc.config.BackoffStrategy.NextDelay(attempt)
				qc.logger.WithFields(log.Fields{
					"attempt":    attempt,
					"next_delay": nextDelay,
					"queue_size": qc.GetQueueSize(),
				}).Debug("Adaptive backoff applied")
			}
		}
	}
}

// flushResult contains the outcome of a flush attempt.
type flushResult struct {
	ok        bool // true if no send errors occurred
	remaining int  // number of messages still in queue after flush
}

// flushQueue attempts to send all queued messages.
// Returns whether the flush succeeded and how many messages remain.
func (qc *QueuedClient) flushQueue() flushResult {
	qc.queueMutex.Lock()
	defer qc.queueMutex.Unlock()

	if len(qc.queue) == 0 {
		return flushResult{ok: true, remaining: 0}
	}

	// Check if underlying client is connected
	if qc.client.IsClosed() {
		return flushResult{ok: false, remaining: len(qc.queue)}
	}

	now := time.Now()
	retained := make([]QueuedMessage, 0)
	sent := 0
	expired := 0
	errors := 0

	for _, msg := range qc.queue {
		// Check message age
		age := now.Sub(msg.Timestamp)
		maxAge := qc.config.MaxMessageAge
		if msg.Critical {
			maxAge = qc.config.MaxCriticalAge
		}

		if age > maxAge {
			expired++
			level := "Debug"
			if msg.Critical {
				level = "Warn"
			}
			qc.logWithLevel(level, "Dropping expired message", log.Fields{
				"age":      age,
				"critical": msg.Critical,
				"type":     msg.Type,
			})
			continue
		}

		// Try to send the message
		var err error
		switch msg.Type {
		case "event":
			if eventMsg, ok := msg.Message.(EventMessage); ok {
				err = qc.client.Send(eventMsg)
			}
		case "response":
			if respMsg, ok := msg.Message.(ResponseMessage); ok {
				err = qc.client.Send(respMsg)
			}
		case "request":
			if reqMsg, ok := msg.Message.(RequestMessage); ok {
				err = qc.client.Send(reqMsg)
			}
		default:
			err = qc.client.Send(msg.Message)
		}

		if err != nil {
			errors++
			msg.Retries++
			maxRetries := qc.config.MaxRetries
			if msg.Critical {
				maxRetries = qc.config.MaxCriticalRetries
			}

			if msg.Retries < maxRetries {
				retained = append(retained, msg)
			} else {
				qc.logger.WithFields(log.Fields{
					"type":    msg.Type,
					"retries": msg.Retries,
				}).Warn("Dropping message after max retries")
			}
		} else {
			sent++
			qc.updateConnectionHealth(true)
			qc.logger.WithFields(log.Fields{
				"type": msg.Type,
				"age":  age,
			}).Debug("Successfully sent queued message")
		}
	}

	qc.queue = retained

	if sent > 0 || expired > 0 {
		qc.logger.WithFields(log.Fields{
			"sent":      sent,
			"errors":    errors,
			"expired":   expired,
			"remaining": len(retained),
		}).Info("Queue flush completed")
	}

	// Update consecutive errors for backoff
	qc.stateMutex.Lock()
	if errors > 0 {
		qc.consecutiveErrors++
	} else {
		qc.consecutiveErrors = 0
	}
	qc.stateMutex.Unlock()

	return flushResult{ok: errors == 0, remaining: len(retained)}
}

// monitorConnectionHealth periodically checks connection health
func (qc *QueuedClient) monitorConnectionHealth() {
	defer qc.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-qc.ctx.Done():
			return
		case <-ticker.C:
			qc.checkConnectionHealth()
		}
	}
}

// checkConnectionHealth assesses connection quality based on recent activity
func (qc *QueuedClient) checkConnectionHealth() {
	qc.healthMutex.Lock()
	defer qc.healthMutex.Unlock()

	timeSinceLastSuccess := time.Since(qc.lastSuccessfulSend)

	var quality string
	switch {
	case timeSinceLastSuccess < 10*time.Second:
		quality = "excellent"
	case timeSinceLastSuccess < 30*time.Second:
		quality = "good"
	case timeSinceLastSuccess < 60*time.Second:
		quality = "poor"
	default:
		quality = "critical"
	}

	if quality != qc.connectionQuality {
		qc.logger.WithFields(log.Fields{
			"old_quality":          qc.connectionQuality,
			"new_quality":          quality,
			"time_since_last_send": timeSinceLastSuccess,
		}).Info("Connection quality changed")
		qc.connectionQuality = quality
	}
}

// updateConnectionHealth updates health metrics after send attempt
func (qc *QueuedClient) updateConnectionHealth(success bool) {
	qc.healthMutex.Lock()
	defer qc.healthMutex.Unlock()

	if success {
		qc.lastSuccessfulSend = time.Now()
	}
}

// queueMessage adds a message to both the local queue and Redis
func (qc *QueuedClient) queueMessage(msgType string, message any, critical bool) {
	qc.queueMutex.Lock()
	defer qc.queueMutex.Unlock()

	// Check if queue is full
	if len(qc.queue) >= qc.config.MaxQueueSize {
		// Remove oldest non-critical message
		removed := false
		for i, msg := range qc.queue {
			if !msg.Critical {
				qc.queue = append(qc.queue[:i], qc.queue[i+1:]...)
				qc.logger.Warn("Queue full, dropped oldest non-critical message")
				removed = true
				break
			}
		}

		// If still full (all critical), drop oldest anyway
		if !removed && len(qc.queue) >= qc.config.MaxQueueSize {
			qc.queue = qc.queue[1:]
			qc.logger.Warn("Queue full, dropped oldest message")
		}
	}

	// Create queued message
	queuedMsg := QueuedMessage{
		Type:      msgType,
		Message:   message,
		Timestamp: time.Now(),
		Retries:   0,
		Critical:  critical,
	}

	// Add to in-memory queue
	qc.queue = append(qc.queue, queuedMsg)

	qc.logger.WithFields(log.Fields{
		"type":       msgType,
		"queue_size": len(qc.queue),
		"critical":   critical,
	}).Debug("Message queued")
}

// isCriticalMessage determines if a message is critical using the configured function or list
func (qc *QueuedClient) isCriticalMessage(msg any) bool {
	// Function takes precedence over list
	if qc.config.IsCriticalMessage != nil {
		return qc.config.IsCriticalMessage(msg)
	}

	// Check against the configured list of critical actions
	if len(qc.config.CriticalMessageActions) > 0 {
		var action string
		switch m := msg.(type) {
		case EventMessage:
			action = m.Action
		case RequestMessage:
			action = m.Action
		case ResponseMessage:
			action = m.Action
		default:
			return false
		}

		for _, pattern := range qc.config.CriticalMessageActions {
			if matchActionPattern(action, pattern) {
				return true
			}
		}
	}

	// Default: no messages are critical
	return false
}

// matchActionPattern checks if an action matches a pattern (supports "*" for prefix matching)
func matchActionPattern(action, pattern string) bool {
	// Exact match
	if action == pattern {
		return true
	}

	// Prefix match with wildcard
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(action, prefix)
	}

	return false
}

// logWithLevel logs with the specified level
func (qc *QueuedClient) logWithLevel(level string, msg string, fields log.Fields) {
	entry := qc.logger.WithFields(fields)
	switch level {
	case "Debug":
		entry.Debug(msg)
	case "Info":
		entry.Info(msg)
	case "Warn":
		entry.Warn(msg)
	case "Error":
		entry.Error(msg)
	default:
		entry.Info(msg)
	}
}

// Send attempts to send a message, queuing it if the connection is down
func (qc *QueuedClient) Send(msg any) error {
	// Try to send immediately
	err := qc.client.Send(msg)

	if err != nil {
		// Check if it's a connection error (including websocket write failures)
		if qc.client.IsClosed() || qc.client.IsConnectionError(err) {

			// Determine message type
			msgType := "unknown"
			switch msg.(type) {
			case EventMessage:
				msgType = "event"
			case RequestMessage:
				msgType = "request"
			case ResponseMessage:
				msgType = "response"
			case ErrorMessage:
				msgType = "error"
			}

			// Queue the message
			critical := qc.isCriticalMessage(msg)
			qc.queueMessage(msgType, msg, critical)

			// Return nil for critical messages to prevent upstream errors
			if critical {
				qc.logger.WithFields(log.Fields{
					"type":     msgType,
					"critical": true,
				}).Info("Critical message queued, suppressing error")
				return nil
			}
		}
	}

	return err
}

// Delegate all other methods to the underlying client

func (qc *QueuedClient) Listen(ctx context.Context) error {
	return qc.client.Listen(ctx)
}

func (qc *QueuedClient) Close() error {
	qc.cancel()
	qc.wg.Wait()

	// Try to flush remaining messages one last time
	qc.flushQueue()

	// Log if we're closing with messages still queued
	qc.queueMutex.Lock()
	remaining := len(qc.queue)
	qc.queueMutex.Unlock()

	if remaining > 0 {
		qc.logger.WithField("remaining", remaining).Warn("Closing with messages still in queue (will be lost)")
	}

	return qc.client.Close()
}

func (qc *QueuedClient) IsClosed() bool {
	return qc.client.IsClosed()
}

func (qc *QueuedClient) ReadMessage() <-chan any {
	return qc.client.ReadMessage()
}

// GetSource returns the message source for this client
func (qc *QueuedClient) GetSource() MessageSource {
	return qc.source
}

// GetQueueSize returns the current number of queued messages (for monitoring)
func (qc *QueuedClient) GetQueueSize() int {
	qc.queueMutex.Lock()
	defer qc.queueMutex.Unlock()
	return len(qc.queue)
}

// GetQueueStats returns enhanced statistics about the queue including Redis metrics
func (qc *QueuedClient) GetQueueStats() map[string]any {
	qc.queueMutex.Lock()
	defer qc.queueMutex.Unlock()

	critical := 0
	normal := 0
	oldest := time.Time{}

	for _, msg := range qc.queue {
		if msg.Critical {
			critical++
		} else {
			normal++
		}
		if oldest.IsZero() || msg.Timestamp.Before(oldest) {
			oldest = msg.Timestamp
		}
	}

	qc.healthMutex.RLock()
	quality := qc.connectionQuality
	lastSend := qc.lastSuccessfulSend
	qc.healthMutex.RUnlock()

	qc.stateMutex.RLock()
	consErrors := qc.consecutiveErrors
	qc.stateMutex.RUnlock()

	stats := map[string]any{
		"total":                len(qc.queue),
		"critical":             critical,
		"normal":               normal,
		"connection_quality":   quality,
		"consecutive_errors":   consErrors,
		"last_successful_send": time.Since(lastSend).String(),
	}

	if !oldest.IsZero() {
		stats["oldest_age"] = time.Since(oldest).String()
	}

	return stats
}

// GetConnectionQuality returns the current connection quality assessment
func (qc *QueuedClient) GetConnectionQuality() string {
	qc.healthMutex.RLock()
	defer qc.healthMutex.RUnlock()
	return qc.connectionQuality
}

// FlushQueueSync synchronously flushes the queue (for testing)
func (qc *QueuedClient) FlushQueueSync() {
	qc.flushQueue()
}

// WaitForQueueEmpty waits for the queue to become empty or timeout
func (qc *QueuedClient) WaitForQueueEmpty(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		if qc.GetQueueSize() == 0 {
			return true
		}
		<-ticker.C
	}
	return false
}

// RegisterWill registers a Last Will message (delegates to underlying client)
func (qc *QueuedClient) RegisterWill(will WillMessage) error {
	return qc.client.RegisterWill(will)
}

// ClearWill clears the Last Will message (delegates to underlying client)
func (qc *QueuedClient) ClearWill() error {
	return qc.client.ClearWill()
}

// SendRawJSON sends pre-serialized JSON bytes directly (delegates to underlying client)
func (qc *QueuedClient) SendRawJSON(jsonBytes []byte) error {
	return qc.client.SendRawJSON(jsonBytes)
}

// IsConnectionError delegates to the underlying client's connection error detection.
func (qc *QueuedClient) IsConnectionError(err error) bool {
	return qc.client.IsConnectionError(err)
}
