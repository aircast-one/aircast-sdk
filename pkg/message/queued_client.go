package message

import (
	"context"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// QueuedMessage represents a message waiting to be sent
type QueuedMessage struct {
	Type      string     `json:"type"`
	Message   any        `json:"message"`
	ChannelID *ChannelID `json:"channel_id,omitempty"`
	Timestamp time.Time  `json:"timestamp"`
	Retries   int        `json:"retries"`
	Critical  bool       `json:"critical"`
}

// QueueConfig configures the message queue behavior
type QueueConfig struct {
	MaxQueueSize       int                // Maximum number of messages to queue (default: 100)
	MaxMessageAge      time.Duration      // Maximum age of queued messages (default: 30s)
	MaxCriticalAge     time.Duration      // Maximum age for critical messages (default: 60s)
	FlushInterval      time.Duration      // How often to try flushing the queue (default: 1s)
	MaxRetries         int                // Maximum retries for normal messages (default: 3)
	MaxCriticalRetries int                // Maximum retries for critical messages (default: 10)
	Source             MessageSource      // Message source (default: SystemDevice)
	IsCriticalMessage  func(msg any) bool // Optional function to determine if a message is critical
}

// DefaultQueueConfig returns sensible defaults
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		MaxQueueSize:       100,
		MaxMessageAge:      30 * time.Second,
		MaxCriticalAge:     60 * time.Second,
		FlushInterval:      1 * time.Second,
		MaxRetries:         3,
		MaxCriticalRetries: 10,
		Source:             SystemDevice,
	}
}

// QueuedClient wraps a Client with message queuing capabilities
type QueuedClient struct {
	client Client
	logger *log.Entry
	config QueueConfig
	source MessageSource // Store source for creating messages

	// Message queue for handling disconnections
	queue      []QueuedMessage
	queueMutex sync.Mutex

	// Connection state tracking
	lastConnected bool
	stateMutex    sync.RWMutex

	// Control channels
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewQueuedClient creates a new client with message queuing
func NewQueuedClient(client Client, logger *log.Entry, config *QueueConfig) Client {
	if config == nil {
		cfg := DefaultQueueConfig()
		config = &cfg
	}

	// Use source from config
	source := config.Source
	if source == "" {
		source = SystemDevice
	}

	ctx, cancel := context.WithCancel(context.Background())

	qc := &QueuedClient{
		client:        client,
		logger:        logger.WithField("component", "QueuedClient"),
		config:        *config,
		source:        source,
		queue:         make([]QueuedMessage, 0, config.MaxQueueSize),
		lastConnected: !client.IsClosed(),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Start the queue processor
	qc.wg.Add(1)
	go qc.processQueue()

	return qc
}

// processQueue periodically attempts to send queued messages
func (qc *QueuedClient) processQueue() {
	defer qc.wg.Done()

	ticker := time.NewTicker(qc.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-qc.ctx.Done():
			return
		case <-ticker.C:
			// Check connection state change
			connected := !qc.client.IsClosed()
			qc.stateMutex.Lock()
			wasConnected := qc.lastConnected
			qc.lastConnected = connected
			qc.stateMutex.Unlock()

			// If we just reconnected, flush immediately
			if connected && !wasConnected {
				qc.logger.Info("Connection restored, flushing message queue")
				qc.flushQueue()
			} else if connected {
				// Regular flush attempt
				qc.flushQueue()
			}
		}
	}
}

// flushQueue attempts to send all queued messages
func (qc *QueuedClient) flushQueue() {
	qc.queueMutex.Lock()
	defer qc.queueMutex.Unlock()

	if len(qc.queue) == 0 {
		return
	}

	// Check if underlying client is connected
	if qc.client.IsClosed() {
		return
	}

	now := time.Now()
	retained := make([]QueuedMessage, 0)
	sent := 0
	expired := 0

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

		// Try to send based on message type
		var err error
		switch msg.Type {
		case "event":
			if eventMsg, ok := msg.Message.(EventMessage); ok {
				err = qc.client.Send(eventMsg, msg.ChannelID)
			}
		case "response":
			if respMsg, ok := msg.Message.(ResponseMessage); ok {
				err = qc.client.Send(respMsg, msg.ChannelID)
			}
		case "request":
			if reqMsg, ok := msg.Message.(RequestMessage); ok {
				err = qc.client.Send(reqMsg, msg.ChannelID)
			}
		default:
			err = qc.client.Send(msg.Message, msg.ChannelID)
		}

		if err != nil {
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
			"expired":   expired,
			"remaining": len(retained),
		}).Info("Queue flush completed")
	}
}

// queueMessage adds a message to the queue
func (qc *QueuedClient) queueMessage(msgType string, message any, channelID *ChannelID, critical bool) {
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

	// Add to queue
	qc.queue = append(qc.queue, QueuedMessage{
		Type:      msgType,
		Message:   message,
		ChannelID: channelID,
		Timestamp: time.Now(),
		Retries:   0,
		Critical:  critical,
	})

	qc.logger.WithFields(log.Fields{
		"type":       msgType,
		"queue_size": len(qc.queue),
		"critical":   critical,
	}).Debug("Message queued")
}

// isCriticalMessage determines if a message is critical using the configured function
func (qc *QueuedClient) isCriticalMessage(msg any) bool {
	if qc.config.IsCriticalMessage != nil {
		return qc.config.IsCriticalMessage(msg)
	}
	// Default: no messages are critical
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
func (qc *QueuedClient) Send(msg any, sessionId *ChannelID) error {
	// Try to send immediately
	err := qc.client.Send(msg, sessionId)

	if err != nil {
		// Check if it's a connection error
		errStr := err.Error()
		if qc.client.IsClosed() ||
			strings.Contains(errStr, "connection is closed") ||
			strings.Contains(errStr, "connection lost") {

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
			qc.queueMessage(msgType, msg, sessionId, critical)

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

// SendEventToChannel sends an event, queuing it if the connection is down
func (qc *QueuedClient) SendEventToChannel(action MessageAction, payload any, sessionID ChannelID) error {
	msg := EventMessage{
		Action:    action,
		Payload:   payload,
		Source:    qc.source,
		ChannelID: sessionID,
	}
	return qc.Send(msg, &sessionID)
}

// Delegate all other methods to the underlying client

func (qc *QueuedClient) Listen(ctx context.Context) error {
	return qc.client.Listen(ctx)
}

func (qc *QueuedClient) SendMessageToChannel(id ChannelID, msg any) error {
	return qc.Send(msg, &id)
}

func (qc *QueuedClient) SendBroadcastMessage(msg any) error {
	return qc.Send(msg, nil)
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
		qc.logger.WithField("remaining", remaining).Warn("Closing with messages still queued")
	}

	return qc.client.Close()
}

func (qc *QueuedClient) IsClosed() bool {
	return qc.client.IsClosed()
}

func (qc *QueuedClient) ReadMessage() <-chan any {
	return qc.client.ReadMessage()
}

func (qc *QueuedClient) SendResponse(req *RequestMessage, payload any) error {
	msg := ResponseMessage{
		Action:    req.Action,
		Payload:   payload,
		Source:    qc.source,
		ChannelID: req.ChannelID,
		ReplyTo:   req.RequestID,
	}
	return qc.Send(msg, &req.ChannelID)
}

func (qc *QueuedClient) SendErrorToChannel(req *RequestMessage, errResponse ErrorResponse) error {
	msg := ErrorMessage{
		Action:    req.Action,
		Source:    qc.source,
		ChannelID: req.ChannelID,
		Error:     errResponse,
		ReplyTo:   req.RequestID,
	}
	return qc.Send(msg, &req.ChannelID)
}

// GetQueueSize returns the current number of queued messages (for monitoring)
func (qc *QueuedClient) GetQueueSize() int {
	qc.queueMutex.Lock()
	defer qc.queueMutex.Unlock()
	return len(qc.queue)
}

// GetQueueStats returns statistics about the queue
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

	stats := map[string]any{
		"total":    len(qc.queue),
		"critical": critical,
		"normal":   normal,
	}

	if !oldest.IsZero() {
		stats["oldest_age"] = time.Since(oldest).String()
	}

	return stats
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
