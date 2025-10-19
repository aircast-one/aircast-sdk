package message

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

// Pool for reusing bytes.Buffer for JSON encoding
var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 512)) // Pre-allocate 512 bytes
	},
}

// Client represents a message client connected to a device or web client
type Client interface {
	// Listen starts listening for incoming messages
	Listen(ctx context.Context) error

	// Send sends a message
	Send(msg any) error

	// Close closes the client connection
	Close() error

	// IsClosed returns whether the client is closed
	IsClosed() bool

	// ReadMessage returns a channel of incoming parsed messages
	ReadMessage() <-chan any

	// GetSource returns the message source for this client
	GetSource() MessageSource

	// RegisterWill registers a Last Will message to be sent if connection closes unexpectedly
	RegisterWill(will WillMessage) error

	// ClearWill clears the Last Will message (use before graceful disconnect)
	ClearWill() error

	// SendRawJSON sends pre-serialized JSON bytes directly to the connection
	// This is useful for forwarding stored messages without re-serialization
	SendRawJSON(jsonBytes []byte) error
}

// Connection represents a WebSocket connection
type Connection interface {
	SendMessage(message []byte) error
	ReadMessage() <-chan []byte
	Close() error
	IsClosed() bool
}

type ClientConfig struct {
	Source MessageSource
}

// client implements the Client interface
type client struct {
	conn       Connection
	msgCh      chan GenericMessage
	logger     *log.Entry
	closed     bool
	closeMutex sync.Mutex
	closeOnce  sync.Once
	source     MessageSource
}

// NewClient creates a new message client
func NewClient(logger *log.Entry, conn Connection, config ClientConfig) Client {
	return &client{
		conn:       conn,
		msgCh:      make(chan GenericMessage, 10000), // Much larger buffer for high throughput
		logger:     logger.WithField("component", "message_client"),
		closed:     false,
		closeMutex: sync.Mutex{},
		closeOnce:  sync.Once{},
		source:     config.Source,
	}
}

// Listen starts listening for incoming websocket messages and parses them
func (c *client) Listen(ctx context.Context) error {
	// Set up a done channel for a synchronized exit.
	done := make(chan struct{})
	defer close(done)

	// Context cancellation handler.
	go func() {
		select {
		case <-ctx.Done():
			c.logger.Trace("Context canceled, stopping client")
			_ = c.Close()
		case <-done:
			// Listen function is done.
		}
	}()

	// Get the message channel once instead of calling ReadMessage() in the loop
	msgChan := c.conn.ReadMessage()

	// Process incoming messages until the connection is closed.
	for {
		select {

		case msgBytes, ok := <-msgChan:
			if !ok {
				if c.logger.Logger.IsLevelEnabled(log.TraceLevel) {
					c.logger.Trace("WebSocket message channel closed")
				}
				_ = c.Close()
				return nil
			}

			// Parse the raw message.
			msg, err := UnmarshalMessage(msgBytes)
			if err != nil {
				c.logger.WithError(err).Error("Failed to parse message")
				// Continue listening, even if a parse error occurs.
				continue
			}

			// Forward the message using safe send (prevents race on channel close)
			if c.safeSend(msg) {
				// Only log trace if enabled to reduce overhead
				if c.logger.Logger.IsLevelEnabled(log.TraceLevel) {
					c.logger.Trace("Message received and forwarded")
				}
			} else if !c.IsClosed() {
				c.logger.Warn("GenericMessage channel full, dropping message")
			}

		case <-ctx.Done():
			if c.logger.Logger.IsLevelEnabled(log.TraceLevel) {
				c.logger.Trace("Context canceled in message loop")
			}
			_ = c.Close()
			return nil
		}
	}
}

// ReadMessage returns a channel of incoming messages.
func (c *client) ReadMessage() <-chan GenericMessage {
	return c.msgCh
}

// safeSend safely sends a message to msgCh with proper synchronization
// Returns true if message was sent, false if client is closed or channel is full
func (c *client) safeSend(msg GenericMessage) bool {
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()

	if c.closed {
		return false
	}

	select {
	case c.msgCh <- msg:
		return true
	default:
		// Channel full
		return false
	}
}

// Send is a helper function that handles the common logic for sending messages
func (c *client) Send(msg any) error {
	if c.IsClosed() {
		return fmt.Errorf("client connection is closed")
	}

	// Prepare envelope based on the message type
	var envelope any
	switch m := msg.(type) {
	case RequestMessage:
		envelope = struct {
			Type string `json:"type"`
			RequestMessage
		}{
			Type:           TypeRequest,
			RequestMessage: m,
		}
	case ResponseMessage:
		envelope = struct {
			Type string `json:"type"`
			ResponseMessage
		}{
			Type:            TypeResponse,
			ResponseMessage: m,
		}
	case ErrorMessage:
		envelope = struct {
			Type string `json:"type"`
			ErrorMessage
		}{
			Type:         TypeError,
			ErrorMessage: m,
		}
	case EventMessage:
		envelope = struct {
			Type string `json:"type"`
			EventMessage
		}{
			Type:         TypeEvent,
			EventMessage: m,
		}
	default:
		return fmt.Errorf("message type not supported: %T", msg)
	}

	// Use pooled buffer for better performance
	buf := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufferPool.Put(buf)
	}()

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(envelope); err != nil {
		c.logger.WithError(err).Error("Failed to marshal message")
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Remove the trailing newline that Encoder adds
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}

	return c.conn.SendMessage(data)
}

// GetSource returns the message source for this client
func (c *client) GetSource() MessageSource {
	return c.source
}

// SourceToDestination converts a MessageSource to the correct MessageDestination
func SourceToDestination(source MessageSource) MessageDestination {
	switch source {
	case SystemWeb:
		return DestinationWeb
	case SystemDevice:
		return DestinationDevice
	case SystemAPI:
		return DestinationAPI
	default:
		// For unknown sources, return as-is (will be validated by router)
		return source
	}
}

// RegisterWill registers a Last Will message to be sent if connection closes unexpectedly
func (c *client) RegisterWill(will WillMessage) error {
	if c.IsClosed() {
		return fmt.Errorf("client connection is closed")
	}

	// Print the will message if logging is enabled

	// Prepare envelope for will message
	envelope := struct {
		Type string `json:"type"`
		WillMessage
	}{
		Type:        TypeWill,
		WillMessage: will,
	}

	// Use pooled buffer for better performance
	buf := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufferPool.Put(buf)
	}()

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(envelope); err != nil {
		c.logger.WithError(err).Error("Failed to marshal will message")
		return fmt.Errorf("failed to marshal will message: %w", err)
	}

	// Remove the trailing newline that Encoder adds
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}

	return c.conn.SendMessage(data)
}

// ClearWill clears the Last Will message (use before graceful disconnect)
func (c *client) ClearWill() error {
	// Send an empty will to clear it
	return c.RegisterWill(WillMessage{})
}

// Close safely closes the client connection.
func (c *client) Close() error {
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	c.closeOnce.Do(func() {
		close(c.msgCh)
	})
	return c.conn.Close()
}

// IsClosed returns whether the client is closed.
func (c *client) IsClosed() bool {
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()
	return c.closed
}

// SendRawJSON sends pre-serialized JSON bytes directly to the connection
// This is useful for forwarding stored messages without re-serialization
func (c *client) SendRawJSON(jsonBytes []byte) error {
	if c.IsClosed() {
		return fmt.Errorf("client connection is closed")
	}

	// Send directly without any processing
	return c.conn.SendMessage(jsonBytes)
}
