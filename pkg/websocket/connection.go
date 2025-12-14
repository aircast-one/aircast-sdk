package websocket

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

// Connection is the interface for WebSocket connection management
type Connection interface {
	Listen(ctx context.Context) error
	SendMessage(message []byte) error
	SendTextMessage(message []byte) error
	SendBinaryMessage(message []byte) error
	ReadMessage() <-chan []byte
	Close() error
	IsClosed() bool
}

// wsConn abstracts the underlying websocket connection
type wsConn interface {
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
	SetWriteDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetPongHandler(h func(string) error)
	Close() error
}

// connectionImpl implements the Connection interface
type connectionImpl struct {
	ws             wsConn
	logger         *log.Entry
	readCh         chan []byte
	doneCh         chan struct{}
	closeOnce      sync.Once
	mu             sync.Mutex // protects closed
	writeMu        sync.Mutex // protects writes to ws (prevents concurrent write panic)
	closed         bool
	pingTicker     *time.Ticker
	pongWait       time.Duration
	pingPeriod     time.Duration
	writeDeadline  time.Duration
	connStartTime  time.Time // Track connection start time for uptime logging
	onPongReceived func()    // Optional callback when pong is received
	pongCounter    int       // Track number of pongs received
	updateInterval int       // Update callback every N pongs (e.g., every 3rd pong = 1 minute)
}

// ConnectionOption is a function that configures a Connection
type ConnectionOption func(*connectionImpl)

// WithPongCallback sets a callback to be invoked periodically when pongs are received
// The callback will be invoked every updateInterval pongs (default: 3, which equals ~1 minute with 20s pings)
func WithPongCallback(callback func(), updateInterval int) ConnectionOption {
	return func(c *connectionImpl) {
		c.onPongReceived = callback
		c.updateInterval = updateInterval
	}
}

// WithConfig sets custom configuration for the connection
func WithConfig(cfg Config) ConnectionOption {
	return func(c *connectionImpl) {
		c.pongWait = cfg.PongWait
		c.pingPeriod = cfg.PingPeriod
		c.writeDeadline = cfg.WriteDeadline
		// Note: ReadBufferSize can only be set before channel is created
		// and is handled in New() function
	}
}

// New creates a new WebSocket connection wrapper from a gorilla websocket connection
func New(ws wsConn, logger *log.Entry, opts ...ConnectionOption) Connection {
	// Start with default configuration
	cfg := DefaultConfig()

	conn := &connectionImpl{
		ws:             ws,
		logger:         logger,
		readCh:         make(chan []byte, cfg.ReadBufferSize),
		doneCh:         make(chan struct{}),
		pongWait:       cfg.PongWait,
		pingPeriod:     cfg.PingPeriod,
		writeDeadline:  cfg.WriteDeadline,
		connStartTime:  time.Now(),
		updateInterval: 3, // Default: update every 3 pongs (~1 minute)
	}

	// Apply options
	for _, opt := range opts {
		opt(conn)
	}

	// Setup ping/pong handlers
	conn.setupPingPong()

	return conn
}

// setupPingPong configures ping/pong handlers for keepalive
func (c *connectionImpl) setupPingPong() {
	// Set initial read deadline - connection will timeout if no messages received
	c.logger.Trace("Setting initial read deadline to ", c.pongWait)
	if err := c.ws.SetReadDeadline(time.Now().Add(c.pongWait)); err != nil {
		c.logger.Error("Failed to set initial read deadline: ", err)
	}

	// Setup pong handler - updates read deadline when pong is received from our pings
	c.ws.SetPongHandler(func(appData string) error {
		c.logger.WithField("pong_wait", c.pongWait).Trace("Pong received, read deadline reset")

		// Increment pong counter and invoke callback periodically
		c.pongCounter++
		if c.onPongReceived != nil && c.pongCounter%c.updateInterval == 0 {
			c.logger.WithField("pong_count", c.pongCounter).Trace("Invoking pong callback")
			// Run callback in goroutine to avoid blocking pong handler
			go c.onPongReceived()
		}

		return c.ws.SetReadDeadline(time.Now().Add(c.pongWait))
	})
}

// Listen starts the read loop and listens for incoming messages until the context is canceled
func (c *connectionImpl) Listen(ctx context.Context) error {
	c.logger.Trace("Starting Listen with keepalive - pingPeriod: ", c.pingPeriod, " pongWait: ", c.pongWait)

	// Start ping ticker for keepalive (protected by mutex)
	c.mu.Lock()
	c.pingTicker = time.NewTicker(c.pingPeriod)
	c.mu.Unlock()
	defer c.pingTicker.Stop()

	// Start ping sender goroutine
	go c.sendPings()

	go func() {
		<-ctx.Done()
		c.logger.Trace("Context canceled, closing connection")
		_ = c.Close()
	}()

	// readLoop blocks until the connection is closed
	c.readLoop()
	c.logger.Trace("Exiting Listen")
	return nil
}

// sendPings sends periodic ping messages to keep the connection alive
func (c *connectionImpl) sendPings() {
	c.logger.WithField("ping_period", c.pingPeriod).Debug("Ping sender started")
	pingCount := 0
	consecutiveErrors := 0
	const maxConsecutiveErrors = 3 // Exit after 3 consecutive failures

	for {
		select {
		case <-c.pingTicker.C:
			pingCount++

			// Check if closed before attempting ping (no mutex held during I/O)
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				c.logger.WithField("ping_count", pingCount).Info("Ping sender stopped")
				return
			}
			c.mu.Unlock()

			c.logger.WithField("ping_count", pingCount).Trace("Sending ping")

			// Acquire write mutex to prevent concurrent writes
			c.writeMu.Lock()
			if err := c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				c.writeMu.Unlock()
				consecutiveErrors++
				c.logger.WithFields(map[string]interface{}{
					"ping_count":         pingCount,
					"consecutive_errors": consecutiveErrors,
					"error":              err,
				}).Warn("Failed to set write deadline for ping")

				// Only exit after multiple consecutive errors
				if consecutiveErrors >= maxConsecutiveErrors {
					c.logger.Error("Stopping ping sender after ", maxConsecutiveErrors, " consecutive errors")
					return
				}
				continue
			}

			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.writeMu.Unlock()
				consecutiveErrors++
				c.logger.WithFields(map[string]interface{}{
					"ping_count":         pingCount,
					"consecutive_errors": consecutiveErrors,
					"error":              err,
				}).Warn("Failed to send ping (transient error, will retry)")

				// Only exit after multiple consecutive errors
				if consecutiveErrors >= maxConsecutiveErrors {
					c.logger.Error("Stopping ping sender after ", maxConsecutiveErrors, " consecutive failures")
					return
				}
				continue
			}
			c.writeMu.Unlock()

			// Success - reset error counter
			consecutiveErrors = 0
			c.logger.WithFields(log.Fields{
				"ping_count": pingCount,
				"pong_wait":  c.pongWait,
			}).Trace("Ping sent successfully")

			// Log periodic summary (every 5 pings ≈ 1.5 minutes at 20s intervals)
			if pingCount%5 == 0 {
				c.logger.WithFields(log.Fields{
					"ping_count":        pingCount,
					"connection_uptime": time.Since(c.connStartTime).Round(time.Second).String(),
				}).Debug("Connection keepalive status")
			}
		case <-c.doneCh:
			c.logger.Trace("Done channel closed, stopping ping sender")
			return
		}
	}
}

// readLoop continuously reads from the websocket and pushes raw bytes into readCh
func (c *connectionImpl) readLoop() {
	c.logger.WithField("pong_wait", c.pongWait).Debug("Read loop started")
	messageCount := 0
	defer func() {
		c.logger.WithField("message_count", messageCount).Debug("Read loop stopped")
		close(c.readCh)
	}()
	for {
		select {
		case <-c.doneCh:
			c.logger.Trace("Done channel closed, exiting read loop")
			return
		default:
			// DO NOT set read deadline here - it's managed by the pong handler
			msgType, data, err := c.ws.ReadMessage()
			if err != nil {
				// Check for timeout error
				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() {
					c.logger.Error("READ TIMEOUT - no pong received within pongWait period (", c.pongWait, ") - connection will close")
					return
				}

				c.logger.Error("Error reading message: ", err)
				// Check for close errors
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					c.logger.Trace("Received normal close error: ", err)
					return
				}
				var opErr *net.OpError
				if errors.As(err, &opErr) {
					c.logger.Error("Network error: ", err)
					return
				}
				// For other errors, exit the loop
				return
			}

			// Only process text or binary messages
			if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
				c.logger.Trace("Ignoring non-text/binary message, type: ", msgType)
				continue
			}

			messageCount++
			select {
			case c.readCh <- data:
				c.logger.Trace("Message #", messageCount, " received and pushed to channel")
			case <-time.After(10 * time.Second):
				c.logger.Warn("Timeout pushing message to channel")
				continue
			case <-c.doneCh:
				c.logger.Trace("Done channel closed while pushing message")
				return
			}
		}
	}
}

// SendMessage sends a message as raw bytes (defaults to text message)
func (c *connectionImpl) SendMessage(message []byte) error {
	c.logger.Trace("SendMessage: entry point, delegating to SendTextMessage, size=", len(message))
	return c.SendTextMessage(message)
}

// SendTextMessage sends a text message as raw bytes
func (c *connectionImpl) SendTextMessage(message []byte) error {
	// Check closed status
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.logger.Warn("Attempt to send text message on closed connection")
		return ErrClosed
	}
	c.mu.Unlock()

	// Acquire write mutex to prevent concurrent writes (fixes "concurrent write to websocket connection" panic)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.logger.Trace("SendTextMessage: About to write ", len(message), " bytes")
	// Set a write deadline optimized for 4G networks
	if err := c.ws.SetWriteDeadline(time.Now().Add(c.writeDeadline)); err != nil {
		c.logger.Error("Error setting write deadline: ", err)
		return err
	}

	c.logger.Trace("SendTextMessage: Calling WriteMessage...")
	err := c.ws.WriteMessage(websocket.TextMessage, message)
	c.logger.Trace("SendTextMessage: WriteMessage returned")

	if err != nil {
		c.logger.WithError(err).Error("Failed to write WebSocket message")
	} else {
		c.logger.Trace("Successfully wrote WebSocket message: ", len(message), " bytes")
	}
	return err
}

// SendBinaryMessage sends a binary message as raw bytes
func (c *connectionImpl) SendBinaryMessage(message []byte) error {
	// Check closed status
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.logger.Warn("Attempt to send binary message on closed connection")
		return ErrClosed
	}
	c.mu.Unlock()

	// Acquire write mutex to prevent concurrent writes
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.logger.Trace("Sending binary message")
	// Set write deadline optimized for 4G networks
	if err := c.ws.SetWriteDeadline(time.Now().Add(c.writeDeadline)); err != nil {
		c.logger.Error("Error setting write deadline: ", err)
		return err
	}
	return c.ws.WriteMessage(websocket.BinaryMessage, message)
}

// ReadMessage returns the channel from which incoming raw messages can be read
func (c *connectionImpl) ReadMessage() <-chan []byte {
	return c.readCh
}

// Close terminates the connection
func (c *connectionImpl) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		// Stop ping ticker if it exists (while holding mutex)
		if c.pingTicker != nil {
			c.pingTicker.Stop()
		}
		c.mu.Unlock()
		c.logger.Trace("Closing connection")

		close(c.doneCh)

		// Acquire write mutex to prevent concurrent writes during close
		c.writeMu.Lock()
		defer c.writeMu.Unlock()

		// Set a write deadline for the close message to prevent indefinite blocking
		if err := c.ws.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
			c.logger.Trace("Failed to set write deadline for close message: ", err)
		}

		// Send a close message before closing
		if err := c.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
			c.logger.Error("Error sending close message: ", err)
		}
		if err := c.ws.Close(); err != nil {
			c.logger.Error("Error closing websocket: ", err)
		}
	})
	return nil
}

// IsClosed returns whether the connection is closed
func (c *connectionImpl) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
