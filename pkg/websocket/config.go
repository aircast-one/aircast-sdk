package websocket

import "time"

// Config contains configuration options for WebSocket connections
type Config struct {
	// PongWait is the time to wait for a pong response before considering the connection dead
	// Optimized for 4G networks with higher latency tolerance
	PongWait time.Duration

	// PingPeriod is the interval at which to send ping messages
	// Must be less than PongWait (typically PongWait/3)
	PingPeriod time.Duration

	// WriteDeadline is the timeout for write operations
	WriteDeadline time.Duration

	// ReadBufferSize is the size of the read channel buffer
	ReadBufferSize int
}

// DefaultConfig returns the default configuration optimized for 4G networks
func DefaultConfig() Config {
	return Config{
		PongWait:       90 * time.Second, // Increased from 30s to tolerate 4G latency spikes
		PingPeriod:     20 * time.Second, // Send ping every 20 seconds (must be less than PongWait/3)
		WriteDeadline:  15 * time.Second, // Increased from 10s for 4G bandwidth optimization
		ReadBufferSize: 100,
	}
}
