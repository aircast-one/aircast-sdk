package websocket

import "errors"

var (
	// ErrCloseMessage indicates a close message was received from the peer
	ErrCloseMessage = errors.New("close message received")

	// ErrClosed indicates an operation was attempted on a closed connection
	ErrClosed = errors.New("connection is closed")
)
