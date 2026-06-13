// Package wtframing provides length-prefixed message framing for byte-stream
// transports such as WebTransport bidirectional streams, where the underlying
// transport preserves no message boundaries. Each message is encoded as a
// 4-byte big-endian length prefix followed by that many payload bytes.
package wtframing

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MaxMessageSize bounds a single framed message. Reads and writes exceeding it
// fail rather than allocating unbounded memory from a hostile or corrupt peer.
const MaxMessageSize = 1 << 20

// ErrMessageTooLarge is returned when a payload exceeds MaxMessageSize.
var ErrMessageTooLarge = errors.New("wtframing: message exceeds max size")

// WriteMessage frames payload and writes it to w in a single write.
func WriteMessage(w io.Writer, payload []byte) error {
	if len(payload) > MaxMessageSize {
		return fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, len(payload), MaxMessageSize)
	}
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	_, err := w.Write(frame)
	return err
}

// ReadMessage reads one framed message from r, returning the payload. It returns
// io.EOF only when r is at a clean frame boundary; a truncated frame returns
// io.ErrUnexpectedEOF.
func ReadMessage(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length > MaxMessageSize {
		return nil, fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, length, MaxMessageSize)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}
