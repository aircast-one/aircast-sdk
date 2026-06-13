package wtframing

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	cases := [][]byte{
		{},
		[]byte("a"),
		[]byte(`{"type":"request","action":"webrtc.answer","payload":{}}`),
		bytes.Repeat([]byte("x"), MaxMessageSize),
	}
	var buf bytes.Buffer
	for _, payload := range cases {
		if err := WriteMessage(&buf, payload); err != nil {
			t.Fatalf("WriteMessage(%d bytes): %v", len(payload), err)
		}
	}
	for i, want := range cases {
		got, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("ReadMessage #%d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadMessage #%d: got %d bytes, want %d", i, len(got), len(want))
		}
	}
}

func TestReadMessageCleanEOF(t *testing.T) {
	_, err := ReadMessage(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestReadMessageTruncatedHeader(t *testing.T) {
	_, err := ReadMessage(bytes.NewReader([]byte{0, 0}))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestReadMessageTruncatedPayload(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 10)
	r := bytes.NewReader(append(header[:], []byte("short")...))
	_, err := ReadMessage(r)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestWriteMessageTooLarge(t *testing.T) {
	err := WriteMessage(io.Discard, make([]byte, MaxMessageSize+1))
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("got %v, want ErrMessageTooLarge", err)
	}
}

func TestReadMessageRejectsOversizeLength(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], MaxMessageSize+1)
	_, err := ReadMessage(bytes.NewReader(header[:]))
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("got %v, want ErrMessageTooLarge", err)
	}
}
