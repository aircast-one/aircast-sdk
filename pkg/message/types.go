package message

import (
	"errors"
)

type MessageType = string
type MessageAction = string
type MessageSource = string
type MessageDestination = string
type RequestID = string
type RoomID = string
type GenericMessage = any

// MessagePayload is the payload contained in a WebSocket message
type MessagePayload any

// Protocol message types
const (
	TypeRequest  MessageType = "request"
	TypeResponse MessageType = "response"
	TypeError    MessageType = "error"
	TypeEvent    MessageType = "event"
	TypeWill     MessageType = "will" // Last Will and Testament
)

// System identifiers (sources)
const (
	SystemDevice MessageSource = "device"
	SystemAPI    MessageSource = "api"
	SystemWeb    MessageSource = "web"
)

// Message routing destinations
const (
	// DestinationWeb routes to the web client(s) watching this device
	DestinationWeb MessageDestination = "web"
	// DestinationAPI routes to the API server (for internal API processing)
	DestinationAPI MessageDestination = "api"
	// DestinationDevice routes to the device/agent
	DestinationDevice MessageDestination = "device"
	// DestinationBroadcast routes to all connected clients (web + api)
	DestinationBroadcast MessageDestination = "broadcast"
)

// Protocol validation errors
var (
	ErrMissingType        = errors.New("missing required 'type' field")
	ErrMissingAction      = errors.New("missing required 'action' field")
	ErrMissingRequestID   = errors.New("missing required 'request_id' field")
	ErrMissingTimestamp   = errors.New("missing required 'timestamp' field")
	ErrInvalidMessageType = errors.New("invalid message type")
	ErrInvalidSystem      = errors.New("invalid system identifier")
)

// ErrDeviceNotFound Custom errors for domain operations
var (
	ErrDeviceNotFound = errors.New("device not found")
)

// RoomMessage is an interface for messages that belong to a room
type RoomMessage interface {
	GetRoomID() RoomID
}

// MutableRoomMessage extends RoomMessage with the ability to set the room ID
// Use pointer receivers when working with this interface
type MutableRoomMessage interface {
	RoomMessage
	SetRoomID(roomID RoomID)
}

// RequestMessage represents a client request
type RequestMessage struct {
	Action       MessageAction     `json:"action"`
	Payload      any               `json:"payload,omitempty"`
	Source       MessageSource     `json:"source"`
	Destination  MessageSource     `json:"destination"`
	RequestID    string            `json:"request_id"`
	RoomID       string            `json:"room_id,omitempty"`
	TraceContext map[string]string `json:"trace_context,omitempty"` // W3C Trace Context (traceparent, tracestate)
}

// ResponseMessage represents a server response
type ResponseMessage struct {
	Action       MessageAction      `json:"action"`
	Payload      any                `json:"payload,omitempty"`
	Source       MessageSource      `json:"source"`
	Destination  MessageDestination `json:"destination"`
	RoomID       RoomID             `json:"room_id,omitempty"`
	ReplyTo      RequestID          `json:"reply_to"`
	TraceContext map[string]string  `json:"trace_context,omitempty"` // W3C Trace Context for correlation
}

// ErrorResponse represents the error details
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

// ErrorMessage represents a server error response
type ErrorMessage struct {
	Action       MessageAction      `json:"action"`
	Source       MessageSource      `json:"source"`
	Destination  MessageDestination `json:"destination"`
	RoomID       RoomID             `json:"room_id,omitempty"`
	Error        ErrorResponse      `json:"error"`
	ReplyTo      RequestID          `json:"reply_to"`
	TraceContext map[string]string  `json:"trace_context,omitempty"` // W3C Trace Context for correlation
}

// EventMessage represents a server-initiated event
type EventMessage struct {
	Action       MessageAction      `json:"action"`
	Payload      any                `json:"payload,omitempty"`
	Source       MessageSource      `json:"source"`
	Destination  MessageDestination `json:"destination"`
	RoomID       RoomID             `json:"room_id,omitempty"`
	Retained     bool               `json:"retained,omitempty"`      // If true, API stores and sends to new clients
	TraceContext map[string]string  `json:"trace_context,omitempty"` // W3C Trace Context (traceparent, tracestate)
}

// WillMessage defines the Last Will and Testament message
// Sent by API if connection closes unexpectedly
type WillMessage struct {
	Action       MessageAction      `json:"action"`
	Payload      any                `json:"payload,omitempty"`
	Destination  MessageDestination `json:"destination"`
	TraceContext map[string]string  `json:"trace_context,omitempty"` // W3C Trace Context (traceparent, tracestate)
}

// GetRoomID implements RoomMessage interface for RequestMessage
func (m RequestMessage) GetRoomID() RoomID {
	return RoomID(m.RoomID)
}

// SetRoomID implements MutableRoomMessage interface for RequestMessage
func (m *RequestMessage) SetRoomID(roomID RoomID) {
	m.RoomID = string(roomID)
}

// GetRoomID implements RoomMessage interface for ResponseMessage
func (m ResponseMessage) GetRoomID() RoomID {
	return m.RoomID
}

// SetRoomID implements MutableRoomMessage interface for ResponseMessage
func (m *ResponseMessage) SetRoomID(roomID RoomID) {
	m.RoomID = roomID
}

// GetRoomID implements RoomMessage interface for ErrorMessage
func (m ErrorMessage) GetRoomID() RoomID {
	return m.RoomID
}

// SetRoomID implements MutableRoomMessage interface for ErrorMessage
func (m *ErrorMessage) SetRoomID(roomID RoomID) {
	m.RoomID = roomID
}

// GetRoomID implements RoomMessage interface for EventMessage
func (m EventMessage) GetRoomID() RoomID {
	return m.RoomID
}

// SetRoomID implements MutableRoomMessage interface for EventMessage
func (m *EventMessage) SetRoomID(roomID RoomID) {
	m.RoomID = roomID
}

// SetMessageRoomID sets the room ID on a message if it implements MutableRoomMessage
// or if it's a raw JSON map. Returns true if the room ID was set successfully.
func SetMessageRoomID(msg any, roomID RoomID) bool {
	// Try typed messages first
	if mutable, ok := msg.(MutableRoomMessage); ok {
		mutable.SetRoomID(roomID)
		return true
	}

	// Handle raw JSON maps (for clients sending untyped messages)
	if msgMap, ok := msg.(map[string]any); ok {
		msgMap["room_id"] = string(roomID)
		return true
	}

	return false
}

// SessionConfig configures persistent session behavior
type SessionConfig struct {
	ClientID     string `json:"client_id"`     // Persistent client identifier
	CleanSession bool   `json:"clean_session"` // If true, discard queued messages
}

// Channel represents a communication channel
type Channel struct {
	ID RoomID `json:"id"`
}
