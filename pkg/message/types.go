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

// RoutableMessage is an interface for messages that can be routed based on destination
type RoutableMessage interface {
	GetDestination() MessageDestination
}

// MemberRoutableMessage is an interface for messages that can be routed to specific members
type MemberRoutableMessage interface {
	GetFromMemberID() string
	GetToMemberID() string
}

// MutableMemberRoutableMessage extends MemberRoutableMessage with setters
type MutableMemberRoutableMessage interface {
	MemberRoutableMessage
	SetFromMemberID(memberID string)
	SetToMemberID(memberID string)
}

// RequestMessage represents a client request
type RequestMessage struct {
	Action       MessageAction     `json:"action"`
	Payload      any               `json:"payload,omitempty"`
	Source       MessageSource     `json:"source"`
	Destination  MessageSource     `json:"destination"`
	RequestID    string            `json:"request_id"`
	RoomID       string            `json:"room_id,omitempty"`
	FromMemberID string            `json:"from_member_id,omitempty"` // Originating member ID
	ToMemberID   string            `json:"to_member_id,omitempty"`   // Target member for routing
	TraceContext map[string]string `json:"trace_context,omitempty"`  // W3C Trace Context (traceparent, tracestate)
}

// ResponseMessage represents a server response
type ResponseMessage struct {
	Action       MessageAction      `json:"action"`
	Payload      any                `json:"payload,omitempty"`
	Source       MessageSource      `json:"source"`
	Destination  MessageDestination `json:"destination"`
	RoomID       RoomID             `json:"room_id,omitempty"`
	FromMemberID string             `json:"from_member_id,omitempty"` // Originating member ID
	ToMemberID   string             `json:"to_member_id,omitempty"`   // Target member for routing
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
	RoomID       string             `json:"room_id,omitempty"`
	FromMemberID string             `json:"from_member_id,omitempty"` // Originating member ID
	ToMemberID   string             `json:"to_member_id,omitempty"`   // Target member for routing
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
	RoomID       string             `json:"room_id,omitempty"`
	FromMemberID string             `json:"from_member_id,omitempty"` // Originating member ID
	ToMemberID   string             `json:"to_member_id,omitempty"`   // Target member for routing
	Retained     bool               `json:"retained,omitempty"`       // If true, API stores and sends to new clients
	TraceContext map[string]string  `json:"trace_context,omitempty"`  // W3C Trace Context (traceparent, tracestate)
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

// GetDestination implements RoutableMessage interface for RequestMessage
func (m RequestMessage) GetDestination() MessageDestination {
	return MessageDestination(m.Destination)
}

// GetFromMemberID implements MemberRoutableMessage interface for RequestMessage
func (m RequestMessage) GetFromMemberID() string {
	return m.FromMemberID
}

// GetToMemberID implements MemberRoutableMessage interface for RequestMessage
func (m RequestMessage) GetToMemberID() string {
	return m.ToMemberID
}

// SetFromMemberID implements MutableMemberRoutableMessage interface for RequestMessage
func (m *RequestMessage) SetFromMemberID(memberID string) {
	m.FromMemberID = memberID
}

// SetToMemberID implements MutableMemberRoutableMessage interface for RequestMessage
func (m *RequestMessage) SetToMemberID(memberID string) {
	m.ToMemberID = memberID
}

// GetRoomID implements RoomMessage interface for ResponseMessage
func (m ResponseMessage) GetRoomID() RoomID {
	return m.RoomID
}

// SetRoomID implements MutableRoomMessage interface for ResponseMessage
func (m *ResponseMessage) SetRoomID(roomID RoomID) {
	m.RoomID = roomID
}

// GetDestination implements RoutableMessage interface for ResponseMessage
func (m ResponseMessage) GetDestination() MessageDestination {
	return m.Destination
}

// GetFromMemberID implements MemberRoutableMessage interface for ResponseMessage
func (m ResponseMessage) GetFromMemberID() string {
	return m.FromMemberID
}

// GetToMemberID implements MemberRoutableMessage interface for ResponseMessage
func (m ResponseMessage) GetToMemberID() string {
	return m.ToMemberID
}

// SetFromMemberID implements MutableMemberRoutableMessage interface for ResponseMessage
func (m *ResponseMessage) SetFromMemberID(memberID string) {
	m.FromMemberID = memberID
}

// SetToMemberID implements MutableMemberRoutableMessage interface for ResponseMessage
func (m *ResponseMessage) SetToMemberID(memberID string) {
	m.ToMemberID = memberID
}

// GetRoomID implements RoomMessage interface for ErrorMessage
func (m ErrorMessage) GetRoomID() RoomID {
	return m.RoomID
}

// SetRoomID implements MutableRoomMessage interface for ErrorMessage
func (m *ErrorMessage) SetRoomID(roomID RoomID) {
	m.RoomID = roomID
}

// GetDestination implements RoutableMessage interface for ErrorMessage
func (m ErrorMessage) GetDestination() MessageDestination {
	return m.Destination
}

// GetFromMemberID implements MemberRoutableMessage interface for ErrorMessage
func (m ErrorMessage) GetFromMemberID() string {
	return m.FromMemberID
}

// GetToMemberID implements MemberRoutableMessage interface for ErrorMessage
func (m ErrorMessage) GetToMemberID() string {
	return m.ToMemberID
}

// SetFromMemberID implements MutableMemberRoutableMessage interface for ErrorMessage
func (m *ErrorMessage) SetFromMemberID(memberID string) {
	m.FromMemberID = memberID
}

// SetToMemberID implements MutableMemberRoutableMessage interface for ErrorMessage
func (m *ErrorMessage) SetToMemberID(memberID string) {
	m.ToMemberID = memberID
}

// GetRoomID implements RoomMessage interface for EventMessage
func (m EventMessage) GetRoomID() RoomID {
	return m.RoomID
}

// SetRoomID implements MutableRoomMessage interface for EventMessage
func (m *EventMessage) SetRoomID(roomID RoomID) {
	m.RoomID = roomID
}

// GetDestination implements RoutableMessage interface for EventMessage
func (m EventMessage) GetDestination() MessageDestination {
	return m.Destination
}

// GetFromMemberID implements MemberRoutableMessage interface for EventMessage
func (m EventMessage) GetFromMemberID() string {
	return m.FromMemberID
}

// GetToMemberID implements MemberRoutableMessage interface for EventMessage
func (m EventMessage) GetToMemberID() string {
	return m.ToMemberID
}

// SetFromMemberID implements MutableMemberRoutableMessage interface for EventMessage
func (m *EventMessage) SetFromMemberID(memberID string) {
	m.FromMemberID = memberID
}

// SetToMemberID implements MutableMemberRoutableMessage interface for EventMessage
func (m *EventMessage) SetToMemberID(memberID string) {
	m.ToMemberID = memberID
}

// GetDestination implements RoutableMessage interface for WillMessage
func (m WillMessage) GetDestination() MessageDestination {
	return m.Destination
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

// ExtractRoomIDFromMessage extracts the room ID from a message if it implements RoomMessage
// or if it's a raw JSON map. Returns empty string if room ID cannot be extracted.
func ExtractRoomIDFromMessage(msg any) string {
	// Try typed messages first
	if roomMsg, ok := msg.(RoomMessage); ok {
		return string(roomMsg.GetRoomID())
	}

	// Handle raw JSON maps (for clients sending untyped messages)
	if msgMap, ok := msg.(map[string]any); ok {
		if roomID, ok := msgMap["room_id"].(string); ok {
			return roomID
		}
	}

	return ""
}

// GetMessageDestination extracts the destination from a message if it implements RoutableMessage
// or if it's a raw JSON map. Returns empty string if destination cannot be extracted.
func GetMessageDestination(msg any) MessageDestination {
	// Try typed messages first
	if routable, ok := msg.(RoutableMessage); ok {
		return routable.GetDestination()
	}

	// Handle raw JSON maps (for clients sending untyped messages)
	if msgMap, ok := msg.(map[string]any); ok {
		if dest, ok := msgMap["destination"].(string); ok {
			return MessageDestination(dest)
		}
	}

	return ""
}

// SetMessageDestination sets the destination field on a message if possible.
// Returns true if the destination was set, false otherwise.
func SetMessageDestination(msg any, destination MessageDestination) bool {
	switch m := msg.(type) {
	case *RequestMessage:
		m.Destination = destination
		return true
	case *ResponseMessage:
		m.Destination = destination
		return true
	case *ErrorMessage:
		m.Destination = destination
		return true
	case *EventMessage:
		m.Destination = destination
		return true
	case map[string]any:
		m["destination"] = string(destination)
		return true
	default:
		// Value types or unsupported types
		return false
	}
}

// GetMessageToMemberID extracts the to_member_id from a message
// Returns empty string if the message doesn't have this field
func GetMessageToMemberID(msg any) string {
	// Try typed messages first using interface
	if routable, ok := msg.(MemberRoutableMessage); ok {
		return routable.GetToMemberID()
	}

	// Handle raw JSON maps (for clients sending untyped messages)
	if msgMap, ok := msg.(map[string]any); ok {
		if id, ok := msgMap["to_member_id"].(string); ok {
			return id
		}
	}

	return ""
}

func SetMessageToMemberID(msg any, toMemberID string) bool {
	// Try typed messages first using interface
	if mutable, ok := msg.(MutableMemberRoutableMessage); ok {
		mutable.SetToMemberID(toMemberID)
		return true
	}

	// Handle raw JSON maps (for clients sending untyped messages)
	if msgMap, ok := msg.(map[string]any); ok {
		msgMap["to_member_id"] = toMemberID
		return true
	}

	// Value types or unsupported types
	return false
}

// GetMessageFromMemberID extracts the from_member_id from a message
// Returns empty string if the message doesn't have this field
func GetMessageFromMemberID(msg any) string {
	// Try typed messages first using interface
	if routable, ok := msg.(MemberRoutableMessage); ok {
		return routable.GetFromMemberID()
	}

	// Handle raw JSON maps (for clients sending untyped messages)
	if msgMap, ok := msg.(map[string]any); ok {
		if id, ok := msgMap["from_member_id"].(string); ok {
			return id
		}
	}

	return ""
}

// SetMessageFromMemberID sets the from_member_id on a message
// Returns true if the field was set, false otherwise
func SetMessageFromMemberID(msg any, fromMemberID string) bool {
	// Try typed messages first using interface
	if mutable, ok := msg.(MutableMemberRoutableMessage); ok {
		mutable.SetFromMemberID(fromMemberID)
		return true
	}

	// Handle raw JSON maps (for clients sending untyped messages)
	if msgMap, ok := msg.(map[string]any); ok {
		msgMap["from_member_id"] = fromMemberID
		return true
	}

	// Value types or unsupported types
	return false
}

// IsValidDestination checks if a destination is one of the allowed values
func IsValidDestination(dest MessageDestination) bool {
	switch dest {
	case DestinationWeb, DestinationAPI, DestinationDevice, DestinationBroadcast:
		return true
	default:
		return false
	}
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
