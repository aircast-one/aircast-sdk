package message

import (
	"errors"
)

type MessageType = string
type MessageAction = string
type MessageSource = string
type MessageDestination = string
type RequestID = string
type ChannelID = string
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

// RequestMessage represents a client request
type RequestMessage struct {
	Action       MessageAction     `json:"action"`
	Payload      any               `json:"payload,omitempty"`
	Source       MessageSource     `json:"source"`
	Destination  MessageSource     `json:"destination"`
	RequestID    string            `json:"request_id"`
	ChannelID    string            `json:"channel_id,omitempty"`
	TraceContext map[string]string `json:"trace_context,omitempty"` // W3C Trace Context (traceparent, tracestate)
}

// ResponseMessage represents a server response
type ResponseMessage struct {
	Action       MessageAction      `json:"action"`
	Payload      any                `json:"payload,omitempty"`
	Source       MessageSource      `json:"source"`
	Destination  MessageDestination `json:"destination"`
	ChannelID    ChannelID          `json:"channel_id,omitempty"`
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
	ChannelID    ChannelID          `json:"channel_id,omitempty"`
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
	ChannelID    ChannelID          `json:"channel_id,omitempty"`
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

// SessionConfig configures persistent session behavior
type SessionConfig struct {
	ClientID     string `json:"client_id"`     // Persistent client identifier
	CleanSession bool   `json:"clean_session"` // If true, discard queued messages
}

// Channel represents a communication channel
type Channel struct {
	ID ChannelID `json:"id"`
}
