package relay

import (
	"encoding/json"
	"fmt"

	"github.com/pavliha/aircast-sdk/pkg/message"
)

// HandlerError represents an error with a custom error code
// This allows handlers to return specific error codes that will be sent to clients
type HandlerError struct {
	Code    string
	Message string
}

// Error implements the error interface
func (e *HandlerError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewError creates a new HandlerError with the given code and message
func NewError(code, message string) *HandlerError {
	return &HandlerError{
		Code:    code,
		Message: message,
	}
}

// Request represents an internal request structure for message handling
type Request struct {
	Action       string
	RoomID       string
	RequestID    string
	Source       string // Source of the request (web, api, device)
	FromMemberID string // Originating member ID (e.g., web session ID)
	Payload      map[string]any
	TraceContext map[string]string // W3C Trace Context (traceparent, tracestate)
}

// EventRequest represents an internal event structure with payload processing
type EventRequest struct {
	Action       string
	RoomID       string
	Source       string
	FromMemberID string // Originating member ID (e.g., web session ID)
	Payload      any
	TraceContext map[string]string // W3C Trace Context (traceparent, tracestate)
}

// CreateFromRequestMessage converts SDK RequestMessage to internal Request
func CreateFromRequestMessage(reqMsg message.RequestMessage) (*Request, error) {
	// Validate required fields
	if reqMsg.RequestID == "" {
		return nil, fmt.Errorf("request ID is required")
	}
	if reqMsg.Source == "" {
		return nil, fmt.Errorf("request source is required for response routing")
	}
	if reqMsg.RoomID == "" {
		return nil, fmt.Errorf("request channel ID is required")
	}

	var payload map[string]any

	if reqMsg.Payload != nil {
		var ok bool
		payload, ok = reqMsg.Payload.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid payload format")
		}
	}

	return &Request{
		Action:       reqMsg.Action,
		RoomID:       reqMsg.RoomID,
		RequestID:    reqMsg.RequestID,
		Source:       reqMsg.Source,
		FromMemberID: reqMsg.FromMemberID, // Preserve originating member ID for session management
		Payload:      payload,
		TraceContext: reqMsg.TraceContext, // Preserve W3C Trace Context for distributed tracing
	}, nil
}

// ProcessPayload unmarshals and validates the request payload into the provided struct
func (r *Request) ProcessPayload(target any) error {
	return processPayload(r.Payload, target)
}

// ProcessPayload unmarshals and validates the event payload into the provided struct
func (e *EventRequest) ProcessPayload(target any) error {
	return processPayload(e.Payload, target)
}

// processPayload is the shared payload processing logic
func processPayload(payload any, target any) error {
	if payload == nil {
		return nil
	}

	// Convert payload to JSON bytes for standard unmarshalling
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Unmarshal JSON to target struct
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return nil
}
