package relay

import (
	"encoding/json"
	"fmt"

	"github.com/pavliha/aircast-sdk/pkg/message"
)

// Request represents an internal request structure for message handling
type Request struct {
	Action       string
	SessionID    string
	RequestID    string
	Source       string // Source of the request (web, api, device)
	Payload      map[string]any
	TraceContext map[string]string // W3C Trace Context (traceparent, tracestate)
}

// EventRequest represents an internal event structure with payload processing
type EventRequest struct {
	Action       string
	SessionID    string
	Source       string
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
	if reqMsg.ChannelID == "" {
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
		SessionID:    reqMsg.ChannelID,
		RequestID:    reqMsg.RequestID,
		Source:       reqMsg.Source,
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
