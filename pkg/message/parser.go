package message

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Pool for reusing generic message maps
var messageMapPool = sync.Pool{
	New: func() any {
		return make(map[string]any, 8) // Pre-allocate for common message size
	},
}

func UnmarshalMessage(data []byte) (any, error) {
	// Get generic message map from pool
	genericMsg := messageMapPool.Get().(map[string]any)
	defer func() {
		// Clear map and return to pool
		for k := range genericMsg {
			delete(genericMsg, k)
		}
		messageMapPool.Put(genericMsg)
	}()

	// Use decoder with bytes reader for better performance
	reader := bytes.NewReader(data)
	decoder := json.NewDecoder(reader)

	if err := decoder.Decode(&genericMsg); err != nil {
		return nil, fmt.Errorf("failed to parse generic message: %w", err)
	}

	// Retrieve the message type quickly
	messageType, ok := genericMsg["type"].(string)
	if !ok {
		return nil, errors.New("invalid message type field")
	}

	// Validate action field
	action, ok := genericMsg["action"].(string)
	if !ok || action == "" {
		return nil, ErrMissingAction
	}

	// Reset reader and use json.Unmarshal for final parsing (faster than second decoder)
	// Based on the type field, unmarshal into the appropriate struct.
	switch messageType {
	case TypeRequest:
		if err := validateRequestFields(genericMsg); err != nil {
			return nil, err
		}
		var req RequestMessage
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to RequestMessage: %w", err)
		}
		if err := validateRequestMessage(&req); err != nil {
			return nil, err
		}
		return &req, nil
	case TypeResponse:
		if err := validateResponseFields(genericMsg); err != nil {
			return nil, err
		}
		var res ResponseMessage
		if err := json.Unmarshal(data, &res); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to ResponseMessage: %w", err)
		}
		if err := validateResponseMessage(&res); err != nil {
			return nil, err
		}
		return &res, nil
	case TypeError:
		if err := validateErrorFields(genericMsg); err != nil {
			return nil, err
		}
		var errMsg ErrorMessage
		if err := json.Unmarshal(data, &errMsg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to ErrorMessage: %w", err)
		}
		if err := validateErrorMessage(&errMsg); err != nil {
			return nil, err
		}
		return &errMsg, nil
	case TypeEvent:
		if err := validateEventFields(genericMsg); err != nil {
			return nil, err
		}
		var event EventMessage
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to EventMessage: %w", err)
		}
		if err := validateEventMessage(&event); err != nil {
			return nil, err
		}
		return &event, nil
	case TypeWill:
		if err := validateWillFields(genericMsg); err != nil {
			return nil, err
		}
		var will WillMessage
		if err := json.Unmarshal(data, &will); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to WillMessage: %w", err)
		}
		if err := validateWillMessage(&will); err != nil {
			return nil, err
		}
		return &will, nil
	default:
		return nil, fmt.Errorf("unknown message type: %s", messageType)
	}
}

// validateMessage validates a message against the protocol requirements
func validateMessage(msg map[string]any) error {
	// Check required fields
	if msg["type"] == nil {
		return ErrMissingType
	}

	msgType, ok := msg["type"].(string)
	if !ok {
		return fmt.Errorf("%w: type must be a string", ErrInvalidMessageType)
	}

	// Validate type is one of the allowed values from the protocol
	switch msgType {
	case TypeRequest, TypeResponse, TypeError, TypeEvent, TypeWill:
		// Valid type according to protocol.md
	default:
		return fmt.Errorf("%w: '%s' is not a valid message type according to protocol", ErrInvalidMessageType, msgType)
	}

	// Action is required for all message types
	if msg["action"] == nil {
		return ErrMissingAction
	}

	// Additional validations for specific message types
	switch msgType {
	case TypeRequest:
		// Request ID is required only for Request messages
		if msg["request_id"] == nil {
			return ErrMissingRequestID
		}
	case TypeResponse:
		if msg["reply_to"] == nil {
			return errors.New("response must include 'reply_to' field")
		}
	case TypeError:
		if msg["reply_to"] == nil {
			return errors.New("error must include 'reply_to' field")
		}
		if msg["error"] == nil {
			return errors.New("error must include 'error' field")
		}
	}

	return nil
}

// isValidSource checks if a source is one of the valid system identifiers
func isValidSource(source MessageSource) bool {
	switch source {
	case SystemDevice, SystemAPI, SystemWeb:
		return true
	default:
		return false
	}
}

// isValidDestination checks if a destination is one of the valid routing destinations
func isValidDestination(destination MessageDestination) bool {
	switch destination {
	case DestinationWeb, DestinationAPI, DestinationDevice, DestinationBroadcast:
		return true
	default:
		return false
	}
}

// validateRequestFields validates request-specific fields in the generic map
func validateRequestFields(msg map[string]any) error {
	requestID, ok := msg["request_id"].(string)
	if !ok || requestID == "" {
		return ErrMissingRequestID
	}
	return nil
}

// validateRequestMessage validates a parsed RequestMessage struct
func validateRequestMessage(req *RequestMessage) error {
	if req.RequestID == "" {
		return ErrMissingRequestID
	}
	if req.Action == "" {
		return ErrMissingAction
	}
	if req.Source == "" {
		return errors.New("missing required 'source' field")
	}
	if !isValidSource(req.Source) {
		return fmt.Errorf("%w: '%s' is not a valid source", ErrInvalidSystem, req.Source)
	}
	if req.Destination == "" {
		return errors.New("missing required 'destination' field")
	}
	if !isValidDestination(req.Destination) {
		return fmt.Errorf("invalid destination: '%s' is not a valid destination", req.Destination)
	}
	return nil
}

// validateResponseFields validates response-specific fields in the generic map
func validateResponseFields(msg map[string]any) error {
	replyTo, ok := msg["reply_to"].(string)
	if !ok || replyTo == "" {
		return errors.New("response must include non-empty 'reply_to' field")
	}
	return nil
}

// validateResponseMessage validates a parsed ResponseMessage struct
func validateResponseMessage(res *ResponseMessage) error {
	if res.ReplyTo == "" {
		return errors.New("response must include non-empty 'reply_to' field")
	}
	if res.Action == "" {
		return ErrMissingAction
	}
	if res.Source == "" {
		return errors.New("missing required 'source' field")
	}
	if !isValidSource(res.Source) {
		return fmt.Errorf("%w: '%s' is not a valid source", ErrInvalidSystem, res.Source)
	}
	if res.Destination == "" {
		return errors.New("missing required 'destination' field")
	}
	if !isValidDestination(res.Destination) {
		return fmt.Errorf("invalid destination: '%s' is not a valid destination", res.Destination)
	}
	return nil
}

// validateErrorFields validates error-specific fields in the generic map
func validateErrorFields(msg map[string]any) error {
	replyTo, ok := msg["reply_to"].(string)
	if !ok || replyTo == "" {
		return errors.New("error must include non-empty 'reply_to' field")
	}

	errorField, ok := msg["error"].(map[string]any)
	if !ok {
		return errors.New("error must include 'error' field as an object")
	}

	code, ok := errorField["code"].(string)
	if !ok || code == "" {
		return errors.New("error.code must be a non-empty string")
	}

	message, ok := errorField["message"].(string)
	if !ok || message == "" {
		return errors.New("error.message must be a non-empty string")
	}

	return nil
}

// validateErrorMessage validates a parsed ErrorMessage struct
func validateErrorMessage(errMsg *ErrorMessage) error {
	if errMsg.ReplyTo == "" {
		return errors.New("error must include non-empty 'reply_to' field")
	}
	if errMsg.Action == "" {
		return ErrMissingAction
	}
	if errMsg.Source == "" {
		return errors.New("missing required 'source' field")
	}
	if !isValidSource(errMsg.Source) {
		return fmt.Errorf("%w: '%s' is not a valid source", ErrInvalidSystem, errMsg.Source)
	}
	if errMsg.Destination == "" {
		return errors.New("missing required 'destination' field")
	}
	if !isValidDestination(errMsg.Destination) {
		return fmt.Errorf("invalid destination: '%s' is not a valid destination", errMsg.Destination)
	}
	if errMsg.Error.Code == "" {
		return errors.New("error.code must be a non-empty string")
	}
	if errMsg.Error.Message == "" {
		return errors.New("error.message must be a non-empty string")
	}
	return nil
}

// validateEventFields validates event-specific fields in the generic map
func validateEventFields(msg map[string]any) error {
	// Events don't have additional required fields beyond action
	return nil
}

// validateEventMessage validates a parsed EventMessage struct
func validateEventMessage(event *EventMessage) error {
	if event.Action == "" {
		return ErrMissingAction
	}
	if event.Source == "" {
		return errors.New("missing required 'source' field")
	}
	if !isValidSource(event.Source) {
		return fmt.Errorf("%w: '%s' is not a valid source", ErrInvalidSystem, event.Source)
	}
	if event.Destination == "" {
		return errors.New("missing required 'destination' field")
	}
	if !isValidDestination(event.Destination) {
		return fmt.Errorf("invalid destination: '%s' is not a valid destination", event.Destination)
	}
	return nil
}

// validateWillFields validates will-specific fields in the generic map
func validateWillFields(msg map[string]any) error {
	// Will messages don't have additional required fields beyond action
	return nil
}

// validateWillMessage validates a parsed WillMessage struct
func validateWillMessage(will *WillMessage) error {
	if will.Action == "" {
		return ErrMissingAction
	}
	if will.Destination == "" {
		return errors.New("missing required 'destination' field")
	}
	if !isValidDestination(will.Destination) {
		return fmt.Errorf("invalid destination: '%s' is not a valid destination", will.Destination)
	}
	return nil
}
