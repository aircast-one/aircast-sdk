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

	// Quick validation of required fields only
	if genericMsg["action"] == nil {
		return nil, ErrMissingAction
	}

	// Reset reader and use json.Unmarshal for final parsing (faster than second decoder)
	// Based on the type field, unmarshal into the appropriate struct.
	switch messageType {
	case TypeRequest:
		if genericMsg["request_id"] == nil {
			return nil, ErrMissingRequestID
		}
		var req RequestMessage
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to RequestMessage: %w", err)
		}
		// Normalize legacy "client" source to "web"
		if req.Source == "client" {
			req.Source = SystemWeb
		}
		return req, nil
	case TypeResponse:
		if genericMsg["reply_to"] == nil {
			return nil, errors.New("response must include 'reply_to' field")
		}
		var res ResponseMessage
		if err := json.Unmarshal(data, &res); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to ResponseMessage: %w", err)
		}
		return res, nil
	case TypeError:
		if genericMsg["reply_to"] == nil {
			return nil, errors.New("error must include 'reply_to' field")
		}
		if genericMsg["error"] == nil {
			return nil, errors.New("error must include 'error' field")
		}
		var errMsg ErrorMessage
		if err := json.Unmarshal(data, &errMsg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to ErrorMessage: %w", err)
		}
		return errMsg, nil
	case TypeEvent:
		var event EventMessage
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to EventMessage: %w", err)
		}
		return event, nil
	case TypeWill:
		var will WillMessage
		if err := json.Unmarshal(data, &will); err != nil {
			return nil, fmt.Errorf("failed to unmarshal to WillMessage: %w", err)
		}
		return will, nil
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
