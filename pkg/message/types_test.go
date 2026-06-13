package message

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageTypeConstants(t *testing.T) {
	// Test that message type constants have expected values
	assert.Equal(t, "request", TypeRequest)
	assert.Equal(t, "response", TypeResponse)
	assert.Equal(t, "error", TypeError)
	assert.Equal(t, "event", TypeEvent)
}

func TestSystemConstants(t *testing.T) {
	// Test that system identifier constants have expected values
	assert.Equal(t, "device", SystemDevice)
	assert.Equal(t, "api", SystemAPI)
}

func TestErrorVariables(t *testing.T) {
	// Test protocol validation errors
	assert.Equal(t, "missing required 'type' field", ErrMissingType.Error())
	assert.Equal(t, "missing required 'action' field", ErrMissingAction.Error())
	assert.Equal(t, "missing required 'request_id' field", ErrMissingRequestID.Error())
	assert.Equal(t, "missing required 'timestamp' field", ErrMissingTimestamp.Error())
	assert.Equal(t, "invalid message type", ErrInvalidMessageType.Error())
	assert.Equal(t, "invalid system identifier", ErrInvalidSystem.Error())

	// Test domain operation errors
	assert.Equal(t, "device not found", ErrDeviceNotFound.Error())

	// Verify they are actual errors
	assert.True(t, errors.Is(ErrMissingType, ErrMissingType))
	assert.True(t, errors.Is(ErrMissingAction, ErrMissingAction))
	assert.True(t, errors.Is(ErrMissingRequestID, ErrMissingRequestID))
	assert.True(t, errors.Is(ErrMissingTimestamp, ErrMissingTimestamp))
	assert.True(t, errors.Is(ErrInvalidMessageType, ErrInvalidMessageType))
	assert.True(t, errors.Is(ErrInvalidSystem, ErrInvalidSystem))
	assert.True(t, errors.Is(ErrDeviceNotFound, ErrDeviceNotFound))
}

func TestRequestMessage(t *testing.T) {
	t.Run("marshal and unmarshal", func(t *testing.T) {
		original := RequestMessage{
			Action:    "test_action",
			Payload:   map[string]string{"key": "value"},
			Source:    SystemDevice,
			RequestID: "req-123",
			RoomID:    "channel-456",
		}

		// Marshal to JSON
		data, err := json.Marshal(original)
		assert.NoError(t, err)

		// Unmarshal back
		var decoded RequestMessage
		err = json.Unmarshal(data, &decoded)
		assert.NoError(t, err)

		assert.Equal(t, original.Action, decoded.Action)
		assert.Equal(t, original.Source, decoded.Source)
		assert.Equal(t, original.RequestID, decoded.RequestID)
		assert.Equal(t, original.RoomID, decoded.RoomID)
	})

	t.Run("omits empty payload", func(t *testing.T) {
		msg := RequestMessage{
			Action:    "test_action",
			Source:    SystemDevice,
			RequestID: "req-123",
		}

		data, err := json.Marshal(msg)
		assert.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)

		_, hasPayload := result["payload"]
		assert.False(t, hasPayload)
	})

	t.Run("omits empty room_id", func(t *testing.T) {
		msg := RequestMessage{
			Action:    "test_action",
			Source:    SystemDevice,
			RequestID: "req-123",
		}

		data, err := json.Marshal(msg)
		assert.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)

		_, hasRoomID := result["room_id"]
		assert.False(t, hasRoomID)
	})
}

func TestResponseMessage(t *testing.T) {
	t.Run("marshal and unmarshal", func(t *testing.T) {
		original := ResponseMessage{
			Action:  "test_action",
			Payload: map[string]string{"status": "success"},
			Source:  SystemAPI,
			RoomID:  "channel-789",
			ReplyTo: "req-123",
		}

		// Marshal to JSON
		data, err := json.Marshal(original)
		assert.NoError(t, err)

		// Unmarshal back
		var decoded ResponseMessage
		err = json.Unmarshal(data, &decoded)
		assert.NoError(t, err)

		assert.Equal(t, original.Action, decoded.Action)
		assert.Equal(t, original.Source, decoded.Source)
		assert.Equal(t, original.RoomID, decoded.RoomID)
		assert.Equal(t, original.ReplyTo, decoded.ReplyTo)
	})

	t.Run("omits empty payload", func(t *testing.T) {
		msg := ResponseMessage{
			Action:  "test_action",
			Source:  SystemAPI,
			ReplyTo: "req-123",
		}

		data, err := json.Marshal(msg)
		assert.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)

		_, hasPayload := result["payload"]
		assert.False(t, hasPayload)
	})

	t.Run("omits empty room_id", func(t *testing.T) {
		msg := ResponseMessage{
			Action:  "test_action",
			Source:  SystemAPI,
			ReplyTo: "req-123",
		}

		data, err := json.Marshal(msg)
		assert.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)

		_, hasRoomID := result["room_id"]
		assert.False(t, hasRoomID)
	})
}

func TestErrorResponse(t *testing.T) {
	t.Run("marshal and unmarshal", func(t *testing.T) {
		original := ErrorResponse{
			Code:    "ERR_NOT_FOUND",
			Message: "Resource not found",
			Details: map[string]string{"resource": "device"},
		}

		// Marshal to JSON
		data, err := json.Marshal(original)
		assert.NoError(t, err)

		// Unmarshal back
		var decoded ErrorResponse
		err = json.Unmarshal(data, &decoded)
		assert.NoError(t, err)

		assert.Equal(t, original.Code, decoded.Code)
		assert.Equal(t, original.Message, decoded.Message)
	})

	t.Run("omits empty details", func(t *testing.T) {
		errResp := ErrorResponse{
			Code:    "ERR_CODE",
			Message: "Error message",
		}

		data, err := json.Marshal(errResp)
		assert.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)

		_, hasDetails := result["details"]
		assert.False(t, hasDetails)
	})
}

func TestErrorMessage(t *testing.T) {
	t.Run("marshal and unmarshal", func(t *testing.T) {
		original := ErrorMessage{
			Action: "failed_action",
			Source: SystemDevice,
			RoomID: "channel-123",
			Error: ErrorResponse{
				Code:    "ERR_FAILED",
				Message: "Operation failed",
				Details: map[string]string{"reason": "timeout"},
			},
			ReplyTo: "req-456",
		}

		// Marshal to JSON
		data, err := json.Marshal(original)
		assert.NoError(t, err)

		// Unmarshal back
		var decoded ErrorMessage
		err = json.Unmarshal(data, &decoded)
		assert.NoError(t, err)

		assert.Equal(t, original.Action, decoded.Action)
		assert.Equal(t, original.Source, decoded.Source)
		assert.Equal(t, original.RoomID, decoded.RoomID)
		assert.Equal(t, original.ReplyTo, decoded.ReplyTo)
		assert.Equal(t, original.Error.Code, decoded.Error.Code)
		assert.Equal(t, original.Error.Message, decoded.Error.Message)
	})

	t.Run("omits empty room_id", func(t *testing.T) {
		msg := ErrorMessage{
			Action: "failed_action",
			Source: SystemDevice,
			Error: ErrorResponse{
				Code:    "ERR_CODE",
				Message: "Error message",
			},
			ReplyTo: "req-123",
		}

		data, err := json.Marshal(msg)
		assert.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)

		_, hasRoomID := result["room_id"]
		assert.False(t, hasRoomID)
	})
}

func TestEventMessage(t *testing.T) {
	t.Run("marshal and unmarshal", func(t *testing.T) {
		original := EventMessage{
			Action:  "device_connected",
			Payload: map[string]string{"device_id": "device-123"},
			Source:  SystemDevice,
			RoomID:  "channel-999",
		}

		// Marshal to JSON
		data, err := json.Marshal(original)
		assert.NoError(t, err)

		// Unmarshal back
		var decoded EventMessage
		err = json.Unmarshal(data, &decoded)
		assert.NoError(t, err)

		assert.Equal(t, original.Action, decoded.Action)
		assert.Equal(t, original.Source, decoded.Source)
		assert.Equal(t, original.RoomID, decoded.RoomID)
	})

	t.Run("omits empty payload", func(t *testing.T) {
		msg := EventMessage{
			Action: "test_event",
			Source: SystemAPI,
		}

		data, err := json.Marshal(msg)
		assert.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)

		_, hasPayload := result["payload"]
		assert.False(t, hasPayload)
	})

	t.Run("omits empty room_id", func(t *testing.T) {
		msg := EventMessage{
			Action: "test_event",
			Source: SystemAPI,
		}

		data, err := json.Marshal(msg)
		assert.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)

		_, hasRoomID := result["room_id"]
		assert.False(t, hasRoomID)
	})
}

func TestTypeAliases(t *testing.T) {
	// Test that type aliases work correctly
	var msgType MessageType = "custom_type"
	assert.Equal(t, "custom_type", msgType)

	var msgAction MessageAction = "custom_action"
	assert.Equal(t, "custom_action", msgAction)

	var msgSource MessageSource = "custom_source"
	assert.Equal(t, "custom_source", msgSource)

	var requestID RequestID = "request-id"
	assert.Equal(t, "request-id", requestID)

	var RoomID RoomID = "channel-id"
	assert.Equal(t, "channel-id", RoomID)

	// Test that GenericMessage can hold different types
	var genericMsg GenericMessage

	genericMsg = RequestMessage{Action: "test"}
	assert.NotNil(t, genericMsg)

	genericMsg = ResponseMessage{Action: "test"}
	assert.NotNil(t, genericMsg)

	genericMsg = ErrorMessage{Action: "test"}
	assert.NotNil(t, genericMsg)

	genericMsg = EventMessage{Action: "test"}
	assert.NotNil(t, genericMsg)

	genericMsg = "string message"
	assert.NotNil(t, genericMsg)

	genericMsg = 42
	assert.NotNil(t, genericMsg)
}

func TestMessagePayload(t *testing.T) {
	// Test that MessagePayload can hold different types
	var payload MessagePayload

	payload = map[string]string{"key": "value"}
	assert.NotNil(t, payload)

	payload = "string payload"
	assert.NotNil(t, payload)

	payload = 123
	assert.NotNil(t, payload)

	payload = []string{"item1", "item2"}
	assert.NotNil(t, payload)

	payload = nil
	assert.Nil(t, payload)
}

func TestMessageStructuresWithComplexPayloads(t *testing.T) {
	t.Run("RequestMessage with nested payload", func(t *testing.T) {
		complexPayload := map[string]any{
			"nested": map[string]any{
				"field1": "value1",
				"field2": 42,
			},
			"array": []any{"item1", "item2", 3},
		}

		msg := RequestMessage{
			Action:    "complex_action",
			Payload:   complexPayload,
			Source:    SystemDevice,
			RequestID: "req-complex",
		}

		data, err := json.Marshal(msg)
		assert.NoError(t, err)

		var decoded RequestMessage
		err = json.Unmarshal(data, &decoded)
		assert.NoError(t, err)

		// Check that complex payload is preserved
		decodedPayload := decoded.Payload.(map[string]any)
		assert.NotNil(t, decodedPayload["nested"])
		assert.NotNil(t, decodedPayload["array"])
	})

	t.Run("ResponseMessage with array payload", func(t *testing.T) {
		arrayPayload := []map[string]string{
			{"id": "1", "name": "first"},
			{"id": "2", "name": "second"},
		}

		msg := ResponseMessage{
			Action:  "list_items",
			Payload: arrayPayload,
			Source:  SystemAPI,
			ReplyTo: "req-list",
		}

		data, err := json.Marshal(msg)
		assert.NoError(t, err)

		var decoded ResponseMessage
		err = json.Unmarshal(data, &decoded)
		assert.NoError(t, err)

		// Check that array payload is preserved
		decodedPayload := decoded.Payload.([]any)
		assert.Len(t, decodedPayload, 2)
	})
}

func TestSetMessageRoomID(t *testing.T) {
	roomID := RoomID("test-room-123")

	t.Run("EventMessage", func(t *testing.T) {
		msg := &EventMessage{Action: "test.action"}
		success := SetMessageRoomID(msg, roomID)
		assert.True(t, success)
		assert.Equal(t, roomID, msg.RoomID)
	})

	t.Run("RequestMessage", func(t *testing.T) {
		msg := &RequestMessage{Action: "test.action"}
		success := SetMessageRoomID(msg, roomID)
		assert.True(t, success)
		assert.Equal(t, string(roomID), msg.RoomID)
	})

	t.Run("ResponseMessage", func(t *testing.T) {
		msg := &ResponseMessage{Action: "test.action"}
		success := SetMessageRoomID(msg, roomID)
		assert.True(t, success)
		assert.Equal(t, roomID, msg.RoomID)
	})

	t.Run("ErrorMessage", func(t *testing.T) {
		msg := &ErrorMessage{Action: "test.action"}
		success := SetMessageRoomID(msg, roomID)
		assert.True(t, success)
		assert.Equal(t, roomID, msg.RoomID)
	})

	t.Run("RawJSONMap", func(t *testing.T) {
		msg := map[string]any{
			"action": "test.action",
			"type":   "request",
		}
		success := SetMessageRoomID(msg, roomID)
		assert.True(t, success)
		assert.Equal(t, string(roomID), msg["room_id"])
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		msg := "not a message"
		success := SetMessageRoomID(msg, roomID)
		assert.False(t, success)
	})

	t.Run("ValueReceiverDoesNotWork", func(t *testing.T) {
		// This demonstrates that value receivers won't work
		// because the interface requires a pointer
		msg := EventMessage{Action: "test.action"}
		success := SetMessageRoomID(msg, roomID) // Not a pointer
		assert.False(t, success)                 // Should fail because value doesn't implement MutableRoomMessage
	})
}

func TestMutableRoomMessageInterface(t *testing.T) {
	roomID := RoomID("test-room-456")

	t.Run("PointerImplementsInterface", func(t *testing.T) {
		msg := &EventMessage{Action: "test.action"}

		// Verify it implements the interface
		var _ MutableRoomMessage = msg

		msg.SetRoomID(roomID)
		assert.Equal(t, roomID, msg.GetRoomID())
	})

	t.Run("AllMessageTypesImplementInterface", func(t *testing.T) {
		// Verify all message types implement MutableRoomMessage
		var _ MutableRoomMessage = &EventMessage{}
		var _ MutableRoomMessage = &RequestMessage{}
		var _ MutableRoomMessage = &ResponseMessage{}
		var _ MutableRoomMessage = &ErrorMessage{}
	})
}
