package message

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalMessage(t *testing.T) {
	t.Run("unmarshal RequestMessage", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "get_device",
			"source": "api",
			"destination": "device",
			"request_id": "req-123",
			"room_id": "channel-456",
			"payload": {"device_id": "device-789"}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		req, ok := msg.(*RequestMessage)
		require.True(t, ok)

		assert.Equal(t, "get_device", req.Action)
		assert.Equal(t, SystemAPI, req.Source)
		assert.Equal(t, "req-123", req.RequestID)
		assert.Equal(t, "channel-456", req.RoomID)
		assert.NotNil(t, req.Payload)
	})

	t.Run("unmarshal ResponseMessage", func(t *testing.T) {
		data := `{
			"type": "response",
			"action": "get_device",
			"source": "device",
			"destination": "web",
			"room_id": "channel-456",
			"reply_to": "req-123",
			"payload": {"status": "success"}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		resp, ok := msg.(*ResponseMessage)
		require.True(t, ok)

		assert.Equal(t, "get_device", resp.Action)
		assert.Equal(t, SystemDevice, resp.Source)
		assert.Equal(t, "channel-456", resp.RoomID)
		assert.Equal(t, "req-123", resp.ReplyTo)
		assert.NotNil(t, resp.Payload)
	})

	t.Run("unmarshal ErrorMessage", func(t *testing.T) {
		data := `{
			"type": "error",
			"action": "get_device",
			"source": "device",
			"destination": "web",
			"room_id": "channel-456",
			"reply_to": "req-123",
			"error": {
				"code": "DEVICE_NOT_FOUND",
				"message": "Device not found",
				"details": {"device_id": "device-789"}
			}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		errMsg, ok := msg.(*ErrorMessage)
		require.True(t, ok)

		assert.Equal(t, "get_device", errMsg.Action)
		assert.Equal(t, SystemDevice, errMsg.Source)
		assert.Equal(t, "channel-456", errMsg.RoomID)
		assert.Equal(t, "req-123", errMsg.ReplyTo)
		assert.Equal(t, "DEVICE_NOT_FOUND", errMsg.Error.Code)
		assert.Equal(t, "Device not found", errMsg.Error.Message)
		assert.NotNil(t, errMsg.Error.Details)
	})

	t.Run("unmarshal EventMessage", func(t *testing.T) {
		data := `{
			"type": "event",
			"action": "device_connected",
			"source": "device",
			"destination": "web",
			"room_id": "channel-456",
			"payload": {"device_id": "device-789", "timestamp": "2023-01-01T00:00:00Z"}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		event, ok := msg.(*EventMessage)
		require.True(t, ok)

		assert.Equal(t, "device_connected", event.Action)
		assert.Equal(t, SystemDevice, event.Source)
		assert.Equal(t, "channel-456", event.RoomID)
		assert.NotNil(t, event.Payload)
	})

	t.Run("invalid JSON", func(t *testing.T) {
		data := `{invalid json}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "failed to parse generic message")
	})

	t.Run("missing type field", func(t *testing.T) {
		data := `{
			"action": "test_action",
			"source": "device"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "invalid message type field")
	})

	t.Run("invalid type field", func(t *testing.T) {
		data := `{
			"type": 123,
			"action": "test_action",
			"source": "device"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "invalid message type field")
	})

	t.Run("unknown message type", func(t *testing.T) {
		data := `{
			"type": "unknown_type",
			"action": "test_action",
			"source": "device"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "unknown message type")
	})

	t.Run("missing action field", func(t *testing.T) {
		data := `{
			"type": "request",
			"source": "device",
			"request_id": "req-123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "missing")
	})

	t.Run("request missing request_id", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "test_action",
			"source": "device"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "missing required 'request_id' field")
	})

	t.Run("response missing reply_to", func(t *testing.T) {
		data := `{
			"type": "response",
			"action": "test_action",
			"source": "device"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "reply_to")
	})

	t.Run("error missing reply_to", func(t *testing.T) {
		data := `{
			"type": "error",
			"action": "test_action",
			"source": "device",
			"error": {"code": "TEST", "message": "test"}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "reply_to")
	})

	t.Run("error missing error field", func(t *testing.T) {
		data := `{
			"type": "error",
			"action": "test_action",
			"source": "device",
			"reply_to": "req-123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "error must include 'error' field")
	})

	t.Run("minimal valid request", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "test",
			"source": "api",
			"destination": "device",
			"request_id": "123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		req, ok := msg.(*RequestMessage)
		require.True(t, ok)
		assert.Equal(t, "test", req.Action)
		assert.Equal(t, "api", req.Source)
		assert.Equal(t, "123", req.RequestID)
		assert.Empty(t, req.RoomID)
		assert.Nil(t, req.Payload)
	})

	t.Run("minimal valid response", func(t *testing.T) {
		data := `{
			"type": "response",
			"action": "test",
			"source": "device",
			"destination": "web",
			"reply_to": "123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		resp, ok := msg.(*ResponseMessage)
		require.True(t, ok)
		assert.Equal(t, "test", resp.Action)
		assert.Equal(t, "device", resp.Source)
		assert.Equal(t, "123", resp.ReplyTo)
		assert.Empty(t, resp.RoomID)
		assert.Nil(t, resp.Payload)
	})

	t.Run("minimal valid error", func(t *testing.T) {
		data := `{
			"type": "error",
			"action": "test",
			"source": "api",
			"destination": "web",
			"reply_to": "123",
			"error": {"code": "ERR", "message": "error"}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		errMsg, ok := msg.(*ErrorMessage)
		require.True(t, ok)
		assert.Equal(t, "test", errMsg.Action)
		assert.Equal(t, "api", errMsg.Source)
		assert.Equal(t, "123", errMsg.ReplyTo)
		assert.Equal(t, "ERR", errMsg.Error.Code)
		assert.Equal(t, "error", errMsg.Error.Message)
		assert.Empty(t, errMsg.RoomID)
	})

	t.Run("minimal valid event", func(t *testing.T) {
		data := `{
			"type": "event",
			"action": "test",
			"source": "device",
			"destination": "web"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		event, ok := msg.(*EventMessage)
		require.True(t, ok)
		assert.Equal(t, "test", event.Action)
		assert.Equal(t, "device", event.Source)
		assert.Empty(t, event.RoomID)
		assert.Nil(t, event.Payload)
	})
}

func TestValidateMessage(t *testing.T) {
	t.Run("valid request message", func(t *testing.T) {
		msg := map[string]any{
			"type":       TypeRequest,
			"action":     "test_action",
			"source":     SystemDevice,
			"request_id": "req-123",
		}

		err := validateMessage(msg)
		assert.NoError(t, err)
	})

	t.Run("valid response message", func(t *testing.T) {
		msg := map[string]any{
			"type":     TypeResponse,
			"action":   "test_action",
			"source":   SystemAPI,
			"reply_to": "req-123",
		}

		err := validateMessage(msg)
		assert.NoError(t, err)
	})

	t.Run("valid error message", func(t *testing.T) {
		msg := map[string]any{
			"type":     TypeError,
			"action":   "test_action",
			"source":   SystemDevice,
			"reply_to": "req-123",
			"error": map[string]any{
				"code":    "ERR_CODE",
				"message": "Error message",
			},
		}

		err := validateMessage(msg)
		assert.NoError(t, err)
	})

	t.Run("valid event message", func(t *testing.T) {
		msg := map[string]any{
			"type":   TypeEvent,
			"action": "test_event",
			"source": SystemAPI,
		}

		err := validateMessage(msg)
		assert.NoError(t, err)
	})

	t.Run("missing type", func(t *testing.T) {
		msg := map[string]any{
			"action": "test_action",
			"source": SystemDevice,
		}

		err := validateMessage(msg)
		assert.Error(t, err)
		assert.Equal(t, ErrMissingType, err)
	})

	t.Run("type not string", func(t *testing.T) {
		msg := map[string]any{
			"type":   123,
			"action": "test_action",
			"source": SystemDevice,
		}

		err := validateMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "type must be a string")
	})

	t.Run("invalid message type", func(t *testing.T) {
		msg := map[string]any{
			"type":   "invalid_type",
			"action": "test_action",
			"source": SystemDevice,
		}

		err := validateMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a valid message type")
	})

	t.Run("missing action", func(t *testing.T) {
		msg := map[string]any{
			"type":   TypeRequest,
			"source": SystemDevice,
		}

		err := validateMessage(msg)
		assert.Error(t, err)
		assert.Equal(t, ErrMissingAction, err)
	})

	t.Run("request missing request_id", func(t *testing.T) {
		msg := map[string]any{
			"type":   TypeRequest,
			"action": "test_action",
			"source": SystemDevice,
		}

		err := validateMessage(msg)
		assert.Error(t, err)
		assert.Equal(t, ErrMissingRequestID, err)
	})

	t.Run("response missing reply_to", func(t *testing.T) {
		msg := map[string]any{
			"type":   TypeResponse,
			"action": "test_action",
			"source": SystemAPI,
		}

		err := validateMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "response must include 'reply_to' field")
	})

	t.Run("error missing reply_to", func(t *testing.T) {
		msg := map[string]any{
			"type":   TypeError,
			"action": "test_action",
			"source": SystemDevice,
			"error": map[string]any{
				"code":    "ERR",
				"message": "error",
			},
		}

		err := validateMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error must include 'reply_to' field")
	})

	t.Run("error missing error field", func(t *testing.T) {
		msg := map[string]any{
			"type":     TypeError,
			"action":   "test_action",
			"source":   SystemDevice,
			"reply_to": "req-123",
		}

		err := validateMessage(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error must include 'error' field")
	})
}

func TestUnmarshalMessageEdgeCases(t *testing.T) {
	t.Run("empty JSON object", func(t *testing.T) {
		data := `{}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "invalid message type field")
	})

	t.Run("empty byte array", func(t *testing.T) {
		msg, err := UnmarshalMessage([]byte{})
		assert.Error(t, err)
		assert.Nil(t, msg)
	})

	t.Run("null byte array", func(t *testing.T) {
		msg, err := UnmarshalMessage(nil)
		assert.Error(t, err)
		assert.Nil(t, msg)
	})

	t.Run("JSON array instead of object", func(t *testing.T) {
		data := `["not", "an", "object"]`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
	})

	t.Run("complex nested payload", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "complex_action",
			"source": "api",
			"destination": "device",
			"request_id": "req-complex",
			"payload": {
				"nested": {
					"deep": {
						"field": "value",
						"number": 42,
						"boolean": true,
						"null": null,
						"array": [1, 2, 3]
					}
				},
				"list": ["a", "b", "c"]
			}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		req, ok := msg.(*RequestMessage)
		require.True(t, ok)
		assert.NotNil(t, req.Payload)

		// Verify the complex structure is preserved
		payload := req.Payload.(map[string]any)
		nested := payload["nested"].(map[string]any)
		deep := nested["deep"].(map[string]any)
		assert.Equal(t, "value", deep["field"])
		assert.Equal(t, float64(42), deep["number"])
		assert.Equal(t, true, deep["boolean"])
		assert.Nil(t, deep["null"])

		list := payload["list"].([]any)
		assert.Len(t, list, 3)
	})

	t.Run("very large payload", func(t *testing.T) {
		// Create a large payload
		largeData := make(map[string]string)
		for i := 0; i < 1000; i++ {
			largeData[string(rune(i))] = "value"
		}

		msg := map[string]any{
			"type":        TypeRequest,
			"action":      "large_action",
			"source":      SystemDevice,
			"destination": DestinationAPI,
			"request_id":  "req-large",
			"payload":     largeData,
		}

		data, err := json.Marshal(msg)
		require.NoError(t, err)

		parsedMsg, err := UnmarshalMessage(data)
		assert.NoError(t, err)
		assert.NotNil(t, parsedMsg)

		req, ok := parsedMsg.(*RequestMessage)
		require.True(t, ok)
		assert.NotNil(t, req.Payload)
	})

	t.Run("special characters in strings", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "test\naction\twith\rspecial\"characters",
			"source": "device",
			"destination": "api",
			"request_id": "req-123",
			"payload": {"field": "value with 'quotes' and \"double quotes\""}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		req, ok := msg.(*RequestMessage)
		require.True(t, ok)
		assert.Contains(t, req.Action, "special")
	})

	t.Run("unicode characters", func(t *testing.T) {
		data := `{
			"type": "event",
			"action": "unicode_test",
			"source": "api",
			"destination": "web",
			"payload": {"emoji": "🚀", "chinese": "你好", "arabic": "مرحبا"}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.NoError(t, err)
		require.NotNil(t, msg)

		event, ok := msg.(*EventMessage)
		require.True(t, ok)
		assert.NotNil(t, event.Payload)

		payload := event.Payload.(map[string]any)
		assert.Equal(t, "🚀", payload["emoji"])
		assert.Equal(t, "你好", payload["chinese"])
		assert.Equal(t, "مرحبا", payload["arabic"])
	})
}

func TestMessageValidation(t *testing.T) {
	t.Run("request with invalid source", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "test",
			"source": "invalid_source",
			"destination": "device",
			"request_id": "123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "not a valid source")
	})

	t.Run("request with missing source", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "test",
			"destination": "device",
			"request_id": "123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "missing required 'source' field")
	})

	t.Run("request with invalid destination", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "test",
			"source": "api",
			"destination": "invalid_dest",
			"request_id": "123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "not a valid destination")
	})

	t.Run("request with missing destination", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "test",
			"source": "api",
			"request_id": "123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "missing required 'destination' field")
	})

	t.Run("request with empty action", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "",
			"source": "api",
			"destination": "device",
			"request_id": "123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "missing required 'action' field")
	})

	t.Run("request with empty request_id", func(t *testing.T) {
		data := `{
			"type": "request",
			"action": "test",
			"source": "api",
			"destination": "device",
			"request_id": ""
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "missing required 'request_id' field")
	})

	t.Run("response with invalid source", func(t *testing.T) {
		data := `{
			"type": "response",
			"action": "test",
			"source": "bad_source",
			"destination": "web",
			"reply_to": "123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "not a valid source")
	})

	t.Run("response with missing destination", func(t *testing.T) {
		data := `{
			"type": "response",
			"action": "test",
			"source": "device",
			"reply_to": "123"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "missing required 'destination' field")
	})

	t.Run("response with empty reply_to", func(t *testing.T) {
		data := `{
			"type": "response",
			"action": "test",
			"source": "device",
			"destination": "web",
			"reply_to": ""
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "reply_to")
	})

	t.Run("error with missing error.code", func(t *testing.T) {
		data := `{
			"type": "error",
			"action": "test",
			"source": "device",
			"destination": "web",
			"reply_to": "123",
			"error": {"message": "error message"}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "error.code")
	})

	t.Run("error with empty error.code", func(t *testing.T) {
		data := `{
			"type": "error",
			"action": "test",
			"source": "device",
			"destination": "web",
			"reply_to": "123",
			"error": {"code": "", "message": "error message"}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "error.code")
	})

	t.Run("error with missing error.message", func(t *testing.T) {
		data := `{
			"type": "error",
			"action": "test",
			"source": "device",
			"destination": "web",
			"reply_to": "123",
			"error": {"code": "ERR_CODE"}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "error.message")
	})

	t.Run("error with empty error.message", func(t *testing.T) {
		data := `{
			"type": "error",
			"action": "test",
			"source": "device",
			"destination": "web",
			"reply_to": "123",
			"error": {"code": "ERR_CODE", "message": ""}
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "error.message")
	})

	t.Run("error with invalid error field type", func(t *testing.T) {
		data := `{
			"type": "error",
			"action": "test",
			"source": "device",
			"destination": "web",
			"reply_to": "123",
			"error": "not an object"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "error must include 'error' field as an object")
	})

	t.Run("event with missing source", func(t *testing.T) {
		data := `{
			"type": "event",
			"action": "test",
			"destination": "web"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "missing required 'source' field")
	})

	t.Run("event with missing destination", func(t *testing.T) {
		data := `{
			"type": "event",
			"action": "test",
			"source": "device"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "missing required 'destination' field")
	})

	t.Run("will with missing destination", func(t *testing.T) {
		data := `{
			"type": "will",
			"action": "disconnect"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "missing required 'destination' field")
	})

	t.Run("will with invalid destination", func(t *testing.T) {
		data := `{
			"type": "will",
			"action": "disconnect",
			"destination": "invalid"
		}`

		msg, err := UnmarshalMessage([]byte(data))
		assert.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "not a valid destination")
	})

	t.Run("valid request with all destinations", func(t *testing.T) {
		destinations := []string{DestinationWeb, DestinationAPI, DestinationDevice, DestinationBroadcast}

		for _, dest := range destinations {
			data := `{
				"type": "request",
				"action": "test",
				"source": "api",
				"destination": "` + dest + `",
				"request_id": "123"
			}`

			msg, err := UnmarshalMessage([]byte(data))
			assert.NoError(t, err, "destination %s should be valid", dest)
			require.NotNil(t, msg)

			req, ok := msg.(*RequestMessage)
			require.True(t, ok)
			assert.Equal(t, dest, req.Destination)
		}
	})

	t.Run("valid request with all sources", func(t *testing.T) {
		sources := []string{SystemDevice, SystemAPI, SystemWeb}

		for _, src := range sources {
			data := `{
				"type": "request",
				"action": "test",
				"source": "` + src + `",
				"destination": "device",
				"request_id": "123"
			}`

			msg, err := UnmarshalMessage([]byte(data))
			assert.NoError(t, err, "source %s should be valid", src)
			require.NotNil(t, msg)

			req, ok := msg.(*RequestMessage)
			require.True(t, ok)
			assert.Equal(t, src, req.Source)
		}
	})
}
