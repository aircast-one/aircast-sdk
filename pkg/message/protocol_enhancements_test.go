package message

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test Retained Messages

func TestEventMessage_RetainedField(t *testing.T) {
	t.Run("retained field is included when true", func(t *testing.T) {
		msg := EventMessage{
			Action:      "test.event",
			Payload:     map[string]any{"data": "test"},
			Source:      SystemDevice,
			Destination: DestinationWeb,
			Retained:    true,
		}

		data, err := json.Marshal(msg)
		require.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		assert.Equal(t, true, result["retained"])
	})

	t.Run("retained field is omitted when false", func(t *testing.T) {
		msg := EventMessage{
			Action:      "test.event",
			Payload:     map[string]any{"data": "test"},
			Source:      SystemDevice,
			Destination: DestinationWeb,
			Retained:    false,
		}

		data, err := json.Marshal(msg)
		require.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		_, exists := result["retained"]
		assert.False(t, exists, "retained field should be omitted when false")
	})

	t.Run("retained field is correctly unmarshaled", func(t *testing.T) {
		jsonData := `{
			"action": "test.event",
			"payload": {"data": "test"},
			"source": "device",
			"destination": "web",
			"retained": true
		}`

		var msg EventMessage
		err := json.Unmarshal([]byte(jsonData), &msg)
		require.NoError(t, err)

		assert.True(t, msg.Retained)
	})
}

func TestEventMessage_RetainedUseCases(t *testing.T) {
	t.Run("battery telemetry should be retained", func(t *testing.T) {
		msg := EventMessage{
			Action:      "mavlink.telemetry.battery",
			Payload:     map[string]any{"level": 85, "voltage": 12.4},
			Source:      SystemDevice,
			Destination: DestinationWeb,
			Retained:    true,
		}

		assert.True(t, msg.Retained, "Battery telemetry should be retained")
	})

	t.Run("streaming data should not be retained", func(t *testing.T) {
		msg := EventMessage{
			Action:      "mavlink.telemetry.gps.stream",
			Payload:     map[string]any{"lat": 40.7128, "lon": -74.0060},
			Source:      SystemDevice,
			Destination: DestinationWeb,
			Retained:    false,
		}

		assert.False(t, msg.Retained, "Streaming data should not be retained")
	})
}

// Test Will Messages

func TestWillMessage_Structure(t *testing.T) {
	t.Run("marshal and unmarshal", func(t *testing.T) {
		will := WillMessage{
			Action:      "device.status",
			Payload:     map[string]any{"status": "offline", "reason": "connection_lost"},
			Destination: DestinationBroadcast,
		}

		data, err := json.Marshal(will)
		require.NoError(t, err)

		var unmarshaled WillMessage
		err = json.Unmarshal(data, &unmarshaled)
		require.NoError(t, err)

		assert.Equal(t, will.Action, unmarshaled.Action)
		assert.Equal(t, will.Destination, unmarshaled.Destination)
		assert.NotNil(t, unmarshaled.Payload)
	})

	t.Run("omits empty payload", func(t *testing.T) {
		will := WillMessage{
			Action:      "device.status",
			Destination: DestinationWeb,
		}

		data, err := json.Marshal(will)
		require.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		_, exists := result["payload"]
		assert.False(t, exists, "payload should be omitted when empty")
	})
}

func TestWillMessage_WithTraceContext(t *testing.T) {
	will := WillMessage{
		Action:      "device.offline",
		Payload:     map[string]any{"reason": "crash"},
		Destination: DestinationBroadcast,
		TraceContext: map[string]string{
			"traceparent": "00-trace-id-span-id-00",
		},
	}

	data, err := json.Marshal(will)
	require.NoError(t, err)

	var unmarshaled WillMessage
	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)

	assert.NotNil(t, unmarshaled.TraceContext)
	assert.Equal(t, "00-trace-id-span-id-00", unmarshaled.TraceContext["traceparent"])
}

func TestUnmarshalMessage_WillType(t *testing.T) {
	t.Run("parses will message correctly", func(t *testing.T) {
		jsonData := `{
			"type": "will",
			"action": "device.status",
			"payload": {"status": "offline"},
			"destination": "broadcast"
		}`

		msg, err := UnmarshalMessage([]byte(jsonData))
		require.NoError(t, err)

		will, ok := msg.(WillMessage)
		require.True(t, ok, "should unmarshal to WillMessage")
		assert.Equal(t, "device.status", string(will.Action))
		assert.Equal(t, DestinationBroadcast, will.Destination)
	})

	t.Run("validates action field", func(t *testing.T) {
		jsonData := `{
			"type": "will",
			"destination": "broadcast"
		}`

		_, err := UnmarshalMessage([]byte(jsonData))
		assert.Error(t, err, "should error on missing action")
	})
}

// Test Session Config

func TestSessionConfig_Structure(t *testing.T) {
	t.Run("marshal and unmarshal", func(t *testing.T) {
		config := SessionConfig{
			ClientID:     "device-123-abc",
			CleanSession: false,
		}

		data, err := json.Marshal(config)
		require.NoError(t, err)

		var unmarshaled SessionConfig
		err = json.Unmarshal(data, &unmarshaled)
		require.NoError(t, err)

		assert.Equal(t, config.ClientID, unmarshaled.ClientID)
		assert.Equal(t, config.CleanSession, unmarshaled.CleanSession)
	})

	t.Run("persistent session config", func(t *testing.T) {
		config := SessionConfig{
			ClientID:     "device-persistent-id",
			CleanSession: false,
		}

		assert.False(t, config.CleanSession, "persistent session should have cleanSession=false")
		assert.NotEmpty(t, config.ClientID, "persistent session must have clientID")
	})

	t.Run("clean session config", func(t *testing.T) {
		config := SessionConfig{
			ClientID:     "device-temp-id",
			CleanSession: true,
		}

		assert.True(t, config.CleanSession, "clean session should have cleanSession=true")
	})
}

// Test Protocol Validation

func TestValidateMessage_WillType(t *testing.T) {
	t.Run("accepts valid will message", func(t *testing.T) {
		msg := map[string]any{
			"type":        TypeWill,
			"action":      "device.status",
			"destination": "broadcast",
		}

		err := validateMessage(msg)
		assert.NoError(t, err, "valid will message should pass validation")
	})

	t.Run("rejects will without action", func(t *testing.T) {
		msg := map[string]any{
			"type":        TypeWill,
			"destination": "broadcast",
		}

		err := validateMessage(msg)
		assert.Error(t, err, "will message without action should fail validation")
	})
}

// Integration Tests

func TestProtocolEnhancements_Integration(t *testing.T) {
	t.Run("retained event message round trip", func(t *testing.T) {
		original := EventMessage{
			Action:      "test.retained",
			Payload:     map[string]any{"value": 42},
			Source:      SystemDevice,
			Destination: DestinationWeb,
			Retained:    true,
		}

		// Wrap in envelope
		envelope := struct {
			Type string `json:"type"`
			EventMessage
		}{
			Type:         TypeEvent,
			EventMessage: original,
		}

		data, err := json.Marshal(envelope)
		require.NoError(t, err)

		// Parse back
		parsed, err := UnmarshalMessage(data)
		require.NoError(t, err)

		event, ok := parsed.(EventMessage)
		require.True(t, ok)
		assert.Equal(t, original.Action, event.Action)
		assert.Equal(t, original.Retained, event.Retained)
	})

	t.Run("will message round trip", func(t *testing.T) {
		original := WillMessage{
			Action:      "device.offline",
			Payload:     map[string]any{"reason": "crash"},
			Destination: DestinationBroadcast,
		}

		// Wrap in envelope
		envelope := struct {
			Type string `json:"type"`
			WillMessage
		}{
			Type:        TypeWill,
			WillMessage: original,
		}

		data, err := json.Marshal(envelope)
		require.NoError(t, err)

		// Parse back
		parsed, err := UnmarshalMessage(data)
		require.NoError(t, err)

		will, ok := parsed.(WillMessage)
		require.True(t, ok)
		assert.Equal(t, original.Action, will.Action)
		assert.Equal(t, original.Destination, will.Destination)
	})
}

// Backward Compatibility Tests

func TestProtocolEnhancements_BackwardCompatibility(t *testing.T) {
	t.Run("old event messages without retained field still work", func(t *testing.T) {
		jsonData := `{
			"type": "event",
			"action": "test.event",
			"source": "device",
			"destination": "web"
		}`

		msg, err := UnmarshalMessage([]byte(jsonData))
		require.NoError(t, err)

		event, ok := msg.(EventMessage)
		require.True(t, ok)
		assert.False(t, event.Retained, "retained should default to false")
	})

	t.Run("all message types still parse correctly", func(t *testing.T) {
		testCases := []struct {
			name        string
			messageType string
			json        string
		}{
			{
				name:        "request",
				messageType: TypeRequest,
				json:        `{"type": "request", "action": "test", "request_id": "123", "source": "web", "destination": "device"}`,
			},
			{
				name:        "response",
				messageType: TypeResponse,
				json:        `{"type": "response", "action": "test", "reply_to": "123", "source": "device", "destination": "web"}`,
			},
			{
				name:        "error",
				messageType: TypeError,
				json:        `{"type": "error", "action": "test", "reply_to": "123", "error": {"code": "ERR", "message": "error"}, "source": "api", "destination": "web"}`,
			},
			{
				name:        "event",
				messageType: TypeEvent,
				json:        `{"type": "event", "action": "test", "source": "device", "destination": "web"}`,
			},
			{
				name:        "will",
				messageType: TypeWill,
				json:        `{"type": "will", "action": "device.offline", "destination": "broadcast"}`,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				msg, err := UnmarshalMessage([]byte(tc.json))
				require.NoError(t, err, "should parse %s message", tc.messageType)
				assert.NotNil(t, msg)
			})
		}
	})
}
