package message

import (
	"encoding/json"
	"testing"
)

// Benchmark parsing different message types
func BenchmarkUnmarshalMessage(b *testing.B) {
	requestData := []byte(`{
		"type": "request",
		"action": "get_device",
		"source": "api",
		"request_id": "req-123",
		"channel_id": "channel-456",
		"payload": {"device_id": "device-789", "status": "active", "metadata": {"key": "value"}}
	}`)

	responseData := []byte(`{
		"type": "response",
		"action": "get_device",
		"source": "device",
		"channel_id": "channel-456",
		"reply_to": "req-123",
		"payload": {"status": "success", "data": {"id": "123", "name": "test"}}
	}`)

	errorData := []byte(`{
		"type": "error",
		"action": "get_device",
		"source": "device",
		"channel_id": "channel-456",
		"reply_to": "req-123",
		"error": {"code": "DEVICE_NOT_FOUND", "message": "Device not found", "details": {"id": "789"}}
	}`)

	eventData := []byte(`{
		"type": "event",
		"action": "device_connected",
		"source": "device",
		"channel_id": "channel-456",
		"payload": {"device_id": "device-789", "timestamp": "2023-01-01T00:00:00Z"}
	}`)

	b.Run("Request", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := UnmarshalMessage(requestData)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Response", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := UnmarshalMessage(responseData)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Error", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			_, err := UnmarshalMessage(errorData)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Event", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := UnmarshalMessage(eventData)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark validation separately
func BenchmarkValidateMessage(b *testing.B) {
	validRequest := map[string]any{
		"type":       TypeRequest,
		"action":     "test_action",
		"source":     SystemDevice,
		"request_id": "req-123",
	}

	validResponse := map[string]any{
		"type":     TypeResponse,
		"action":   "test_action",
		"source":   SystemAPI,
		"reply_to": "req-123",
	}

	validError := map[string]any{
		"type":     TypeError,
		"action":   "test_action",
		"source":   SystemDevice,
		"reply_to": "req-123",
		"error": map[string]any{
			"code":    "ERR_CODE",
			"message": "Error message",
		},
	}

	validEvent := map[string]any{
		"type":   TypeEvent,
		"action": "test_event",
		"source": SystemAPI,
	}

	b.Run("Request", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = validateMessage(validRequest)
		}
	})

	b.Run("Response", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = validateMessage(validResponse)
		}
	})

	b.Run("Error", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = validateMessage(validError)
		}
	})

	b.Run("Event", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = validateMessage(validEvent)
		}
	})
}

// Benchmark parsing with different payload sizes
func BenchmarkUnmarshalMessagePayloadSize(b *testing.B) {
	smallPayload := map[string]string{"key": "value"}
	mediumPayload := make(map[string]string)
	for i := range 100 {
		mediumPayload[string(rune('a'+i%26))+string(rune(i))] = "value" + string(rune(i))
	}

	largePayload := make(map[string]any)
	for i := range 1000 {
		largePayload["key"+string(rune(i))] = map[string]any{
			"nested": "value",
			"index":  i,
			"data":   "lorem ipsum dolor sit amet consectetur adipiscing elit",
		}
	}

	b.Run("Small", func(b *testing.B) {
		msg := map[string]any{
			"type":       TypeRequest,
			"action":     "test",
			"source":     SystemDevice,
			"request_id": "req-123",
			"payload":    smallPayload,
		}
		data, _ := json.Marshal(msg)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := UnmarshalMessage(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Medium", func(b *testing.B) {
		msg := map[string]any{
			"type":       TypeRequest,
			"action":     "test",
			"source":     SystemDevice,
			"request_id": "req-123",
			"payload":    mediumPayload,
		}
		data, _ := json.Marshal(msg)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := UnmarshalMessage(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Large", func(b *testing.B) {
		msg := map[string]any{
			"type":       TypeRequest,
			"action":     "test",
			"source":     SystemDevice,
			"request_id": "req-123",
			"payload":    largePayload,
		}
		data, _ := json.Marshal(msg)

		b.ResetTimer()
		for b.Loop() {
			_, err := UnmarshalMessage(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark parallel parsing
func BenchmarkUnmarshalMessageParallel(b *testing.B) {
	data := []byte(`{
		"type": "request",
		"action": "test_action",
		"source": "device",
		"request_id": "req-123",
		"payload": {"key": "value", "status": "active"}
	}`)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := UnmarshalMessage(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark memory allocations
func BenchmarkUnmarshalMessageAllocs(b *testing.B) {
	data := []byte(`{
		"type": "request",
		"action": "test_action",
		"source": "device",
		"request_id": "req-123",
		"channel_id": "channel-456",
		"payload": {"device_id": "device-789", "status": "active"}
	}`)

	b.ReportAllocs()

	for b.Loop() {
		_, _ = UnmarshalMessage(data)
	}
}
