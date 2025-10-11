package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestBuilder(t *testing.T) {
	t.Run("BuildsCompleteRequest", func(t *testing.T) {
		payload := map[string]any{"key": "value"}
		traceCtx := map[string]string{"traceparent": "00-123-456-01"}

		req := NewRequest("test.action").
			WithRequestID("req-123").
			WithSource(SystemDevice).
			WithDestination(DestinationAPI).
			WithPayload(payload).
			WithChannelID("channel-1").
			WithTraceContext(traceCtx).
			Build()

		assert.Equal(t, "test.action", req.Action)
		assert.Equal(t, "req-123", req.RequestID)
		assert.Equal(t, SystemDevice, req.Source)
		assert.Equal(t, MessageSource(DestinationAPI), req.Destination)
		assert.Equal(t, payload, req.Payload)
		assert.Equal(t, "channel-1", req.ChannelID)
		assert.Equal(t, traceCtx, req.TraceContext)
	})

	t.Run("AutoGeneratesRequestID", func(t *testing.T) {
		req := NewRequest("test.action").Build()
		assert.NotEmpty(t, req.RequestID)
	})

	t.Run("BuildsMinimalRequest", func(t *testing.T) {
		req := NewRequest("test.action").
			WithSource(SystemClient).
			WithDestination(DestinationDevice).
			Build()

		assert.Equal(t, "test.action", req.Action)
		assert.Equal(t, SystemClient, req.Source)
		assert.Equal(t, MessageSource(DestinationDevice), req.Destination)
		assert.NotEmpty(t, req.RequestID)
	})
}

func TestResponseBuilder(t *testing.T) {
	t.Run("BuildsCompleteResponse", func(t *testing.T) {
		payload := map[string]any{"result": "success"}
		traceCtx := map[string]string{"traceparent": "00-123-456-01"}

		resp := NewResponse("test.action").
			WithSource(SystemAPI).
			WithDestination(DestinationWeb).
			WithPayload(payload).
			WithReplyTo("req-123").
			WithChannelID("channel-1").
			WithTraceContext(traceCtx).
			Build()

		assert.Equal(t, "test.action", resp.Action)
		assert.Equal(t, SystemAPI, resp.Source)
		assert.Equal(t, DestinationWeb, resp.Destination)
		assert.Equal(t, payload, resp.Payload)
		assert.Equal(t, RequestID("req-123"), resp.ReplyTo)
		assert.Equal(t, ChannelID("channel-1"), resp.ChannelID)
		assert.Equal(t, traceCtx, resp.TraceContext)
	})

	t.Run("BuildsMinimalResponse", func(t *testing.T) {
		resp := NewResponse("test.action").
			WithSource(SystemDevice).
			WithDestination(DestinationWeb).
			WithReplyTo("req-123").
			Build()

		assert.Equal(t, "test.action", resp.Action)
		assert.Equal(t, SystemDevice, resp.Source)
		assert.Equal(t, DestinationWeb, resp.Destination)
		assert.Equal(t, RequestID("req-123"), resp.ReplyTo)
	})
}

func TestEventBuilder(t *testing.T) {
	t.Run("BuildsCompleteEvent", func(t *testing.T) {
		payload := map[string]any{"status": "active"}
		traceCtx := map[string]string{"traceparent": "00-123-456-01"}

		event := NewEvent("device.connection.active").
			WithSource(SystemDevice).
			WithDestination(DestinationWeb).
			WithPayload(payload).
			WithChannelID("channel-1").
			WithTraceContext(traceCtx).
			Build()

		assert.Equal(t, "device.connection.active", event.Action)
		assert.Equal(t, SystemDevice, event.Source)
		assert.Equal(t, DestinationWeb, event.Destination)
		assert.Equal(t, payload, event.Payload)
		assert.Equal(t, ChannelID("channel-1"), event.ChannelID)
		assert.Equal(t, traceCtx, event.TraceContext)
	})

	t.Run("BuildsMinimalEvent", func(t *testing.T) {
		event := NewEvent("system.ready").
			WithSource(SystemAPI).
			WithDestination(DestinationBroadcast).
			Build()

		assert.Equal(t, "system.ready", event.Action)
		assert.Equal(t, SystemAPI, event.Source)
		assert.Equal(t, DestinationBroadcast, event.Destination)
	})
}

func TestErrorBuilder(t *testing.T) {
	t.Run("BuildsCompleteError", func(t *testing.T) {
		details := map[string]any{"field": "value"}
		traceCtx := map[string]string{"traceparent": "00-123-456-01"}

		errMsg := NewError("test.action", "VALIDATION_ERROR", "Invalid request").
			WithSource(SystemAPI).
			WithDestination(DestinationDevice).
			WithDetails(details).
			WithReplyTo("req-123").
			WithChannelID("channel-1").
			WithTraceContext(traceCtx).
			Build()

		assert.Equal(t, "test.action", errMsg.Action)
		assert.Equal(t, SystemAPI, errMsg.Source)
		assert.Equal(t, DestinationDevice, errMsg.Destination)
		assert.Equal(t, "VALIDATION_ERROR", errMsg.Error.Code)
		assert.Equal(t, "Invalid request", errMsg.Error.Message)
		assert.Equal(t, details, errMsg.Error.Details)
		assert.Equal(t, RequestID("req-123"), errMsg.ReplyTo)
		assert.Equal(t, ChannelID("channel-1"), errMsg.ChannelID)
		assert.Equal(t, traceCtx, errMsg.TraceContext)
	})

	t.Run("BuildsMinimalError", func(t *testing.T) {
		errMsg := NewError("test.action", "ERROR", "Something failed").
			WithSource(SystemAPI).
			WithDestination(DestinationWeb).
			WithReplyTo("req-123").
			Build()

		assert.Equal(t, "test.action", errMsg.Action)
		assert.Equal(t, "ERROR", errMsg.Error.Code)
		assert.Equal(t, "Something failed", errMsg.Error.Message)
		assert.Equal(t, RequestID("req-123"), errMsg.ReplyTo)
	})
}

func TestMapBuilder(t *testing.T) {
	t.Run("BuildsCompleteMapMessage", func(t *testing.T) {
		msg := NewMapMessage(TypeRequest, "test.action").
			WithRequestID("req-123").
			WithSource(SystemClient).
			WithDestination(DestinationAPI).
			WithPayload(map[string]any{"key": "value"}).
			WithChannelID("channel-1").
			WithTimestamp(1234567890).
			WithTraceContext(map[string]string{"traceparent": "00-123-456-01"}).
			Build()

		assert.Equal(t, "request", msg["type"])
		assert.Equal(t, "test.action", msg["action"])
		assert.Equal(t, "req-123", msg["request_id"])
		assert.Equal(t, SystemClient, msg["source"])
		assert.Equal(t, DestinationAPI, msg["destination"])
		assert.Equal(t, "channel-1", msg["channel_id"])
		assert.Equal(t, int64(1234567890), msg["timestamp"])

		payload, ok := msg["payload"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "value", payload["key"])
	})

	t.Run("BuildsErrorMapMessage", func(t *testing.T) {
		msg := NewMapMessage(TypeError, "test.action").
			WithReplyTo("req-123").
			WithSource(SystemAPI).
			WithDestination(DestinationWeb).
			WithError("ERROR_CODE", "Error message", map[string]any{"detail": "info"}).
			Build()

		assert.Equal(t, "error", msg["type"])
		assert.Equal(t, "test.action", msg["action"])
		assert.Equal(t, "req-123", msg["reply_to"])

		errMap, ok := msg["error"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "ERROR_CODE", errMap["code"])
		assert.Equal(t, "Error message", errMap["message"])

		details, ok := errMap["details"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "info", details["detail"])
	})

	t.Run("BuildsWithCustomFields", func(t *testing.T) {
		msg := NewMapMessage(TypeEvent, "custom.event").
			WithSource(SystemDevice).
			WithDestination(DestinationBroadcast).
			WithField("custom_field", "custom_value").
			WithField("another_field", 42).
			Build()

		assert.Equal(t, "custom_value", msg["custom_field"])
		assert.Equal(t, 42, msg["another_field"])
	})
}

func TestConvenienceFunctions(t *testing.T) {
	t.Run("NewSFUPublisherOffer", func(t *testing.T) {
		req := NewSFUPublisherOffer("req-123", "sdp-content")

		assert.Equal(t, "api.sfu.publisher.offer", req.Action)
		assert.Equal(t, "req-123", req.RequestID)
		assert.Equal(t, SystemDevice, req.Source)
		assert.Equal(t, MessageSource(DestinationAPI), req.Destination)

		payload, ok := req.Payload.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "sdp-content", payload["sdp"])
	})

	t.Run("NewSFUSubscriberOffer", func(t *testing.T) {
		req := NewSFUSubscriberOffer("req-456", "sdp-content")

		assert.Equal(t, "api.sfu.subscriber.offer", req.Action)
		assert.Equal(t, "req-456", req.RequestID)
		assert.Equal(t, SystemClient, req.Source)
		assert.Equal(t, MessageSource(DestinationAPI), req.Destination)

		payload, ok := req.Payload.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "sdp-content", payload["sdp"])
	})

	t.Run("NewSFUPublisherICE", func(t *testing.T) {
		req := NewSFUPublisherICE("req-789", "candidate-string", "0", 0)

		assert.Equal(t, "api.sfu.publisher.ice", req.Action)
		assert.Equal(t, SystemDevice, req.Source)
		assert.Equal(t, MessageSource(DestinationAPI), req.Destination)

		payload, ok := req.Payload.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "candidate-string", payload["candidate"])
		assert.Equal(t, "0", payload["sdpMid"])
		assert.Equal(t, 0, payload["sdpMLineIndex"])
	})

	t.Run("NewSFUSubscriberICE", func(t *testing.T) {
		req := NewSFUSubscriberICE("req-101", "candidate-string", "1", 1)

		assert.Equal(t, "api.sfu.subscriber.ice", req.Action)
		assert.Equal(t, SystemClient, req.Source)
		assert.Equal(t, MessageSource(DestinationAPI), req.Destination)

		payload, ok := req.Payload.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "candidate-string", payload["candidate"])
		assert.Equal(t, "1", payload["sdpMid"])
		assert.Equal(t, 1, payload["sdpMLineIndex"])
	})

	t.Run("NewDeviceSessionReady", func(t *testing.T) {
		payload := map[string]any{"ready": true}
		event := NewDeviceSessionReady(payload)

		assert.Equal(t, "device.session.ready", event.Action)
		assert.Equal(t, SystemDevice, event.Source)
		assert.Equal(t, DestinationWeb, event.Destination)
		assert.Equal(t, payload, event.Payload)
	})

	t.Run("NewConnectionActiveEvent", func(t *testing.T) {
		event := NewConnectionActiveEvent(SystemDevice, DestinationWeb)

		assert.Equal(t, "device.connection.active", event.Action)
		assert.Equal(t, SystemDevice, event.Source)
		assert.Equal(t, DestinationWeb, event.Destination)
	})

	t.Run("NewConnectionInactiveEvent", func(t *testing.T) {
		event := NewConnectionInactiveEvent(SystemClient, DestinationDevice)

		assert.Equal(t, "client.connection.inactive", event.Action)
		assert.Equal(t, SystemClient, event.Source)
		assert.Equal(t, DestinationDevice, event.Destination)
	})

	t.Run("NewTimestampedMapRequest", func(t *testing.T) {
		payload := map[string]any{"data": "value"}
		msg := NewTimestampedMapRequest("test.action", "req-123", SystemClient, DestinationDevice, payload)

		assert.Equal(t, "request", msg["type"])
		assert.Equal(t, "test.action", msg["action"])
		assert.Equal(t, "req-123", msg["request_id"])
		assert.Equal(t, SystemClient, msg["source"])
		assert.Equal(t, DestinationDevice, msg["destination"])
		assert.NotNil(t, msg["timestamp"])
		assert.Equal(t, payload, msg["payload"])
	})

	t.Run("NewTimestampedMapResponse", func(t *testing.T) {
		payload := map[string]any{"result": "ok"}
		msg := NewTimestampedMapResponse("test.action", "req-123", SystemAPI, DestinationWeb, payload)

		assert.Equal(t, "response", msg["type"])
		assert.Equal(t, "test.action", msg["action"])
		assert.Equal(t, "req-123", msg["reply_to"])
		assert.Equal(t, SystemAPI, msg["source"])
		assert.Equal(t, DestinationWeb, msg["destination"])
		assert.NotNil(t, msg["timestamp"])
		assert.Equal(t, payload, msg["payload"])
	})
}

func TestBuilderChaining(t *testing.T) {
	t.Run("RequestBuilderChaining", func(t *testing.T) {
		// Verify that all methods return the builder for chaining
		builder := NewRequest("test.action")
		assert.Equal(t, builder, builder.WithRequestID("123"))
		assert.Equal(t, builder, builder.WithSource(SystemDevice))
		assert.Equal(t, builder, builder.WithDestination(DestinationAPI))
		assert.Equal(t, builder, builder.WithPayload(nil))
		assert.Equal(t, builder, builder.WithChannelID("ch-1"))
		assert.Equal(t, builder, builder.WithTraceContext(nil))
	})

	t.Run("ResponseBuilderChaining", func(t *testing.T) {
		builder := NewResponse("test.action")
		assert.Equal(t, builder, builder.WithSource(SystemAPI))
		assert.Equal(t, builder, builder.WithDestination(DestinationWeb))
		assert.Equal(t, builder, builder.WithPayload(nil))
		assert.Equal(t, builder, builder.WithReplyTo("123"))
		assert.Equal(t, builder, builder.WithChannelID("ch-1"))
		assert.Equal(t, builder, builder.WithTraceContext(nil))
	})

	t.Run("EventBuilderChaining", func(t *testing.T) {
		builder := NewEvent("test.event")
		assert.Equal(t, builder, builder.WithSource(SystemDevice))
		assert.Equal(t, builder, builder.WithDestination(DestinationBroadcast))
		assert.Equal(t, builder, builder.WithPayload(nil))
		assert.Equal(t, builder, builder.WithChannelID("ch-1"))
		assert.Equal(t, builder, builder.WithTraceContext(nil))
	})

	t.Run("ErrorBuilderChaining", func(t *testing.T) {
		builder := NewError("test.action", "CODE", "message")
		assert.Equal(t, builder, builder.WithSource(SystemAPI))
		assert.Equal(t, builder, builder.WithDestination(DestinationDevice))
		assert.Equal(t, builder, builder.WithDetails(nil))
		assert.Equal(t, builder, builder.WithReplyTo("123"))
		assert.Equal(t, builder, builder.WithChannelID("ch-1"))
		assert.Equal(t, builder, builder.WithTraceContext(nil))
	})

	t.Run("MapBuilderChaining", func(t *testing.T) {
		builder := NewMapMessage(TypeRequest, "test.action")
		assert.Equal(t, builder, builder.WithRequestID("123"))
		assert.Equal(t, builder, builder.WithReplyTo("456"))
		assert.Equal(t, builder, builder.WithSource(SystemClient))
		assert.Equal(t, builder, builder.WithDestination(DestinationAPI))
		assert.Equal(t, builder, builder.WithPayload(nil))
		assert.Equal(t, builder, builder.WithChannelID("ch-1"))
		assert.Equal(t, builder, builder.WithTimestamp(123))
		assert.Equal(t, builder, builder.WithError("CODE", "msg", nil))
		assert.Equal(t, builder, builder.WithTraceContext(nil))
		assert.Equal(t, builder, builder.WithField("key", "value"))
	})
}