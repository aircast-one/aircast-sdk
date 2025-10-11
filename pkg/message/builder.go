package message

import (
	"time"

	"github.com/google/uuid"
)

// RequestBuilder provides a fluent API for building RequestMessage
type RequestBuilder struct {
	msg RequestMessage
}

// NewRequest creates a new request builder with the given action
func NewRequest(action MessageAction) *RequestBuilder {
	return &RequestBuilder{
		msg: RequestMessage{
			Action:    action,
			RequestID: uuid.New().String(),
		},
	}
}

// WithRequestID sets a specific request ID
func (b *RequestBuilder) WithRequestID(requestID string) *RequestBuilder {
	b.msg.RequestID = requestID
	return b
}

// WithSource sets the message source
func (b *RequestBuilder) WithSource(source MessageSource) *RequestBuilder {
	b.msg.Source = source
	return b
}

// WithDestination sets the message destination
func (b *RequestBuilder) WithDestination(destination MessageDestination) *RequestBuilder {
	b.msg.Destination = MessageSource(destination)
	return b
}

// WithPayload sets the message payload
func (b *RequestBuilder) WithPayload(payload any) *RequestBuilder {
	b.msg.Payload = payload
	return b
}

// WithChannelID sets the channel ID
func (b *RequestBuilder) WithChannelID(channelID string) *RequestBuilder {
	b.msg.ChannelID = channelID
	return b
}

// WithTraceContext sets the W3C trace context
func (b *RequestBuilder) WithTraceContext(traceContext map[string]string) *RequestBuilder {
	b.msg.TraceContext = traceContext
	return b
}

// Build returns the constructed RequestMessage
func (b *RequestBuilder) Build() RequestMessage {
	return b.msg
}

// ResponseBuilder provides a fluent API for building ResponseMessage
type ResponseBuilder struct {
	msg ResponseMessage
}

// NewResponse creates a new response builder with the given action
func NewResponse(action MessageAction) *ResponseBuilder {
	return &ResponseBuilder{
		msg: ResponseMessage{
			Action: action,
		},
	}
}

// WithSource sets the message source
func (b *ResponseBuilder) WithSource(source MessageSource) *ResponseBuilder {
	b.msg.Source = source
	return b
}

// WithDestination sets the message destination
func (b *ResponseBuilder) WithDestination(destination MessageDestination) *ResponseBuilder {
	b.msg.Destination = destination
	return b
}

// WithPayload sets the message payload
func (b *ResponseBuilder) WithPayload(payload any) *ResponseBuilder {
	b.msg.Payload = payload
	return b
}

// WithReplyTo sets the request ID this response is replying to
func (b *ResponseBuilder) WithReplyTo(replyTo string) *ResponseBuilder {
	b.msg.ReplyTo = replyTo
	return b
}

// WithChannelID sets the channel ID
func (b *ResponseBuilder) WithChannelID(channelID string) *ResponseBuilder {
	b.msg.ChannelID = channelID
	return b
}

// WithTraceContext sets the W3C trace context
func (b *ResponseBuilder) WithTraceContext(traceContext map[string]string) *ResponseBuilder {
	b.msg.TraceContext = traceContext
	return b
}

// Build returns the constructed ResponseMessage
func (b *ResponseBuilder) Build() ResponseMessage {
	return b.msg
}

// EventBuilder provides a fluent API for building EventMessage
type EventBuilder struct {
	msg EventMessage
}

// NewEvent creates a new event builder with the given action
func NewEvent(action MessageAction) *EventBuilder {
	return &EventBuilder{
		msg: EventMessage{
			Action: action,
		},
	}
}

// WithSource sets the message source
func (b *EventBuilder) WithSource(source MessageSource) *EventBuilder {
	b.msg.Source = source
	return b
}

// WithDestination sets the message destination
func (b *EventBuilder) WithDestination(destination MessageDestination) *EventBuilder {
	b.msg.Destination = destination
	return b
}

// WithPayload sets the message payload
func (b *EventBuilder) WithPayload(payload any) *EventBuilder {
	b.msg.Payload = payload
	return b
}

// WithChannelID sets the channel ID
func (b *EventBuilder) WithChannelID(channelID string) *EventBuilder {
	b.msg.ChannelID = channelID
	return b
}

// WithTraceContext sets the W3C trace context
func (b *EventBuilder) WithTraceContext(traceContext map[string]string) *EventBuilder {
	b.msg.TraceContext = traceContext
	return b
}

// Build returns the constructed EventMessage
func (b *EventBuilder) Build() EventMessage {
	return b.msg
}

// ErrorBuilder provides a fluent API for building ErrorMessage
type ErrorBuilder struct {
	msg ErrorMessage
}

// NewError creates a new error builder with the given action and error code
func NewError(action MessageAction, code string, message string) *ErrorBuilder {
	return &ErrorBuilder{
		msg: ErrorMessage{
			Action: action,
			Error: ErrorResponse{
				Code:    code,
				Message: message,
			},
		},
	}
}

// WithSource sets the message source
func (b *ErrorBuilder) WithSource(source MessageSource) *ErrorBuilder {
	b.msg.Source = source
	return b
}

// WithDestination sets the message destination
func (b *ErrorBuilder) WithDestination(destination MessageDestination) *ErrorBuilder {
	b.msg.Destination = destination
	return b
}

// WithDetails sets error details
func (b *ErrorBuilder) WithDetails(details any) *ErrorBuilder {
	b.msg.Error.Details = details
	return b
}

// WithReplyTo sets the request ID this error is replying to
func (b *ErrorBuilder) WithReplyTo(replyTo string) *ErrorBuilder {
	b.msg.ReplyTo = replyTo
	return b
}

// WithChannelID sets the channel ID
func (b *ErrorBuilder) WithChannelID(channelID string) *ErrorBuilder {
	b.msg.ChannelID = channelID
	return b
}

// WithTraceContext sets the W3C trace context
func (b *ErrorBuilder) WithTraceContext(traceContext map[string]string) *ErrorBuilder {
	b.msg.TraceContext = traceContext
	return b
}

// Build returns the constructed ErrorMessage
func (b *ErrorBuilder) Build() ErrorMessage {
	return b.msg
}

// MapBuilder provides a fluent API for building messages as map[string]any
// This is useful for testing and scenarios where you need dynamic message construction
type MapBuilder struct {
	msg map[string]any
}

// NewMapMessage creates a new map message builder
func NewMapMessage(msgType MessageType, action MessageAction) *MapBuilder {
	return &MapBuilder{
		msg: map[string]any{
			"type":   msgType,
			"action": action,
		},
	}
}

// WithRequestID adds a request_id field
func (b *MapBuilder) WithRequestID(requestID string) *MapBuilder {
	b.msg["request_id"] = requestID
	return b
}

// WithReplyTo adds a reply_to field
func (b *MapBuilder) WithReplyTo(replyTo string) *MapBuilder {
	b.msg["reply_to"] = replyTo
	return b
}

// WithSource adds a source field
func (b *MapBuilder) WithSource(source MessageSource) *MapBuilder {
	b.msg["source"] = source
	return b
}

// WithDestination adds a destination field
func (b *MapBuilder) WithDestination(destination MessageDestination) *MapBuilder {
	b.msg["destination"] = destination
	return b
}

// WithPayload adds a payload field
func (b *MapBuilder) WithPayload(payload any) *MapBuilder {
	b.msg["payload"] = payload
	return b
}

// WithChannelID adds a channel_id field
func (b *MapBuilder) WithChannelID(channelID string) *MapBuilder {
	b.msg["channel_id"] = channelID
	return b
}

// WithTimestamp adds a timestamp field
func (b *MapBuilder) WithTimestamp(timestamp int64) *MapBuilder {
	b.msg["timestamp"] = timestamp
	return b
}

// WithError adds an error field for error messages
func (b *MapBuilder) WithError(code, message string, details any) *MapBuilder {
	errorMap := map[string]any{
		"code":    code,
		"message": message,
	}
	if details != nil {
		errorMap["details"] = details
	}
	b.msg["error"] = errorMap
	return b
}

// WithTraceContext adds trace context fields
func (b *MapBuilder) WithTraceContext(traceContext map[string]string) *MapBuilder {
	b.msg["trace_context"] = traceContext
	return b
}

// WithRetained marks the message as retained (for event messages)
func (b *MapBuilder) WithRetained(retained bool) *MapBuilder {
	b.msg["retained"] = retained
	return b
}

// WithField adds a custom field to the message
func (b *MapBuilder) WithField(key string, value any) *MapBuilder {
	b.msg[key] = value
	return b
}

// Build returns the constructed message map
func (b *MapBuilder) Build() map[string]any {
	return b.msg
}

// Convenience functions for common message patterns

// NewSFUPublisherOffer creates a publisher offer request
func NewSFUPublisherOffer(requestID string, sdp string) RequestMessage {
	return NewRequest("api.sfu.publisher.offer").
		WithRequestID(requestID).
		WithSource(SystemDevice).
		WithDestination(DestinationAPI).
		WithPayload(map[string]any{
			"sdp": sdp,
		}).
		Build()
}

// NewSFUSubscriberOffer creates a subscriber offer request
func NewSFUSubscriberOffer(requestID string, sdp string) RequestMessage {
	return NewRequest("api.sfu.subscriber.offer").
		WithRequestID(requestID).
		WithSource(SystemClient).
		WithDestination(DestinationAPI).
		WithPayload(map[string]any{
			"sdp": sdp,
		}).
		Build()
}

// NewSFUPublisherICE creates a publisher ICE candidate request
func NewSFUPublisherICE(requestID string, candidate string, sdpMid string, sdpMLineIndex int) RequestMessage {
	return NewRequest("api.sfu.publisher.ice").
		WithRequestID(requestID).
		WithSource(SystemDevice).
		WithDestination(DestinationAPI).
		WithPayload(map[string]any{
			"candidate":     candidate,
			"sdpMid":        sdpMid,
			"sdpMLineIndex": sdpMLineIndex,
		}).
		Build()
}

// NewSFUSubscriberICE creates a subscriber ICE candidate request
func NewSFUSubscriberICE(requestID string, candidate string, sdpMid string, sdpMLineIndex int) RequestMessage {
	return NewRequest("api.sfu.subscriber.ice").
		WithRequestID(requestID).
		WithSource(SystemClient).
		WithDestination(DestinationAPI).
		WithPayload(map[string]any{
			"candidate":     candidate,
			"sdpMid":        sdpMid,
			"sdpMLineIndex": sdpMLineIndex,
		}).
		Build()
}

// NewDeviceSessionReady creates a device session ready event
func NewDeviceSessionReady(payload any) EventMessage {
	return NewEvent("device.session.ready").
		WithSource(SystemDevice).
		WithDestination(DestinationWeb).
		WithPayload(payload).
		Build()
}

// NewConnectionActiveEvent creates a connection active event
func NewConnectionActiveEvent(source MessageSource, destination MessageDestination) EventMessage {
	action := string(source) + ".connection.active"
	return NewEvent(MessageAction(action)).
		WithSource(source).
		WithDestination(destination).
		Build()
}

// NewConnectionInactiveEvent creates a connection inactive event
func NewConnectionInactiveEvent(source MessageSource, destination MessageDestination) EventMessage {
	action := string(source) + ".connection.inactive"
	return NewEvent(MessageAction(action)).
		WithSource(source).
		WithDestination(destination).
		Build()
}

// NewTimestampedMapRequest creates a map request with timestamp
func NewTimestampedMapRequest(action MessageAction, requestID string, source MessageSource, destination MessageDestination, payload any) map[string]any {
	return NewMapMessage(TypeRequest, action).
		WithRequestID(requestID).
		WithSource(source).
		WithDestination(destination).
		WithTimestamp(time.Now().UnixMilli()).
		WithPayload(payload).
		Build()
}

// NewTimestampedMapResponse creates a map response with timestamp
func NewTimestampedMapResponse(action MessageAction, replyTo string, source MessageSource, destination MessageDestination, payload any) map[string]any {
	return NewMapMessage(TypeResponse, action).
		WithReplyTo(replyTo).
		WithSource(source).
		WithDestination(destination).
		WithTimestamp(time.Now().UnixMilli()).
		WithPayload(payload).
		Build()
}
