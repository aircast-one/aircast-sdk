package relay

import (
	"testing"

	"github.com/pavliha/aircast-sdk/pkg/message"
	"github.com/stretchr/testify/assert"
)

func TestCreateFromRequestMessage_Success(t *testing.T) {
	reqMsg := message.RequestMessage{
		Action:    "test.action",
		RoomID:    "session-123",
		RequestID: "req-123",
		Source:    "device",
		Payload: map[string]any{
			"field1": "value1",
			"field2": 42,
		},
		TraceContext: map[string]string{
			"traceparent": "00-trace-id-span-id-01",
		},
	}

	req, err := CreateFromRequestMessage(reqMsg)

	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Equal(t, "test.action", req.Action)
	assert.Equal(t, "session-123", req.SessionID)
	assert.Equal(t, "req-123", req.RequestID)
	assert.Equal(t, "device", req.Source)
	assert.Equal(t, "value1", req.Payload["field1"])
	assert.Equal(t, 42, req.Payload["field2"])
	assert.Equal(t, "00-trace-id-span-id-01", req.TraceContext["traceparent"])
}

func TestCreateFromRequestMessage_NilPayload(t *testing.T) {
	reqMsg := message.RequestMessage{
		Action:    "test.action",
		RoomID:    "session-123",
		RequestID: "req-123",
		Source:    "web",
		Payload:   nil,
	}

	req, err := CreateFromRequestMessage(reqMsg)

	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Nil(t, req.Payload)
}

func TestCreateFromRequestMessage_InvalidPayload(t *testing.T) {
	reqMsg := message.RequestMessage{
		Action:    "test.action",
		RoomID:    "session-123",
		RequestID: "req-123",
		Source:    "web",
		Payload:   "not a map", // Invalid payload type
	}

	req, err := CreateFromRequestMessage(reqMsg)

	assert.Error(t, err)
	assert.Nil(t, req)
	assert.Contains(t, err.Error(), "invalid payload format")
}

func TestCreateFromRequestMessage_MissingRequestID(t *testing.T) {
	reqMsg := message.RequestMessage{
		Action:    "test.action",
		RoomID:    "session-123",
		RequestID: "", // Missing request ID
		Source:    "web",
		Payload:   nil,
	}

	req, err := CreateFromRequestMessage(reqMsg)

	assert.Error(t, err)
	assert.Nil(t, req)
	assert.Contains(t, err.Error(), "request ID is required")
}

func TestCreateFromRequestMessage_MissingSource(t *testing.T) {
	reqMsg := message.RequestMessage{
		Action:    "test.action",
		RoomID:    "session-123",
		RequestID: "req-123",
		Source:    "", // Missing source
		Payload:   nil,
	}

	req, err := CreateFromRequestMessage(reqMsg)

	assert.Error(t, err)
	assert.Nil(t, req)
	assert.Contains(t, err.Error(), "request source is required")
}

func TestCreateFromRequestMessage_MissingRoomID(t *testing.T) {
	reqMsg := message.RequestMessage{
		Action:    "test.action",
		RoomID:    "", // Missing room ID
		RequestID: "req-123",
		Source:    "web",
		Payload:   nil,
	}

	req, err := CreateFromRequestMessage(reqMsg)

	assert.Error(t, err)
	assert.Nil(t, req)
	assert.Contains(t, err.Error(), "request channel ID is required")
}

func TestRequest_ProcessPayload_Success(t *testing.T) {
	req := &Request{
		Payload: map[string]any{
			"name": "John",
			"age":  30,
		},
	}

	type TestStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	var result TestStruct
	err := req.ProcessPayload(&result)

	assert.NoError(t, err)
	assert.Equal(t, "John", result.Name)
	assert.Equal(t, 30, result.Age)
}

func TestRequest_ProcessPayload_NilPayload(t *testing.T) {
	req := &Request{
		Payload: nil,
	}

	type TestStruct struct {
		Name string `json:"name"`
	}

	var result TestStruct
	err := req.ProcessPayload(&result)

	assert.NoError(t, err)
	assert.Equal(t, "", result.Name) // Should be zero value
}

func TestRequest_ProcessPayload_InvalidTarget(t *testing.T) {
	req := &Request{
		Payload: map[string]any{
			"name": "John",
			"age":  "not a number", // Type mismatch
		},
	}

	type TestStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	var result TestStruct
	err := req.ProcessPayload(&result)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal payload")
}

func TestEventRequest_ProcessPayload_Success(t *testing.T) {
	eventReq := &EventRequest{
		Payload: map[string]any{
			"status": "active",
			"count":  5,
		},
	}

	type TestStruct struct {
		Status string `json:"status"`
		Count  int    `json:"count"`
	}

	var result TestStruct
	err := eventReq.ProcessPayload(&result)

	assert.NoError(t, err)
	assert.Equal(t, "active", result.Status)
	assert.Equal(t, 5, result.Count)
}

func TestProcessPayload_ComplexNestedStructure(t *testing.T) {
	payload := map[string]any{
		"user": map[string]any{
			"name": "John",
			"address": map[string]any{
				"city":  "New York",
				"state": "NY",
			},
		},
		"tags": []any{"tag1", "tag2"},
	}

	type Address struct {
		City  string `json:"city"`
		State string `json:"state"`
	}

	type User struct {
		Name    string  `json:"name"`
		Address Address `json:"address"`
	}

	type TestStruct struct {
		User User     `json:"user"`
		Tags []string `json:"tags"`
	}

	var result TestStruct
	err := processPayload(payload, &result)

	assert.NoError(t, err)
	assert.Equal(t, "John", result.User.Name)
	assert.Equal(t, "New York", result.User.Address.City)
	assert.Equal(t, "NY", result.User.Address.State)
	assert.Equal(t, []string{"tag1", "tag2"}, result.Tags)
}

func TestProcessPayload_WithOptionalFields(t *testing.T) {
	payload := map[string]any{
		"required": "value",
		// "optional" field is missing
	}

	type TestStruct struct {
		Required string `json:"required"`
		Optional string `json:"optional,omitempty"`
	}

	var result TestStruct
	err := processPayload(payload, &result)

	assert.NoError(t, err)
	assert.Equal(t, "value", result.Required)
	assert.Equal(t, "", result.Optional) // Should be empty string (zero value)
}

func TestProcessPayload_WithPointerFields(t *testing.T) {
	count := 42
	payload := map[string]any{
		"name":  "test",
		"count": count,
	}

	type TestStruct struct {
		Name  *string `json:"name"`
		Count *int    `json:"count"`
	}

	var result TestStruct
	err := processPayload(payload, &result)

	assert.NoError(t, err)
	assert.NotNil(t, result.Name)
	assert.Equal(t, "test", *result.Name)
	assert.NotNil(t, result.Count)
	assert.Equal(t, 42, *result.Count)
}

func TestProcessPayload_EmptyPayload(t *testing.T) {
	payload := map[string]any{}

	type TestStruct struct {
		Name string `json:"name"`
	}

	var result TestStruct
	err := processPayload(payload, &result)

	assert.NoError(t, err)
	assert.Equal(t, "", result.Name)
}
