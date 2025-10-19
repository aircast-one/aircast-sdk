package room

import (
	"context"
	"testing"

	"github.com/pavliha/aircast-sdk/pkg/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockClient for testing
type MockClient struct {
	mock.Mock
}

func (m *MockClient) Send(msg any) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockClient) GetSource() message.MessageSource {
	args := m.Called()
	return args.Get(0).(message.MessageSource)
}

// Stub other Client interface methods (not used in Room tests)
func (m *MockClient) Listen(ctx context.Context) error            { return nil }
func (m *MockClient) Close() error                                { return nil }
func (m *MockClient) IsClosed() bool                              { return false }
func (m *MockClient) ReadMessage() <-chan any                     { return nil }
func (m *MockClient) RegisterWill(will message.WillMessage) error { return nil }
func (m *MockClient) ClearWill() error                            { return nil }
func (m *MockClient) SendRawJSON(jsonBytes []byte) error          { return nil }

func TestRoom_SendResponse(t *testing.T) {
	client := &MockClient{}
	roomID := message.RoomID("test-room")
	room := NewRoom(client, roomID)

	req := &message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		RoomID:    roomID,
		Source:    message.SystemWeb,
	}
	payload := map[string]any{"result": "success"}

	client.On("GetSource").Return(message.SystemDevice)
	client.On("Send", mock.MatchedBy(func(msg any) bool {
		resp, ok := msg.(message.ResponseMessage)
		return ok && resp.Action == "test.action" && resp.ReplyTo == "req-123"
	})).Return(nil)

	err := room.SendResponse(req, payload)
	assert.NoError(t, err)
	client.AssertExpectations(t)
}

func TestRoom_SendError(t *testing.T) {
	client := &MockClient{}
	roomID := message.RoomID("test-room")
	room := NewRoom(client, roomID)

	req := &message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		RoomID:    roomID,
		Source:    message.SystemWeb,
	}
	errResp := message.ErrorResponse{
		Code:    "TEST_ERROR",
		Message: "Test error message",
	}

	client.On("GetSource").Return(message.SystemDevice)
	client.On("Send", mock.MatchedBy(func(msg any) bool {
		errMsg, ok := msg.(message.ErrorMessage)
		return ok && errMsg.Error.Code == "TEST_ERROR" && errMsg.ReplyTo == "req-123"
	})).Return(nil)

	err := room.SendError(req, errResp)
	assert.NoError(t, err)
	client.AssertExpectations(t)
}

func TestRoom_SendEvent(t *testing.T) {
	client := &MockClient{}
	roomID := message.RoomID("test-room")
	room := NewRoom(client, roomID)

	action := message.MessageAction("test.event")
	payload := map[string]any{"data": "test"}
	destination := message.DestinationWeb

	client.On("GetSource").Return(message.SystemDevice)
	client.On("Send", mock.MatchedBy(func(msg any) bool {
		evt, ok := msg.(message.EventMessage)
		return ok && evt.Action == action && evt.Destination == destination
	})).Return(nil)

	err := room.SendEvent(action, payload, destination)
	assert.NoError(t, err)
	client.AssertExpectations(t)
}

func TestRoom_RoomID(t *testing.T) {
	client := &MockClient{}
	roomID := message.RoomID("test-room-123")
	room := NewRoom(client, roomID)

	assert.Equal(t, roomID, room.RoomID())
}

func TestNewRoom(t *testing.T) {
	client := &MockClient{}
	roomID := message.RoomID("test-room")
	room := NewRoom(client, roomID)

	assert.NotNil(t, room)
	assert.Equal(t, roomID, room.RoomID())
}
