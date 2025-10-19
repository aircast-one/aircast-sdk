package room

import (
	"github.com/pavliha/aircast-sdk/pkg/message"
)

// Room is a lightweight helper for sending messages to a specific room
type Room struct {
	client message.Client
	roomID message.RoomID
}

// NewRoom creates a new Room helper
func NewRoom(client message.Client, roomID message.RoomID) *Room {
	return &Room{
		client: client,
		roomID: roomID,
	}
}

// SendResponse sends a response message to the room
func (r *Room) SendResponse(req *message.RequestMessage, payload any) error {
	return r.client.Send(message.ResponseMessage{
		Action:      req.Action,
		Payload:     payload,
		Source:      r.client.GetSource(),
		Destination: message.SourceToDestination(req.Source),
		RoomID:      r.roomID,
		ReplyTo:     req.RequestID,
	})
}

// SendError sends an error message to the room
func (r *Room) SendError(req *message.RequestMessage, errResponse message.ErrorResponse) error {
	return r.client.Send(message.ErrorMessage{
		Action:      req.Action,
		Source:      r.client.GetSource(),
		Destination: message.SourceToDestination(req.Source),
		RoomID:      r.roomID,
		Error:       errResponse,
		ReplyTo:     req.RequestID,
	})
}

// SendEvent sends an event message to the room
func (r *Room) SendEvent(action message.MessageAction, payload any, destination message.MessageDestination) error {
	return r.client.Send(message.EventMessage{
		Action:      action,
		Payload:     payload,
		Source:      r.client.GetSource(),
		Destination: destination,
		RoomID:      r.roomID,
	})
}

// RoomID returns the room's ID
func (r *Room) RoomID() message.RoomID {
	return r.roomID
}
