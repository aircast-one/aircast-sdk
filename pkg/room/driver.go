package room

import (
	"github.com/pavliha/aircast-sdk/pkg/message"
)

// Driver defines the interface for room subscription backends
type Driver interface {
	// Subscribe adds a subscriber to a room and returns a channel for receiving messages
	Subscribe(roomID message.RoomID, subscriberID string, bufferSize int) (<-chan message.GenericMessage, error)

	// Unsubscribe removes a subscriber from a room
	Unsubscribe(roomID message.RoomID, subscriberID string)

	// Publish sends a message to all subscribers of a room
	Publish(roomID message.RoomID, msg message.GenericMessage)

	// SubscriberCount returns the number of subscribers for a room
	SubscriberCount(roomID message.RoomID) int

	// CloseAll closes all subscriber channels and clears all rooms
	CloseAll()
}
