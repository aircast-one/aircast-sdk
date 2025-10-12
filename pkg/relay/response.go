package relay

// ResponseWriter defines a common interface for sending responses back to clients.
// Both aircast-api and aircast-agent implement this interface with their specific needs:
// - API: Uses session-based routing with ResponseSender
// - Agent: Uses message.Client for direct responses
type ResponseWriter interface {
	// SendSuccess sends a success response with the given payload
	SendSuccess(payload any) error
	// SendError sends an error response with the given code and message
	SendError(code string, message string) error
}

// Note: Actual Response implementations remain in their respective services
// because they have service-specific logging, tracing, and routing needs.
// This interface documents the common contract.
