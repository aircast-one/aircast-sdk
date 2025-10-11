# Aircast SDK

A Go SDK for the Aircast messaging protocol, providing WebSocket-based communication with destination-based routing, resilient message delivery, and comprehensive tracing support.

## Overview

Aircast SDK implements a robust messaging protocol designed for IoT device-to-web communication, particularly suited for applications like drone control systems, telemetry streaming, and real-time device management. The SDK provides a type-safe client library with built-in connection resilience and message queuing.

## Features

- **Type-Safe Messaging**: Strongly-typed message structures (Request, Response, Event, Error)
- **Destination-Based Routing**: Explicit message routing to web clients, API servers, devices, or broadcast
- **Resilient Delivery**: QueuedClient with automatic retry and message queuing during disconnections
- **Channel Support**: Multi-channel communication for session isolation
- **Trace Context**: W3C Trace Context integration for distributed tracing
- **High Performance**: Optimized parsing with buffer pooling and efficient JSON handling
- **WebRTC-Aware**: Prioritized delivery for critical WebRTC signaling messages
- **Comprehensive Testing**: Full test suite with benchmarks and stress tests

## Installation

```bash
go get github.com/pavliha/aircast-sdk
```

## Quick Start

### Basic Client Usage

```go
package main

import (
    "context"
    "github.com/pavliha/aircast-sdk/pkg/message"
    log "github.com/sirupsen/logrus"
)

func main() {
    // Create your WebSocket connection (implements message.Connection interface)
    conn := YourWebSocketConnection()

    // Create a message client
    logger := log.NewEntry(log.New())
    client := message.NewClient(logger, conn, message.ClientConfig{
        Source: message.SystemDevice,
    })

    // Start listening for messages
    ctx := context.Background()
    go client.Listen(ctx)

    // Send an event to web clients
    client.SendEventToChannel(
        "telemetry.update",
        map[string]any{
            "altitude": 100,
            "speed": 25,
        },
        "channel-123",
    )

    // Receive messages
    for msg := range client.ReadMessage() {
        switch m := msg.(type) {
        case *message.RequestMessage:
            // Handle request
            client.SendResponse(m, map[string]string{"status": "ok"})
        case *message.EventMessage:
            // Handle event
        }
    }
}
```

### Resilient Client with Queuing

For applications requiring message delivery guarantees during temporary disconnections:

```go
// Create base client
baseClient := message.NewClient(logger, conn, message.ClientConfig{
    Source: message.SystemDevice,
})

// Wrap with QueuedClient for resilience
config := message.DefaultQueueConfig()
config.MaxQueueSize = 200
config.MaxCriticalRetries = 10

client := message.NewQueuedClient(baseClient, logger, &config)

// Messages are automatically queued during disconnections
// and replayed when connection is restored
client.SendEventToChannel("sensor.reading", data, channelID)
```

## Message Types

### Request
Client-initiated requests expecting a response:

```go
client.Send(message.RequestMessage{
    Action:      "system.restart",
    Source:      message.SystemClient,
    Destination: message.DestinationDevice,
    RequestID:   "req-123",
    Payload:     map[string]any{},
}, &channelID)
```

### Response
Server responses to requests:

```go
client.SendResponse(request, map[string]any{
    "status": "success",
})
```

### Event
Server-initiated events (no response expected):

```go
client.SendEventToChannel(
    "mavlink.telemetry.update",
    telemetryData,
    channelID,
)
```

### Error
Error responses:

```go
client.SendErrorToChannel(request, message.ErrorResponse{
    Code:    "DEVICE_OFFLINE",
    Message: "Device is not responding",
})
```

## Message Routing

The SDK implements destination-based routing. Every message must specify where it should be delivered:

| Destination | Description | Use Case |
|------------|-------------|----------|
| `DestinationWeb` | Routes to web client(s) | Device telemetry, status updates |
| `DestinationDevice` | Routes to device/agent | Web commands, requests |
| `DestinationAPI` | Routes to API server only | Internal metrics, heartbeats |
| `DestinationBroadcast` | Routes to all clients | Critical alerts, system events |

Example:

```go
// Send telemetry to web clients only
client.SendEventToChannel(
    "telemetry.update",
    data,
    channelID,
)

// Send heartbeat to API (not forwarded to web)
client.Send(message.EventMessage{
    Action:      "device.heartbeat",
    Source:      message.SystemDevice,
    Destination: message.DestinationAPI,
    Payload:     healthData,
}, nil)
```

See [MESSAGE_ROUTING.md](MESSAGE_ROUTING.md) for detailed routing documentation.

## QueuedClient Features

The `QueuedClient` provides resilient message delivery:

- **Automatic Queuing**: Messages are queued when connection is lost
- **Smart Retry**: Configurable retry logic with exponential backoff
- **Critical Message Priority**: WebRTC signaling messages get higher priority
- **Age-Based Expiry**: Old messages are dropped to prevent stale data
- **Connection Recovery**: Automatic queue flush when connection is restored

```go
// Configure queue behavior
config := message.QueueConfig{
    MaxQueueSize:       100,              // Max queued messages
    MaxMessageAge:      30 * time.Second, // Max age for normal messages
    MaxCriticalAge:     60 * time.Second, // Max age for critical messages
    FlushInterval:      1 * time.Second,  // Flush attempt frequency
    MaxRetries:         3,                // Retries for normal messages
    MaxCriticalRetries: 10,               // Retries for critical messages
    Source:             message.SystemDevice,
}

// Get queue statistics
stats := queuedClient.GetQueueStats()
// Returns: {"total": 5, "critical": 2, "normal": 3, "oldest_age": "5.2s"}
```

## Testing

### Run All Tests

```bash
make test
```

### Run Tests with Coverage

```bash
make test.coverage
```

### Generate HTML Coverage Report

```bash
make test.coverage.html
```

### Run Benchmarks

```bash
go test -bench=. ./pkg/message/
```

## Development

### Available Make Targets

```bash
make help           # Show all available targets
make test           # Run tests
make lint           # Run linter
make fmt            # Format code
make vet            # Run go vet
make check          # Run all checks (fmt, vet, lint, test)
make clean          # Clean generated files
```

### Version Management

```bash
make version        # Show current version
make version.patch  # Create patch version (v1.0.0 -> v1.0.1)
make version.minor  # Create minor version (v1.0.0 -> v1.1.0)
make version.major  # Create major version (v1.0.0 -> v2.0.0)
make version.dev    # Create dev version (v1.0.0-dev.1)
make version.alpha  # Create alpha version (v1.0.0-alpha.1)
make version.rc     # Create release candidate (v1.0.0-rc.1)
```

## Architecture

```
┌─────────┐                  ┌─────────┐                  ┌────────┐
│   Web   │◄────────────────►│   API   │◄────────────────►│ Device │
│ Client  │   WebSocket      │ Server  │   WebSocket      │ Agent  │
└─────────┘                  └─────────┘                  └────────┘
     │                             │                            │
     │   destination: "device"     │                            │
     │────────────────────────────►│──────────────────────────►│
     │                             │                            │
     │                             │    destination: "web"      │
     │◄────────────────────────────│◄───────────────────────────│
```

## Performance

The SDK is optimized for high-throughput scenarios:

- **Buffer Pooling**: Reuses byte buffers for JSON encoding
- **Channel Buffering**: Large message channels (10,000 messages) prevent blocking
- **Zero-Copy Parsing**: Efficient JSON parsing with minimal allocations
- **Conditional Logging**: Log statements only execute when log level is enabled

Benchmark results (example):

```
BenchmarkJSONParsing-8         1000000    1234 ns/op    512 B/op    8 allocs/op
BenchmarkMessageSend-8         500000     2345 ns/op    1024 B/op   12 allocs/op
```

## License

[Your License Here]

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Maintain test coverage above 80%
- Run `make check` before committing
- Follow Go best practices and idioms
- Add benchmarks for performance-critical code
- Update documentation for API changes

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/pavliha/aircast-sdk).
