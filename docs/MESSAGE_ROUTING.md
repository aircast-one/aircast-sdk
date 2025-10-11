# Aircast Message Routing Protocol

## Overview

The Aircast protocol uses a **destination-based routing** system that allows explicit control over where messages are delivered. Every message includes a `destination` field that tells the API server how to route it.

## Architecture

```
┌─────────┐                  ┌─────────┐                  ┌────────┐
│   Web   │◄────────────────►│   API   │◄────────────────►│ Device │
│ Client  │                  │ Server  │                  │ Agent  │
└─────────┘                  └─────────┘                  └────────┘
     │                             │                            │
     │   destination: "device"     │                            │
     │────────────────────────────►│──────────────────────────►│
     │                             │                            │
     │                             │    destination: "web"      │
     │◄────────────────────────────│◄───────────────────────────│
     │                             │                            │
     │   destination: "api"        │                            │
     │────────────────────────────►│ (processed internally)     │
     │                             │                            │
```

## Destination Types

### 1. `destination: "web"`
Routes the message to the web client(s) watching this device.

**Use Cases:**
- Device sending telemetry data to web UI
- Device sending events/notifications to web clients
- API forwarding responses to web clients

**Example (Device → Web):**
```typescript
{
  type: "event",
  action: "mavlink.telemetry.update",
  source: "device",
  destination: "web",
  payload: { altitude: 100, speed: 25 }
}
```

### 2. `destination: "device"`
Routes the message to the device/agent.

**Use Cases:**
- Web client sending commands to device
- Web client requesting data from device
- API forwarding requests to device

**Example (Web → Device):**
```typescript
{
  type: "request",
  action: "system.restart",
  source: "client",
  destination: "device",
  request_id: "req-123",
  payload: {}
}
```

### 3. `destination: "api"`
Message is processed by the API server and not forwarded.

**Use Cases:**
- Device sending internal status updates for API processing
- Device sending metrics for API to store/aggregate
- Web client requesting API-managed resources

**Example (Device → API):**
```typescript
{
  type: "event",
  action: "device.heartbeat",
  source: "device",
  destination: "api",
  payload: {
    uptime: 3600,
    memory_used: 45,
    cpu_load: 23
  }
}
```

### 4. `destination: "broadcast"`
Routes the message to all connected clients (web + potentially API processing).

**Use Cases:**
- Device sending alerts to all listeners
- Critical events that need wide distribution
- Status changes that affect multiple components

**Example (Device → Broadcast):**
```typescript
{
  type: "event",
  action: "device.error.critical",
  source: "device",
  destination: "broadcast",
  payload: {
    error: "Camera connection lost",
    severity: "critical"
  }
}
```

## Message Structure

All messages must include these fields:

```typescript
interface BaseMessage {
  type: 'request' | 'response' | 'error' | 'event' | 'will';
  action: string;                    // Action identifier
  source: 'client' | 'device' | 'api'; // Who sent this message
  destination: 'web' | 'device' | 'api' | 'broadcast'; // Where to route it
  request_id?: string;               // For request/response correlation
  retained?: boolean;                // If true, store and send to new clients (events only)
  trace_context?: {                  // W3C Trace Context for distributed tracing
    traceparent: string;
    tracestate?: string;
  };
  payload?: any;                     // Message-specific data
}
```

## Routing Rules

### Device → ?

| Destination | Behavior |
|------------|----------|
| `web` | Forwarded to all connected web clients watching this device |
| `api` | Processed by API, **not** forwarded to web |
| `device` | **Ignored** (device shouldn't send to itself) |
| `broadcast` | Forwarded to web clients + optionally processed by API |

### Web → ?

| Destination | Behavior |
|------------|----------|
| `device` | Forwarded to the device agent |
| `api` | Processed by API, **not** forwarded to device |
| `web` | **Ignored** (unusual case, logged as warning) |
| `broadcast` | Forwarded to device + processed by API |

## Implementation

### TypeScript (SDK)

```typescript
import { MessageDestination } from '@aircast/sdk';

// Sending a request to device
await messageClient.request(
  'system.restart',
  {},
  10000, // timeout
  MessageDestination.Device // destination
);

// Sending an event to web clients
messageClient.sendEvent(
  'telemetry.update',
  { altitude: 100 },
  MessageDestination.Web
);
```

### Go (Agent)

```go
import "github.com/pavliha/aircast-sdk/pkg/message"

// Send event to web clients
client.SendEvent(message.EventMessage{
    Action:      "mavlink.telemetry.update",
    Source:      message.SystemDevice,
    Destination: message.DestinationWeb,
    Payload: map[string]any{
        "altitude": 100,
        "speed":    25,
    },
})

// Send heartbeat to API (not forwarded to web)
client.SendEvent(message.EventMessage{
    Action:      "device.heartbeat",
    Source:      message.SystemDevice,
    Destination: message.DestinationAPI,
    Payload: map[string]any{
        "uptime":      3600,
        "memory_used": 45,
    },
})
```

## Destination Field is Required

The `destination` field is **mandatory** on all messages. Messages without a destination field will be:
- **Logged as errors**
- **Dropped** (not forwarded)
- **Returned with error** to the sender

This ensures explicit and predictable routing behavior across the entire system.

## Best Practices

1. **Always specify destination** - Be explicit about message routing
2. **Use `api` for internal data** - Keep internal metrics/heartbeats from flooding web clients
3. **Use `web` for UI updates** - Send user-facing data directly to web
4. **Use `broadcast` sparingly** - Only for critical events that need wide distribution
5. **Include trace context** - Enable distributed tracing for debugging

## Examples

### Example 1: Device sends telemetry to web
```typescript
// Device (Go)
client.SendEvent(message.EventMessage{
    Action:      "mavlink.telemetry.update",
    Source:      message.SystemDevice,
    Destination: message.DestinationWeb, // Routes to web clients
    Payload: telemetryData,
})
```

### Example 2: Web client commands device
```typescript
// Web client (TypeScript)
await device.system.restart(); // SDK internally sets destination: "device"
```

### Example 3: Device reports health to API
```go
// Device (Go)
client.SendEvent(message.EventMessage{
    Action:      "device.health.report",
    Source:      message.SystemDevice,
    Destination: message.DestinationAPI, // API processes, not forwarded to web
    Payload: map[string]any{
        "cpu_load":    23,
        "memory_used": 45,
        "uptime":      3600,
    },
})
```

### Example 4: Critical alert broadcast
```go
// Device (Go)
client.SendEvent(message.EventMessage{
    Action:      "device.alert.critical",
    Source:      message.SystemDevice,
    Destination: message.DestinationBroadcast, // Goes to web + API
    Payload: map[string]any{
        "severity": "critical",
        "message":  "Camera connection lost",
    },
})
```

## API Implementation Details

The API server routes messages based on the `destination` field in `session_coordinator.go`:

```go
// Extract destination from message
destination, err := extractDestinationFromMessage(msg)

// Route based on destination
switch destination {
case message.DestinationWeb, message.DestinationBroadcast:
    webSession.SendMessage(msg) // Forward to web

case message.DestinationAPI:
    // Process internally, don't forward

case message.DestinationDevice:
    deviceSession.SendMessage(msg) // Forward to device
}
```

## Debugging

Enable debug logging to see routing decisions:

```bash
# API
LOG_LEVEL=debug ./aircast-api

# Look for log entries like:
[DEVICE→WEB] Routing to web based on destination field
[WEB→DEVICE] Routing to device based on destination field
[DEVICE→API] Message for API processing, not forwarding to web
```

## Protocol Enhancements

### Retained Messages

**Status:** ✅ Implemented in SDK

Retained messages allow devices to mark important state updates that should be immediately delivered to new web clients upon connection.

**Use Case:** When a new web client connects, it needs to see the current device state (battery level, GPS position, connection status) without waiting for the next update.

**How it works:**
1. Device sends event with `retained: true`
2. API server stores the latest retained message per action
3. When a new web client connects, API immediately sends all retained messages
4. Subsequent updates replace the stored retained message

**Example:**

```go
// Device sends retained telemetry
client.SendEvent(message.EventMessage{
    Action:      "mavlink.telemetry.battery",
    Payload:     map[string]any{"level": 85, "voltage": 12.4},
    Source:      message.SystemDevice,
    Destination: message.DestinationWeb,
    Retained:    true,  // Store and send to new clients
})

// Streaming data (not retained)
client.SendEvent(message.EventMessage{
    Action:      "mavlink.telemetry.gps.stream",
    Payload:     gpsData,
    Source:      message.SystemDevice,
    Destination: message.DestinationWeb,
    Retained:    false,  // Don't store, too frequent
})
```

**What should be retained:**
- ✅ Battery status
- ✅ Connection status
- ✅ Last known GPS position
- ✅ Device configuration
- ❌ High-frequency streaming data
- ❌ Ephemeral signaling messages (WebRTC ICE candidates)
- ❌ Log entries

### Last Will and Testament (LWT)

**Status:** ✅ Implemented in SDK

Last Will allows devices to register a message that will be automatically sent by the API if the connection closes unexpectedly (crash, network loss).

**Use Case:** Web clients need immediate notification when a device disconnects unexpectedly, not just a timeout.

**How it works:**
1. Device registers will message on connection using `client.RegisterWill()`
2. API stores the will for this device/session
3. If connection closes gracefully → will is cleared and not sent
4. If connection closes unexpectedly → API sends will message to specified destination

**Example:**

```go
// Register will on connect
client.RegisterWill(message.WillMessage{
    Action:      "device.status",
    Payload:     map[string]any{
        "status":    "offline",
        "reason":    "connection_lost",
        "timestamp": time.Now(),
    },
    Destination: message.DestinationBroadcast,
})

// Graceful disconnect (clears will first)
client.ClearWill()  // Will not be executed
client.SendEvent(message.EventMessage{
    Action:      "device.status",
    Payload:     map[string]any{"status": "offline", "reason": "shutdown"},
    Source:      message.SystemDevice,
    Destination: message.DestinationBroadcast,
})
client.Close()
```

**Message Types:**

```typescript
interface WillMessage {
  action: string;
  payload?: any;
  destination: 'web' | 'api' | 'broadcast';
  trace_context?: {
    traceparent: string;
    tracestate?: string;
  };
}
```

### Persistent Sessions

**Status:** ✅ Protocol defined (API implementation required)

Persistent sessions allow messages to survive device restarts and temporary disconnections.

**Use Case:** Critical commands (firmware updates, configuration changes) sent while device is offline must be delivered when it reconnects.

**How it works:**
1. Device connects with a stable `clientID` and `cleanSession: false`
2. When device is offline, API queues messages in persistent storage (database/Redis)
3. When device reconnects with same `clientID`, API delivers all queued messages
4. Messages have configurable TTL and priority

**Example:**

```go
// Device connects with persistent session
client.Connect(message.SessionConfig{
    ClientID:     "device-123-abc",  // Stable ID across restarts
    CleanSession: false,             // Keep queued messages
})

// Web sends command while device offline
// → API queues message in database

// Device reconnects (after restart)
client.Connect(message.SessionConfig{
    ClientID:     "device-123-abc",  // Same ID
    CleanSession: false,
})
// → API delivers all queued commands
```

**Session Configuration:**

```typescript
interface SessionConfig {
  client_id: string;      // Stable identifier for the client
  clean_session: boolean; // true = discard old messages, false = keep queued
}
```

**Queue Properties:**
- Messages have priority (critical commands first)
- Messages have TTL (expire after configurable time)
- Queue size limits (oldest non-critical dropped first)
- Persistent storage (survives API restart)

### Protocol Enhancement Summary

| Feature | Status | Benefit | Complexity |
|---------|--------|---------|------------|
| Retained Messages | ✅ SDK Ready | New clients get instant state | Low |
| Last Will | ✅ SDK Ready | Auto disconnect notifications | Low |
| Persistent Sessions | 📋 Protocol Defined | Messages survive restarts | High |

**Implementation Status:**
- **SDK (Go)**: All three features implemented in protocol types
- **API Server**: Requires implementation of storage and routing logic (see [PROTOCOL_ENHANCEMENTS.md](docs/PROTOCOL_ENHANCEMENTS.md))

## Future Enhancements

Additional potential features:
- **Destination filtering** - Filter by specific web session IDs
- **Priority routing** - High-priority messages take precedence
- **Destination groups** - Route to named groups of clients
- **Conditional routing** - Route based on message content
- **Topic-based routing** - Combine destination with topic patterns for more granular subscriptions
