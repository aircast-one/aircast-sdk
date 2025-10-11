# Protocol Enhancements: Retained Messages, Last Will, and Persistent Sessions

## Overview

This document outlines the design and implementation plan for three critical features inspired by MQTT:

1. **Retained Messages** - New clients immediately receive current device state
2. **Last Will and Testament (LWT)** - Automatic disconnect notifications
3. **Persistent Sessions** - Message delivery survives restarts

These enhancements maintain our simple destination-based routing while adding MQTT-like reliability.

---

## 1. Retained Messages

### Problem
When a new web client connects, it must wait for the next telemetry update to see device state. This could take seconds or minutes for infrequent data (battery level, connection status, etc.).

### Solution
Allow devices to mark messages as "retained". The API server stores the latest retained message per action and immediately sends them to new web clients on connection.

### Protocol Changes

**Update EventMessage** in `pkg/message/types.go`:

```go
type EventMessage struct {
    Action       MessageAction      `json:"action"`
    Payload      any                `json:"payload,omitempty"`
    Source       MessageSource      `json:"source"`
    Destination  MessageDestination `json:"destination"`
    ChannelID    ChannelID          `json:"channel_id,omitempty"`
    Retained     bool               `json:"retained,omitempty"`  // NEW: Store for new clients
    TraceContext map[string]string  `json:"trace_context,omitempty"`
}
```

### API Implementation

```go
// RetainedMessageStore manages retained messages per device
type RetainedMessageStore struct {
    // Key format: "deviceID:action" -> EventMessage
    messages map[string]EventMessage
    mutex    sync.RWMutex
}

// Store retained message when received from device
func (s *RetainedMessageStore) Store(deviceID string, msg EventMessage) {
    if !msg.Retained {
        return
    }

    key := fmt.Sprintf("%s:%s", deviceID, msg.Action)
    s.mutex.Lock()
    s.messages[key] = msg
    s.mutex.Unlock()
}

// Retrieve all retained messages for a device
func (s *RetainedMessageStore) GetAll(deviceID string) []EventMessage {
    s.mutex.RLock()
    defer s.mutex.RUnlock()

    prefix := deviceID + ":"
    var messages []EventMessage
    for key, msg := range s.messages {
        if strings.HasPrefix(key, prefix) {
            messages = append(messages, msg)
        }
    }
    return messages
}

// Delete retained message (when device sends empty payload)
func (s *RetainedMessageStore) Delete(deviceID string, action MessageAction) {
    key := fmt.Sprintf("%s:%s", deviceID, action)
    s.mutex.Lock()
    delete(s.messages, key)
    s.mutex.Unlock()
}
```

### Usage Examples

**Device sends current state (retained):**
```go
client.SendEvent(EventMessage{
    Action:      "mavlink.telemetry.battery",
    Payload:     map[string]any{"level": 85, "voltage": 12.4},
    Source:      message.SystemDevice,
    Destination: message.DestinationWeb,
    Retained:    true,  // New web clients get this immediately
})

client.SendEvent(EventMessage{
    Action:      "device.connection.status",
    Payload:     map[string]any{"status": "connected", "latency_ms": 45},
    Source:      message.SystemDevice,
    Destination: message.DestinationWeb,
    Retained:    true,
})
```

**Device sends streaming data (not retained):**
```go
client.SendEvent(EventMessage{
    Action:      "mavlink.telemetry.gps.stream",
    Payload:     gpsData,
    Source:      message.SystemDevice,
    Destination: message.DestinationWeb,
    Retained:    false,  // Don't store, too frequent
})
```

**Clear retained message:**
```go
// Send retained message with nil/empty payload to delete
client.SendEvent(EventMessage{
    Action:      "mavlink.telemetry.battery",
    Payload:     nil,
    Source:      message.SystemDevice,
    Destination: message.DestinationWeb,
    Retained:    true,
})
```

### API Connection Flow

```go
// When web client connects to device
func (api *API) handleWebClientConnect(deviceID string, webSession *WebSession) {
    // 1. Send all retained messages immediately
    retainedMessages := api.retainedStore.GetAll(deviceID)
    for _, msg := range retainedMessages {
        webSession.SendMessage(msg)
    }

    // 2. Continue with normal message routing
    // ...
}
```

### Use Cases

| Action | Retained | Reason |
|--------|----------|--------|
| `mavlink.telemetry.battery` | ✅ Yes | Infrequent, clients need current value |
| `mavlink.telemetry.gps` | ✅ Yes | Last known position important |
| `device.connection.status` | ✅ Yes | Clients need to know if device online |
| `device.system.info` | ✅ Yes | Static device info |
| `webrtc.session.ice.candidate` | ❌ No | Ephemeral, time-sensitive |
| `mavlink.telemetry.gps.stream` | ❌ No | High frequency, current only |
| `device.log.entry` | ❌ No | Historical, not state |

---

## 2. Last Will and Testament (LWT)

### Problem
When a device disconnects unexpectedly (crash, network loss), the web client must wait for connection timeout to detect it. There's no immediate "device offline" notification.

### Solution
Devices register a "will message" when connecting. If the connection closes unexpectedly, the API automatically sends the will message to notify listeners.

### Protocol Changes

**Add new message type** in `pkg/message/types.go`:

```go
const (
    TypeRequest  MessageType = "request"
    TypeResponse MessageType = "response"
    TypeError    MessageType = "error"
    TypeEvent    MessageType = "event"
    TypeWill     MessageType = "will"  // NEW: Last will registration
)

// WillMessage defines what to send if connection lost
type WillMessage struct {
    Action       MessageAction      `json:"action"`
    Payload      any                `json:"payload"`
    Destination  MessageDestination `json:"destination"`
    TraceContext map[string]string  `json:"trace_context,omitempty"`
}
```

**Update Client interface** in `pkg/message/client.go`:

```go
type Client interface {
    // ... existing methods ...

    // RegisterWill sets the Last Will message
    // Sent by API if connection closes unexpectedly
    RegisterWill(will WillMessage) error

    // ClearWill removes the will (on graceful disconnect)
    ClearWill() error
}
```

### API Implementation

```go
// WillRegistry manages last will messages
type WillRegistry struct {
    // Key: deviceID or channelID -> WillMessage
    wills map[string]WillMessage
    mutex sync.RWMutex
}

// Register will message when client connects
func (w *WillRegistry) Register(clientID string, will WillMessage) {
    w.mutex.Lock()
    w.wills[clientID] = will
    w.mutex.Unlock()
}

// Execute will on unexpected disconnect
func (w *WillRegistry) Execute(clientID string) *WillMessage {
    w.mutex.Lock()
    defer w.mutex.Unlock()

    if will, exists := w.wills[clientID]; exists {
        delete(w.wills, clientID)  // Execute once
        return &will
    }
    return nil
}

// Clear will on graceful disconnect
func (w *WillRegistry) Clear(clientID string) {
    w.mutex.Lock()
    delete(w.wills, clientID)
    w.mutex.Unlock()
}
```

### Client Implementation

```go
// In pkg/message/client.go

func (c *client) RegisterWill(will WillMessage) error {
    envelope := struct {
        Type string `json:"type"`
        WillMessage
    }{
        Type:        TypeWill,
        WillMessage: will,
    }

    data, err := json.Marshal(envelope)
    if err != nil {
        return err
    }

    return c.conn.SendMessage(data)
}

func (c *client) ClearWill() error {
    // Send empty will to clear
    return c.RegisterWill(WillMessage{})
}
```

### API Connection Handling

```go
// When device connects
func (api *API) handleDeviceConnect(deviceID string, deviceSession *DeviceSession) {
    // Device sends will message during handshake
    // API stores it in WillRegistry
}

// When connection closes
func (api *API) handleConnectionClose(deviceID string, graceful bool) {
    if graceful {
        // Graceful disconnect - clear will without executing
        api.willRegistry.Clear(deviceID)
    } else {
        // Unexpected disconnect - execute will
        if will := api.willRegistry.Execute(deviceID); will != nil {
            // Convert to EventMessage and broadcast
            event := EventMessage{
                Action:       will.Action,
                Payload:      will.Payload,
                Source:       SystemAPI,
                Destination:  will.Destination,
                TraceContext: will.TraceContext,
            }

            // Route based on destination
            api.routeMessage(deviceID, event)
        }
    }
}
```

### Usage Examples

**Device registers will on connect:**
```go
// During connection handshake
client.RegisterWill(WillMessage{
    Action:      "device.status",
    Payload: map[string]any{
        "status": "offline",
        "reason": "connection_lost",
        "timestamp": time.Now(),
    },
    Destination: message.DestinationBroadcast,
})
```

**Graceful disconnect (clears will):**
```go
// Before closing connection
client.ClearWill()
client.SendEvent(EventMessage{
    Action:      "device.status",
    Payload:     map[string]any{"status": "offline", "reason": "shutdown"},
    Source:      message.SystemDevice,
    Destination: message.DestinationBroadcast,
})
client.Close()
```

**Unexpected disconnect:**
```go
// Connection lost (crash, network failure)
// API automatically sends will message:
// {
//   type: "event",
//   action: "device.status",
//   source: "api",
//   destination: "broadcast",
//   payload: { status: "offline", reason: "connection_lost", ... }
// }
```

### Web Client Experience

```typescript
// Web client subscribes to device status
messageClient.on('event', (msg: EventMessage) => {
    if (msg.action === 'device.status') {
        const { status, reason } = msg.payload;

        if (status === 'offline') {
            if (reason === 'connection_lost') {
                // Unexpected disconnect - show alert
                showAlert('Device disconnected unexpectedly');
            } else if (reason === 'shutdown') {
                // Graceful shutdown - show info
                showInfo('Device shut down');
            }
        }
    }
});
```

---

## 3. Persistent Sessions

### Problem
When a device restarts or temporarily loses connectivity, messages sent during the offline period are lost. Critical commands (like firmware updates) need guaranteed delivery.

### Solution
API stores messages for offline clients in a persistent queue (database/Redis). When the client reconnects with the same `clientID`, queued messages are delivered.

### Protocol Changes

**Add session configuration** in `pkg/message/types.go`:

```go
// SessionConfig sent during connection handshake
type SessionConfig struct {
    ClientID     string `json:"client_id"`      // Persistent client identifier
    CleanSession bool   `json:"clean_session"`  // true = discard old messages
}
```

### Database Schema

```sql
-- PostgreSQL
CREATE TABLE message_queue (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    message JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    priority INT NOT NULL DEFAULT 0,  -- Higher = more important
    retries INT NOT NULL DEFAULT 0,
    INDEX idx_client_expires (client_id, expires_at, priority)
);

-- Cleanup expired messages (run periodically)
CREATE INDEX idx_expires ON message_queue(expires_at);
```

**Redis Alternative:**
```
Key pattern: queue:{clientID}
Value: List of serialized messages
TTL: 24 hours (auto-expire)
```

### API Implementation

```go
// PersistentQueue stores messages for offline clients
type PersistentQueue struct {
    db     *sql.DB
    logger *log.Entry
}

// Enqueue message for offline client
func (pq *PersistentQueue) Enqueue(clientID string, msg any, priority int, ttl time.Duration) error {
    data, err := json.Marshal(msg)
    if err != nil {
        return err
    }

    query := `
        INSERT INTO message_queue (client_id, message, expires_at, priority)
        VALUES ($1, $2, $3, $4)
    `
    expiresAt := time.Now().Add(ttl)

    _, err = pq.db.Exec(query, clientID, data, expiresAt, priority)
    return err
}

// Dequeue all messages for client
func (pq *PersistentQueue) Dequeue(clientID string, maxMessages int) ([]any, error) {
    query := `
        SELECT id, message FROM message_queue
        WHERE client_id = $1 AND expires_at > $2
        ORDER BY priority DESC, created_at ASC
        LIMIT $3
    `

    rows, err := pq.db.Query(query, clientID, time.Now(), maxMessages)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var messages []any
    var ids []int64

    for rows.Next() {
        var id int64
        var data []byte
        if err := rows.Scan(&id, &data); err != nil {
            continue
        }

        var msg any
        if err := json.Unmarshal(data, &msg); err != nil {
            pq.logger.WithError(err).Warn("Failed to unmarshal queued message")
            continue
        }

        messages = append(messages, msg)
        ids = append(ids, id)
    }

    // Delete delivered messages
    if len(ids) > 0 {
        placeholders := make([]string, len(ids))
        args := make([]any, len(ids))
        for i, id := range ids {
            placeholders[i] = fmt.Sprintf("$%d", i+1)
            args[i] = id
        }
        deleteQuery := fmt.Sprintf("DELETE FROM message_queue WHERE id IN (%s)",
            strings.Join(placeholders, ","))
        _, err = pq.db.Exec(deleteQuery, args...)
        if err != nil {
            pq.logger.WithError(err).Warn("Failed to delete delivered messages")
        }
    }

    return messages, nil
}

// GetQueueSize returns number of queued messages for client
func (pq *PersistentQueue) GetQueueSize(clientID string) (int, error) {
    var count int
    query := `SELECT COUNT(*) FROM message_queue WHERE client_id = $1 AND expires_at > $2`
    err := pq.db.QueryRow(query, clientID, time.Now()).Scan(&count)
    return count, err
}
```

### Connection Flow

```go
// Device connects with session config
func (api *API) handleDeviceConnect(deviceID string, session SessionConfig) {
    if session.CleanSession {
        // Clear all queued messages
        api.persistentQueue.Clear(session.ClientID)
    } else {
        // Deliver queued messages
        messages, err := api.persistentQueue.Dequeue(session.ClientID, 100)
        if err != nil {
            api.logger.WithError(err).Error("Failed to dequeue messages")
            return
        }

        api.logger.WithFields(log.Fields{
            "client_id": session.ClientID,
            "count":     len(messages),
        }).Info("Delivering queued messages")

        for _, msg := range messages {
            api.sendToDevice(deviceID, msg)
        }
    }
}

// When routing message to device
func (api *API) routeToDevice(deviceID string, msg any) error {
    deviceSession := api.getDeviceSession(deviceID)

    if deviceSession == nil || deviceSession.IsClosed() {
        // Device offline - enqueue message
        clientID := api.getClientID(deviceID)
        priority := calculatePriority(msg)
        ttl := calculateTTL(msg)

        return api.persistentQueue.Enqueue(clientID, msg, priority, ttl)
    }

    // Device online - send immediately
    return deviceSession.SendMessage(msg)
}
```

### Priority and TTL Calculation

```go
func calculatePriority(msg any) int {
    switch m := msg.(type) {
    case RequestMessage:
        action := string(m.Action)

        // Critical commands
        if strings.Contains(action, "firmware.update") {
            return 100
        }
        if strings.Contains(action, "system.restart") {
            return 90
        }
        if strings.HasPrefix(action, "webrtc.") {
            return 80  // WebRTC signaling is time-sensitive but important
        }

        // Normal commands
        return 50

    case EventMessage:
        // Events are generally lower priority
        return 10
    }

    return 0
}

func calculateTTL(msg any) time.Duration {
    switch m := msg.(type) {
    case RequestMessage:
        action := string(m.Action)

        // Long TTL for important commands
        if strings.Contains(action, "firmware") {
            return 24 * time.Hour
        }
        if strings.Contains(action, "system") {
            return 12 * time.Hour
        }

        // Short TTL for time-sensitive messages
        if strings.HasPrefix(action, "webrtc.") {
            return 5 * time.Minute
        }

        // Default
        return 1 * time.Hour
    }

    return 30 * time.Minute
}
```

### Usage Examples

**Device connects with persistent session:**
```go
// Agent maintains consistent clientID across restarts
config := message.SessionConfig{
    ClientID:     "device-123-abc",  // Stable ID (stored in config)
    CleanSession: false,             // Keep queued messages
}

client.Connect(config)
// API delivers any messages queued during offline period
```

**Device connects with clean session:**
```go
config := message.SessionConfig{
    ClientID:     generateNewClientID(),
    CleanSession: true,  // Discard old messages
}

client.Connect(config)
// Fresh start, no queued messages
```

**Web client sends command to offline device:**
```go
// Web sends restart command
await device.system.restart();

// Device is offline
// → API enqueues command with priority=90, ttl=12h
// → Device reconnects 10 minutes later
// → API delivers queued restart command
// → Device restarts
```

---

## Architecture Diagram

```
┌─────────┐                 ┌──────────────────────────────────────────┐                 ┌────────┐
│   Web   │                 │           API Server                     │                 │ Device │
│ Client  │                 │                                          │                 │ Agent  │
└────┬────┘                 │  ┌─────────────────┐                    │                 └───┬────┘
     │                      │  │ RetainedMessage │                    │                      │
     │                      │  │     Store       │                    │                      │
     │                      │  └─────────────────┘                    │                      │
     │                      │  ┌─────────────────┐                    │                      │
     │                      │  │  Will Registry  │                    │                      │
     │                      │  └─────────────────┘                    │                      │
     │                      │  ┌─────────────────┐                    │                      │
     │                      │  │ PersistentQueue │                    │                      │
     │                      │  │  (PostgreSQL)   │                    │                      │
     │                      │  └─────────────────┘                    │                      │
     │                      └──────────────┬───────────────────────────┘                      │
     │                                     │                                                  │
     │  ① Device connects                  │                                                  │
     │                                     │◄─────────────────────────────────────────────────┤
     │                                     │  SessionConfig{clientID, cleanSession}           │
     │                                     │  WillMessage{action, payload, destination}       │
     │                                     │                                                  │
     │                                     ├──► Check PersistentQueue for clientID           │
     │                                     ├──► Deliver 5 queued messages ─────────────────►│
     │                                     ├──► Register will in WillRegistry                │
     │                                     │                                                  │
     │  ② Device sends retained state      │                                                  │
     │                                     │◄─────────────────────────────────────────────────┤
     │                                     │  EventMessage{retained: true, action: "battery"} │
     │                                     │                                                  │
     │                                     ├──► Store in RetainedMessageStore                │
     │◄────────────────────────────────────┤  Forward to web                                 │
     │  EventMessage{battery: 85%}         │                                                  │
     │                                     │                                                  │
     │  ③ New web client connects          │                                                  │
     ├────────────────────────────────────►│                                                  │
     │                                     ├──► Fetch all retained for device                │
     │◄────────────────────────────────────┤  from RetainedMessageStore                      │
     │  EventMessage{battery: 85%}         │                                                  │
     │  EventMessage{gps: {...}}           │                                                  │
     │  EventMessage{status: online}       │                                                  │
     │                                     │                                                  │
     │  ④ Device disconnects unexpectedly  │                                                  │
     │                                     │  ╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳╳   │
     │                                     │                                                  │
     │                                     ├──► Detect ungraceful close                       │
     │                                     ├──► Execute will from WillRegistry                │
     │◄────────────────────────────────────┤  Send to destination (broadcast)                │
     │  EventMessage{status: "offline",    │                                                  │
     │    reason: "connection_lost"}       │                                                  │
     │                                     │                                                  │
     │  ⑤ Web sends command (device offline)                                                  │
     ├────────────────────────────────────►│                                                  │
     │  RequestMessage{action: "restart",  │                                                  │
     │    destination: "device"}           │                                                  │
     │                                     │                                                  │
     │                                     ├──► Device offline, enqueue in PersistentQueue   │
     │                                     │    {clientID, message, priority: 90, ttl: 12h}  │
     │◄────────────────────────────────────┤                                                  │
     │  ResponseMessage{queued: true}      │                                                  │
     │                                     │                                                  │
     │  ⑥ Device reconnects (10 min later) │                                                  │
     │                                     │◄─────────────────────────────────────────────────┤
     │                                     │  SessionConfig{clientID, cleanSession: false}    │
     │                                     │                                                  │
     │                                     ├──► Dequeue from PersistentQueue                  │
     │                                     ├──► Deliver restart command ─────────────────────►│
     │                                     │                                                  │
     │                                     │◄─────────────────────────────────────────────────┤
     │◄────────────────────────────────────┤  ResponseMessage{success: true}                 │
     │                                     │  (device restarting...)                          │
     │                                     │                                                  │
```

---

## Implementation Phases

### Phase 1: Retained Messages (1-2 days)

| Task | Files | Effort |
|------|-------|--------|
| Add `Retained bool` to EventMessage | `pkg/message/types.go` | 5 min |
| Create RetainedMessageStore | `internal/api/retained_store.go` | 2 hours |
| Store retained on receive | `internal/api/message_handler.go` | 1 hour |
| Send retained on web connect | `internal/api/session_handler.go` | 2 hours |
| Add unit tests | `internal/api/retained_store_test.go` | 2 hours |
| Add integration tests | `test/integration/retained_test.go` | 2 hours |
| Update documentation | `MESSAGE_ROUTING.md` | 1 hour |

**Deliverable:** Web clients see current device state immediately

### Phase 2: Last Will and Testament (2-3 days)

| Task | Files | Effort |
|------|-------|--------|
| Add WillMessage type | `pkg/message/types.go` | 10 min |
| Add TypeWill constant | `pkg/message/types.go` | 5 min |
| Add RegisterWill/ClearWill to interface | `pkg/message/client.go` | 30 min |
| Implement RegisterWill in client | `pkg/message/client.go` | 1 hour |
| Create WillRegistry | `internal/api/will_registry.go` | 2 hours |
| Handle will registration in API | `internal/api/connection_handler.go` | 2 hours |
| Execute will on disconnect | `internal/api/disconnect_handler.go` | 2 hours |
| Add unit tests | `internal/api/will_registry_test.go` | 2 hours |
| Add integration tests | `test/integration/will_test.go` | 3 hours |
| Update documentation | `MESSAGE_ROUTING.md` | 1 hour |

**Deliverable:** Automatic disconnect notifications

### Phase 3: Persistent Sessions (3-5 days)

| Task | Files | Effort |
|------|-------|--------|
| Add SessionConfig type | `pkg/message/types.go` | 10 min |
| Design database schema | `migrations/` | 1 hour |
| Create PersistentQueue interface | `internal/api/queue/interface.go` | 1 hour |
| Implement PostgreSQL queue | `internal/api/queue/postgres.go` | 4 hours |
| Implement Redis queue (optional) | `internal/api/queue/redis.go` | 3 hours |
| Add priority/TTL calculation | `internal/api/queue/priority.go` | 2 hours |
| Handle session config on connect | `internal/api/connection_handler.go` | 2 hours |
| Enqueue for offline devices | `internal/api/message_router.go` | 2 hours |
| Dequeue on reconnect | `internal/api/connection_handler.go` | 2 hours |
| Add cleanup job for expired | `internal/api/queue/cleanup.go` | 2 hours |
| Add unit tests | `internal/api/queue/*_test.go` | 4 hours |
| Add integration tests | `test/integration/persistent_test.go` | 4 hours |
| Update documentation | `MESSAGE_ROUTING.md` | 2 hours |

**Deliverable:** Messages survive restarts

### Phase 4: Documentation & Examples (1 day)

| Task | Files | Effort |
|------|-------|--------|
| Update MESSAGE_ROUTING.md | `MESSAGE_ROUTING.md` | 2 hours |
| Add usage examples | `docs/examples/` | 2 hours |
| Write migration guide | `docs/MIGRATION.md` | 2 hours |
| Update API documentation | `docs/API.md` | 1 hour |
| Create runbook for operations | `docs/OPERATIONS.md` | 1 hour |

---

## Storage Options Comparison

### Option A: In-Memory (Development/Testing)

```go
type InMemoryStore struct {
    retained map[string]EventMessage
    wills    map[string]WillMessage
    queue    map[string][]any
    mutex    sync.RWMutex
}
```

**Pros:**
- ✅ Zero dependencies
- ✅ Fast development
- ✅ Simple debugging

**Cons:**
- ❌ Lost on restart
- ❌ No clustering
- ❌ Memory limits

**Use for:** Local development, unit tests

### Option B: Redis (Recommended for Production)

```go
type RedisStore struct {
    client *redis.Client
}

// Retained: Hash per device
// Key: retained:{deviceID}
// Field: action -> JSON message

// Will: Hash with TTL
// Key: will:{clientID} -> JSON message

// Queue: Sorted Set (score = priority + timestamp)
// Key: queue:{clientID}
// Member: JSON message
```

**Pros:**
- ✅ Persistent
- ✅ Fast (in-memory)
- ✅ Clustering support
- ✅ Built-in TTL
- ✅ Atomic operations

**Cons:**
- ⚠️ Adds Redis dependency
- ⚠️ Memory limits (less than disk)

**Use for:** Production, horizontal scaling

### Option C: PostgreSQL (Long-term Storage)

See schema in "Persistent Sessions" section above.

**Pros:**
- ✅ Durable
- ✅ No memory limits
- ✅ Queryable (analytics)
- ✅ Transactional

**Cons:**
- ⚠️ Slower than Redis
- ⚠️ More complex

**Use for:** Persistent queue with long TTLs

### Recommended Hybrid Approach

```
┌─────────────────┬──────────────────┬─────────────────┐
│    Feature      │   Storage        │    Backup       │
├─────────────────┼──────────────────┼─────────────────┤
│ Retained Msgs   │ Redis Hash       │ Snapshot to DB  │
│ Will Registry   │ Redis Hash+TTL   │ None (ephemeral)│
│ Persist Queue   │ PostgreSQL       │ WAL replication │
└─────────────────┴──────────────────┴─────────────────┘
```

**Why:**
- Retained messages: Fast access (Redis), periodic backup to DB
- Will registry: Ephemeral by nature, Redis TTL perfect fit
- Persistent queue: Needs durability, PostgreSQL handles it

---

## Configuration

```go
// In API config
type ProtocolConfig struct {
    // Retained messages
    RetainedMessagesEnabled bool
    RetainedTTL             time.Duration  // Default: 24h
    MaxRetainedPerDevice    int            // Default: 100

    // Last will
    LastWillEnabled         bool
    WillExecutionDelay      time.Duration  // Default: 100ms (debounce)

    // Persistent sessions
    PersistentSessionsEnabled bool
    QueueStorage              string         // "memory", "redis", "postgres"
    DefaultMessageTTL         time.Duration  // Default: 1h
    MaxQueueSizePerClient     int            // Default: 1000
    QueueCleanupInterval      time.Duration  // Default: 5m
}
```

**Environment Variables:**
```bash
# Retained messages
PROTOCOL_RETAINED_ENABLED=true
PROTOCOL_RETAINED_TTL=24h
PROTOCOL_MAX_RETAINED_PER_DEVICE=100

# Last will
PROTOCOL_LAST_WILL_ENABLED=true
PROTOCOL_WILL_EXECUTION_DELAY=100ms

# Persistent sessions
PROTOCOL_PERSISTENT_SESSIONS_ENABLED=true
PROTOCOL_QUEUE_STORAGE=redis  # or: memory, postgres
PROTOCOL_DEFAULT_MESSAGE_TTL=1h
PROTOCOL_MAX_QUEUE_SIZE_PER_CLIENT=1000
PROTOCOL_QUEUE_CLEANUP_INTERVAL=5m

# Redis (if using Redis storage)
REDIS_URL=redis://localhost:6379/0

# PostgreSQL (if using Postgres storage)
DATABASE_URL=postgresql://user:pass@localhost/aircast
```

---

## Testing Strategy

### Unit Tests

```go
// Test retained message store
func TestRetainedMessageStore(t *testing.T) {
    store := NewRetainedMessageStore()

    // Store message
    msg := EventMessage{Action: "battery", Payload: map[string]any{"level": 85}, Retained: true}
    store.Store("device-123", msg)

    // Retrieve
    messages := store.GetAll("device-123")
    assert.Equal(t, 1, len(messages))
    assert.Equal(t, "battery", messages[0].Action)

    // Update
    msg.Payload = map[string]any{"level": 80}
    store.Store("device-123", msg)
    messages = store.GetAll("device-123")
    assert.Equal(t, 1, len(messages))  // Still 1 (updated, not added)
    assert.Equal(t, 80, messages[0].Payload["level"])

    // Delete
    store.Delete("device-123", "battery")
    messages = store.GetAll("device-123")
    assert.Equal(t, 0, len(messages))
}

// Test will registry
func TestWillRegistry(t *testing.T) {
    registry := NewWillRegistry()

    will := WillMessage{Action: "device.offline", Payload: map[string]any{"reason": "crash"}}
    registry.Register("device-123", will)

    // Execute will
    executedWill := registry.Execute("device-123")
    assert.NotNil(t, executedWill)
    assert.Equal(t, "device.offline", executedWill.Action)

    // Can't execute twice
    executedWill = registry.Execute("device-123")
    assert.Nil(t, executedWill)
}

// Test persistent queue
func TestPersistentQueue(t *testing.T) {
    db := setupTestDB(t)
    queue := NewPostgresQueue(db)

    msg := RequestMessage{Action: "restart", RequestID: "req-123"}

    // Enqueue
    err := queue.Enqueue("device-123", msg, 90, 1*time.Hour)
    assert.NoError(t, err)

    // Get size
    size, err := queue.GetQueueSize("device-123")
    assert.NoError(t, err)
    assert.Equal(t, 1, size)

    // Dequeue
    messages, err := queue.Dequeue("device-123", 10)
    assert.NoError(t, err)
    assert.Equal(t, 1, len(messages))

    // Queue now empty
    size, err = queue.GetQueueSize("device-123")
    assert.Equal(t, 0, size)
}
```

### Integration Tests

```go
func TestRetainedMessagesIntegration(t *testing.T) {
    // 1. Start API server
    api := startTestAPIServer(t)
    defer api.Stop()

    // 2. Device connects and sends retained message
    device := connectTestDevice(t, api, "device-123")
    device.SendEvent(EventMessage{
        Action: "battery",
        Payload: map[string]any{"level": 85},
        Retained: true,
        Destination: DestinationWeb,
    })

    // 3. Web client connects
    web := connectTestWebClient(t, api, "device-123")

    // 4. Verify web client receives retained message immediately
    msg := <-web.Messages()
    assert.Equal(t, "battery", msg.Action)
    assert.Equal(t, 85, msg.Payload["level"])
}

func TestLastWillIntegration(t *testing.T) {
    api := startTestAPIServer(t)
    defer api.Stop()

    // Device connects with will
    device := connectTestDevice(t, api, "device-123")
    device.RegisterWill(WillMessage{
        Action: "device.offline",
        Payload: map[string]any{"reason": "crash"},
        Destination: DestinationBroadcast,
    })

    // Web client connects
    web := connectTestWebClient(t, api, "device-123")

    // Device disconnects unexpectedly
    device.ForceClose()  // Simulate crash

    // Verify web client receives will message
    msg := <-web.Messages()
    assert.Equal(t, "device.offline", msg.Action)
    assert.Equal(t, "crash", msg.Payload["reason"])
}

func TestPersistentSessionsIntegration(t *testing.T) {
    api := startTestAPIServer(t)
    defer api.Stop()

    // Device connects with persistent session
    device := connectTestDevice(t, api, "device-123")
    device.SetSessionConfig(SessionConfig{
        ClientID: "device-123-persistent",
        CleanSession: false,
    })

    // Device goes offline
    device.Close()

    // Web sends command while device offline
    web := connectTestWebClient(t, api, "device-123")
    web.SendRequest(RequestMessage{
        Action: "system.restart",
        RequestID: "req-123",
        Destination: DestinationDevice,
    })

    // Device reconnects with same clientID
    device = connectTestDevice(t, api, "device-123")
    device.SetSessionConfig(SessionConfig{
        ClientID: "device-123-persistent",
        CleanSession: false,
    })

    // Verify device receives queued command
    msg := <-device.Messages()
    assert.Equal(t, "system.restart", msg.Action)
}
```

---

## Monitoring & Observability

### Metrics

```go
// Retained messages
retained_messages_total{device_id}      // Total retained messages per device
retained_messages_stored_total          // Counter: messages stored
retained_messages_served_total          // Counter: messages sent to new clients

// Last will
will_registered_total                   // Counter: wills registered
will_executed_total                     // Counter: wills executed
will_cleared_total                      // Counter: wills cleared (graceful)

// Persistent sessions
queue_size{client_id}                   // Current queue size per client
queue_enqueued_total{priority}          // Counter: messages enqueued by priority
queue_dequeued_total                    // Counter: messages dequeued
queue_expired_total                     // Counter: messages expired
queue_delivery_latency_seconds          // Histogram: time from enqueue to delivery
```

### Logs

```go
// Retained messages
logger.WithFields(log.Fields{
    "device_id": deviceID,
    "action": msg.Action,
    "retained": true,
}).Info("Stored retained message")

// Last will
logger.WithFields(log.Fields{
    "client_id": clientID,
    "will_action": will.Action,
    "disconnect_type": "unexpected",
}).Warn("Executing last will")

// Persistent sessions
logger.WithFields(log.Fields{
    "client_id": clientID,
    "queue_size": size,
    "delivered": len(messages),
}).Info("Delivered queued messages on reconnect")
```

### Health Checks

```go
// GET /health/protocol
{
    "retained_messages": {
        "enabled": true,
        "total_devices": 42,
        "total_messages": 156
    },
    "last_will": {
        "enabled": true,
        "registered": 42
    },
    "persistent_sessions": {
        "enabled": true,
        "storage": "redis",
        "total_queued": 12,
        "oldest_message_age_seconds": 120
    }
}
```

---

## Migration Guide

### For Existing Deployments

#### Step 1: Deploy with features disabled
```bash
PROTOCOL_RETAINED_ENABLED=false
PROTOCOL_LAST_WILL_ENABLED=false
PROTOCOL_PERSISTENT_SESSIONS_ENABLED=false
```

#### Step 2: Update device agents
Deploy new agent version with protocol support (backward compatible).

#### Step 3: Enable retained messages
```bash
PROTOCOL_RETAINED_ENABLED=true
```

Devices will automatically start sending retained messages. No code changes needed if using default `Retained: false`.

#### Step 4: Enable last will
```bash
PROTOCOL_LAST_WILL_ENABLED=true
```

Update device connection code to register will:
```go
client.RegisterWill(WillMessage{...})
```

#### Step 5: Setup persistent storage
```bash
# If using PostgreSQL
docker run -d -p 5432:5432 postgres:15
# Run migration
psql < migrations/001_message_queue.sql

# If using Redis
docker run -d -p 6379:6379 redis:7
```

#### Step 6: Enable persistent sessions
```bash
PROTOCOL_PERSISTENT_SESSIONS_ENABLED=true
PROTOCOL_QUEUE_STORAGE=postgres  # or redis
DATABASE_URL=postgresql://...
```

Update device connection to use persistent clientID:
```go
client.Connect(SessionConfig{
    ClientID: loadOrGeneratePersistentID(),
    CleanSession: false,
})
```

### Backward Compatibility

All features are **backward compatible**:

- Retained messages: `Retained` field defaults to `false`
- Last will: Devices without will registration work normally
- Persistent sessions: Without `SessionConfig`, treated as `CleanSession: true`

Old clients continue working without protocol changes.

---

## Performance Considerations

### Retained Messages

**Memory usage:** ~500 bytes per retained message
- 100 devices × 10 retained messages = 500 KB
- Negligible for most deployments

**CPU impact:** Minimal
- Store: O(1) hash insert
- Retrieve: O(n) where n = retained messages per device (~10)

### Last Will

**Memory usage:** ~200 bytes per will
- 1000 devices = 200 KB
- Negligible

**CPU impact:** Minimal
- Register: O(1)
- Execute: O(1)

### Persistent Sessions

**Storage:** ~1 KB per queued message
- 1000 queued messages = 1 MB
- PostgreSQL or Redis handles this easily

**Query performance:**
```sql
-- Optimized with index
SELECT ... WHERE client_id = ? AND expires_at > ?
ORDER BY priority DESC, created_at ASC
LIMIT 100;

-- Index: (client_id, expires_at, priority)
-- Query time: <1ms for typical queue sizes
```

**Cleanup performance:**
```sql
-- Run every 5 minutes
DELETE FROM message_queue WHERE expires_at < NOW();

-- With index on expires_at: <10ms for thousands of rows
```

---

## Security Considerations

### Retained Messages

**Issue:** Sensitive data in retained messages
**Solution:**
```go
// Don't retain sensitive data
if containsSensitiveData(msg.Payload) {
    msg.Retained = false
}
```

### Last Will

**Issue:** Malicious device registering fake will
**Solution:**
```go
// Validate will message
if will.Destination == DestinationBroadcast {
    // Only allow specific actions
    allowedActions := []string{"device.status", "device.offline"}
    if !contains(allowedActions, will.Action) {
        return ErrUnauthorizedWillAction
    }
}
```

### Persistent Sessions

**Issue:** Queue flooding attack
**Solution:**
```go
// Rate limit message queueing
if queue.GetQueueSize(clientID) > MaxQueueSizePerClient {
    return ErrQueueFull
}

// Limit message size
if len(messageJSON) > MaxMessageSize {
    return ErrMessageTooLarge
}
```

**Issue:** Client ID spoofing
**Solution:**
```go
// Bind clientID to device authentication
clientID := generateClientID(deviceAuth.DeviceID, deviceAuth.Secret)
// Or use certificate-based clientID
```

---

## Future Enhancements

### 1. Shared Subscriptions (Load Balancing)
```go
// Multiple web clients share load
client1.Subscribe("$share/dashboard/device/+/telemetry")
client2.Subscribe("$share/dashboard/device/+/telemetry")
// Messages distributed between client1 and client2
```

### 2. Topic-Based Routing
```go
// Combine destination + topic patterns
EventMessage{
    Destination: "web",
    Topic: "device.123.telemetry.battery",  // NEW
}

// Web clients subscribe to topics
web.Subscribe("device.*.telemetry.*")
```

### 3. Message Replay
```go
// Request historical messages
client.Replay(
    action: "mavlink.telemetry",
    since: time.Now().Add(-1 * time.Hour),
    limit: 1000,
)
```

### 4. Quality of Service Levels
```go
EventMessage{
    QoS: 2,  // 0=at most once, 1=at least once, 2=exactly once
}
```

---

## Summary

These three enhancements bring MQTT-like reliability to our custom protocol:

| Feature | Benefit | Complexity | Value |
|---------|---------|------------|-------|
| Retained Messages | Instant state for new clients | Low | High |
| Last Will | Auto disconnect notifications | Medium | High |
| Persistent Sessions | Survive restarts | High | Critical |

**Total implementation time:** 7-10 days

**Much faster than migrating to MQTT:** 2-3 weeks (with risks)

**Maintains our advantages:**
- Simple destination-based routing
- Built-in request-response
- Full control over protocol

**Next steps:**
1. Review and approve design
2. Setup development environment (Redis/PostgreSQL)
3. Implement Phase 1 (Retained Messages)
4. Deploy and test in staging
5. Proceed to Phase 2 and 3
