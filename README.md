# gochannel

[![Go Reference](https://pkg.go.dev/badge/github.com/bagussdw/gochannel.svg)](https://pkg.go.dev/github.com/bagussdw/gochannel)
[![Go Report Card](https://goreportcard.com/badge/github.com/bagussdw/gochannel)](https://goreportcard.com/report/github.com/bagussdw/gochannel)

**gochannel** is a generic, typed publish/subscribe (pub/sub) message bus for Go.

It allows producers and consumers to communicate through topics without being directly coupled to each other. The library is fully generic, typesafe, and thread-safe, with **zero external dependencies**.

## Features

- ✅ **Generic & Type-Safe** — Full Go generics support (`Channel[K comparable, V any]`)
- ✅ **Zero Dependencies** — Pure standard library, no external packages required
- ✅ **Thread-Safe** — All operations are safe for concurrent use
- ✅ **Buffered Channels** — Configurable buffer size per subscriber
- ✅ **Topic-Based** — Subscribe to specific topics, broadcast to all
- ✅ **Non-Blocking Publish** — Publishers never block, slow subscribers drop messages
- ✅ **Panic-Safe** — Gracefully handles closed channels and dead subscribers
- ✅ **Clean Shutdown** — `Close()` method to properly tear down all subscribers

## Installation

```bash
go get github.com/robby031/gochannel
```

## Quick Start

```go
package main

import (
    "fmt"
    "time"
    "github.com/robby031/gochannel"
)

func main() {
    // Create a channel with string keys and string values
    ch := gochannel.New[string, string]()

    // Subscribe to a topic
    sub := ch.Subscribe("orders")
    defer ch.Unsubscribe("orders", sub)

    // Publish messages in a goroutine
    go func() {
        ch.Publish("orders", "New order received!")
        ch.Publish("orders", "Order #1234 completed")
    }()

    // Receive messages
    for msg := range sub {
        fmt.Println("Received:", msg)
        if msg == "Order #1234 completed" {
            break
        }
    }
}
```

## API Reference

### Creating a Channel

```go
// String keys, string values
ch := gochannel.New[string, string]()

// Int keys, custom struct values
type Event struct { ID int; Name string }
ch := gochannel.New[int, Event]()

// String keys, pointer values
ch := gochannel.New[string, *MyStruct]()
```

### Subscribing

```go
// Subscribe with default buffer size (10)
sub := ch.Subscribe("topic")

// Subscribe with custom buffer size
sub := ch.SubscribeWithBuffer("topic", 100)

// sub is a receive-only channel (<-chan V)
go func() {
    for msg := range sub {
        fmt.Println(msg)
    }
}()
```

### Publishing

```go
// Publish to a specific topic
ch.Publish("topic", "message")

// Broadcast to all topics
ch.Broadcast("system-wide notification")

// Broadcast to all topics except one
ch.BroadcastExcept("topic", "secret message")
```

### Topic Management

```go
// Check if a topic has subscribers
if ch.HasSubscribers("topic") {
    // ...
}

// Get subscriber count for a topic
count := ch.SubscriberCount("topic")

// Get total number of active topics
topics := ch.TopicCount()
```

### Cleanup

```go
// Unsubscribe a specific subscriber
ch.Unsubscribe("topic", sub)

// Close all subscribers across all topics
ch.Close()
```

## Advanced Examples

### Multiple Subscribers per Topic

```go
ch := gochannel.New[string, string]()

sub1 := ch.Subscribe("broadcast")
sub2 := ch.Subscribe("broadcast")
defer ch.Unsubscribe("broadcast", sub1)
defer ch.Unsubscribe("broadcast", sub2)

ch.Publish("broadcast", "Message to all subscribers")
```

### Struct Messages

```go
type OrderEvent struct {
    OrderID   string
    Status    string
    Timestamp time.Time
}

ch := gochannel.New[string, OrderEvent]()
sub := ch.Subscribe("orders")
defer ch.Unsubscribe("orders", sub)

go func() {
    ch.Publish("orders", OrderEvent{
        OrderID: "ORD-001",
        Status:  "completed",
    })
}()

event := <-sub
fmt.Printf("Order %s is %s\n", event.OrderID, event.Status)
```

### Non-Blocking Broadcast Pattern

```go
type UpdateEvent struct {
    Event string
    Data  interface{}
}

// Simulating the pattern from a real gateway application
bus := gochannel.New[string, *UpdateEvent]()

// WebSocket client subscribes
clientCh := bus.Subscribe("ws-client-1")

// Notification service broadcasts
bus.Publish("ws-client-1", &UpdateEvent{
    Event: "transaction_updated",
    Data:  map[string]interface{}{"status": "paid"},
})
```

## When to Use gochannel

gochannel is ideal for:

- **Event-driven architectures** within a single process
- **Real-time notification systems** (WebSocket broadcast, status updates)
- **In-process message passing** between goroutines/workers
- **Plugin/hook systems** where multiple components react to events
- **Decoupling producers from consumers** in medium-complexity Go applications

For distributed pub/sub across processes or machines, consider using message brokers like NATS, RabbitMQ, or Redis Pub/Sub instead.

## Performance

gochannel is designed for low-latency, in-process communication. A rough benchmark:

- Publish to a topic with 1 subscriber: ~200-300ns
- Publish to a topic with 10 subscribers: ~1-3μs
- Broadcast to 10 topics: ~5-10μs

The library is non-blocking by design: publishers never wait for slow consumers. If a subscriber's channel buffer is full, the message is dropped for that subscriber.

## License

MIT
