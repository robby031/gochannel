# gochannel

[![Go Reference](https://pkg.go.dev/badge/github.com/robby031/gochannel.svg)](https://pkg.go.dev/github.com/robby031/gochannel)
[![Go Report Card](https://goreportcard.com/badge/github.com/robby031/gochannel)](https://goreportcard.com/report/github.com/robby031/gochannel)

**gochannel** is a generic, typed publish/subscribe (pub/sub) message bus for Go. It allows producers and consumers to communicate through topics without being directly coupled to each other. The library is fully generic, typesafe, and thread-safe, with **zero external dependencies**.

## Features

- **Generic & Type-Safe** — Full Go generics support (`Channel[K comparable, V any]`)
- **Zero Dependencies** — Pure standard library, no external packages required
- **Thread-Safe** — All operations are safe for concurrent use
- **Context-Aware Subscriptions** — `SubscribeCtx` automatically cleans up subscriptions when context cancels to prevent goroutine leaks
- **Drop Monitoring** — Built-in atomic counter (`Dropped`) and `OnDrop` callback for logging discarded messages
- **Explicit Closed State** — Prevents new subscriptions after `Close()` and gracefully handles double-close
- **Buffered Channels** — Configurable buffer size per subscriber (buffer size must be > 0)
- **Topic-Based** — Subscribe to specific topics, broadcast to all
- **Non-Blocking Publish** — Publishers never block, slow subscribers drop messages safely

## Installation

```bash
go get github.com/robby031/gochannel
```

## Quick Start

```go
package main

import (
	"fmt"

	"github.com/robby031/gochannel"
)

func main() {
	// Create a channel with string keys and string values
	ch := gochannel.New[string, string]()

	// Optional: monitor dropped messages when subscriber buffers are full
	ch.OnDrop = func(topic string, msg string) {
		fmt.Printf("Warning: message dropped on topic '%s': %s\n", topic, msg)
	}

	// Subscribe to a topic
	sub := ch.Subscribe("orders")
	defer ch.Unsubscribe("orders", sub)

	// Publish messages
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
```

### Subscribing

Note: Buffer size must always be greater than `0`. Subscribing with a buffer size `<= 0` will panic to prevent guaranteed message drops.

```go
// Subscribe with default buffer size (10)
sub := ch.Subscribe("topic")

// Subscribe with custom buffer size (> 0 required)
sub := ch.SubscribeWithBuffer("topic", 100)

// Context-aware subscribe (automatically unsubscribes when ctx is cancelled)
sub := ch.SubscribeCtx(ctx, "topic")
sub := ch.SubscribeCtxWithBuffer(ctx, "topic", 100)
```

### Publishing & Drop Monitoring

```go
// Publish to a specific topic
ch.Publish("topic", "message")

// Broadcast to all topics
ch.Broadcast("system-wide notification")

// Broadcast to all topics except one
ch.BroadcastExcept("topic", "secret message")

// Check total dropped messages across all subscribers
totalDropped := ch.Dropped.Load()

// Register a callback for dropped messages
ch.OnDrop = func(topic string, msg MyMessageType) {
    log.Printf("[DROP WARNING] Topic: %v, Msg: %v", topic, msg)
}
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

### Cleanup & Lifecycle

```go
// Unsubscribe a specific subscriber
ch.Unsubscribe("topic", sub)

// Close channel and all subscriber channels across all topics.
// Subscribing to a closed channel will panic.
ch.Close()
```

## Advanced Examples

### Preventing Goroutine Leaks with Context

```go
func handleUserSession(ctx context.Context, bus *gochannel.Channel[string, string], userID string) {
    // Automatically unsubscribes when ctx is done (HTTP request cancel, timeout, etc.)
    sub := bus.SubscribeCtx(ctx, "user-events-"+userID)

    for {
        select {
        case <-ctx.Done():
            return
        case msg, ok := <-sub:
            if !ok {
                return // Channel closed by bus.Close()
            }
            fmt.Printf("User event: %s\n", msg)
        }
    }
}
```

### Monitoring Drop Rates under Heavy Load

```go
bus := gochannel.New[string, EventData]()

bus.OnDrop = func(topic string, msg EventData) {
    metrics.Incr("pubsub.message_dropped", []string{"topic:" + topic})
}

// Check aggregate drops periodically
go func() {
    for range time.Tick(1 * time.Minute) {
        fmt.Printf("Total dropped messages in last minute: %d\n", bus.Dropped.Swap(0))
    }
}()
```

## License

MIT License