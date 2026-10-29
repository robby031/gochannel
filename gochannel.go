package gochannel

import (
	"context"
	"sync"
	"sync/atomic"
)

// Channel is a generic, typed publish/subscribe message bus.
//
// K is the topic key type (must be comparable, e.g. string, int).
// V is the message value type (any type).
type Channel[K comparable, V any] struct {
	mu     sync.RWMutex
	subs   map[K]*subscription[V]
	closed bool

	// Atomic counter to track total discarded messages
	Dropped atomic.Uint64
	// Optional callback that is executed when a message fails to send.
	OnDrop func(topic K, msg V)
}

// subscription holds all subscribers (channels) for a single topic.
type subscription[V any] struct {
	mu   sync.RWMutex
	subs map[chan V]struct{}
}

// New creates a new typed pub/sub channel.
func New[K comparable, V any]() *Channel[K, V] {
	return &Channel[K, V]{
		subs: make(map[K]*subscription[V]),
	}
}

// Subscribe registers a new subscriber for the given topic and returns a
// receive-only channel that will receive messages published to that topic.
//
// The returned channel is buffered (default buffer size is 10). Callers must
// read from the channel to prevent backpressure. Always call Unsubscribe
// when done to avoid goroutine leaks.
func (c *Channel[K, V]) Subscribe(topic K) <-chan V {
	return c.SubscribeWithBuffer(topic, 10)
}

// SubscribeWithBuffer is like Subscribe but allows specifying the channel
// buffer size. Use a larger buffer if the subscriber may process messages
// slower than they arrive.
func (c *Channel[K, V]) SubscribeWithBuffer(topic K, buffer int) <-chan V {
	if buffer <= 0 {
		panic("gochannel: buffer size must be greater than 0 to prevent guaranteed message drops")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Reject new subscribers if the channel is closed
	if c.closed {
		panic("gochannel: cannot subscribe to a closed channel")
	}

	sub, ok := c.subs[topic]
	if !ok {
		sub = &subscription[V]{subs: make(map[chan V]struct{})}
		c.subs[topic] = sub
	}

	ch := make(chan V, buffer)
	sub.add(ch)
	return ch
}

// SubscribeCtx creates a subscriber that will automatically unsubscribe when the context is finished.
func (c *Channel[K, V]) SubscribeCtx(ctx context.Context, topic K) <-chan V {
	return c.SubscribeCtxWithBuffer(ctx, topic, 10)
}

// SubscribeCtxWithBuffer is a version of SubscribeCtx with buffer size customization.
func (c *Channel[K, V]) SubscribeCtxWithBuffer(ctx context.Context, topic K, buffer int) <-chan V {
	ch := c.SubscribeWithBuffer(topic, buffer)

	go func() {
		<-ctx.Done()
		c.Unsubscribe(topic, ch)
	}()

	return ch
}

// Unsubscribe removes a subscriber channel from the given topic and closes
// the channel. After calling Unsubscribe, the caller must stop reading from
// the channel.
func (c *Channel[K, V]) Unsubscribe(topic K, ch <-chan V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sub, ok := c.subs[topic]
	if !ok {
		return
	}

	cch := c.channelLookup(sub, ch)
	// remove returns true when the subscription set is now empty, checked
	// atomically under sub.mu — eliminating the TOCTOU between remove and len.
	if sub.remove(cch) {
		delete(c.subs, topic)
	}
}

func (c *Channel[K, V]) channelLookup(sub *subscription[V], ch <-chan V) chan V {
	sub.mu.RLock()
	defer sub.mu.RUnlock()
	for cch := range sub.subs {
		if cch == ch {
			return cch
		}
	}
	return nil
}

// Publish sends a message to all subscribers of the given topic.
// It returns immediately (non-blocking). If a subscriber's channel is full,
// the message is dropped for that subscriber.
func (c *Channel[K, V]) Publish(topic K, msg V) {
	c.mu.RLock()
	if c.closed { // Prevent publishing if it is already closed
		c.mu.RUnlock()
		return
	}
	sub, ok := c.subs[topic]
	c.mu.RUnlock()

	if ok {
		c.notify(topic, sub, msg)
	}
}

// Broadcast sends a message to all subscribers across all topics.
func (c *Channel[K, V]) Broadcast(msg V) {
	c.mu.RLock()
	if c.closed || len(c.subs) == 0 {
		c.mu.RUnlock()
		return
	}

	type target struct {
		topic K
		sub   *subscription[V]
	}
	targets := make([]target, 0, len(c.subs))
	for topic, sub := range c.subs {
		targets = append(targets, target{topic: topic, sub: sub})
	}
	c.mu.RUnlock()

	for _, t := range targets {
		c.notify(t.topic, t.sub, msg)
	}
}

// BroadcastExcept sends a message to all subscribers across all topics
// except the given topic.
func (c *Channel[K, V]) BroadcastExcept(topic K, msg V) {
	c.mu.RLock()
	if c.closed || len(c.subs) == 0 {
		c.mu.RUnlock()
		return
	}

	type target struct {
		topic K
		sub   *subscription[V]
	}
	targets := make([]target, 0, len(c.subs))
	for id, sub := range c.subs {
		if id != topic {
			targets = append(targets, target{topic: id, sub: sub})
		}
	}
	c.mu.RUnlock()

	for _, t := range targets {
		c.notify(t.topic, t.sub, msg)
	}
}

func (c *Channel[K, V]) notify(topic K, sub *subscription[V], msg V) {
	sub.mu.RLock()
	var dead []chan V

	for ch := range sub.subs {
		func(ch chan V) {
			defer func() {
				if recover() != nil {
					dead = append(dead, ch)
				}
			}()

			select {
			case ch <- msg:
				// Message sent successfully
			default:
				// channel full, handle drop
				c.Dropped.Add(1)
				if c.OnDrop != nil {
					c.OnDrop(topic, msg)
				}
			}
		}(ch)
	}
	sub.mu.RUnlock()

	// Cleanup of dead channels
	if len(dead) > 0 {
		sub.mu.Lock()
		for _, ch := range dead {
			delete(sub.subs, ch)
		}
		sub.mu.Unlock()
	}
}

// HasSubscribers reports whether the given topic has any active subscribers.
func (c *Channel[K, V]) HasSubscribers(topic K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sub, ok := c.subs[topic]
	if !ok {
		return false
	}
	return sub.len() > 0
}

// SubscriberCount returns the number of active subscribers for the given topic.
// It returns 0 if the topic has no subscribers or doesn't exist.
func (c *Channel[K, V]) SubscriberCount(topic K) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sub, ok := c.subs[topic]
	if !ok {
		return 0
	}
	return sub.len()
}

// TopicCount returns the number of topics that have at least one subscriber.
func (c *Channel[K, V]) TopicCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.subs)
}

// Close removes all subscribers from all topics and closes all subscriber
// channels. After calling Close, the Channel should not be used.
func (c *Channel[K, V]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Prevent silent double-close
	if c.closed {
		return
	}
	c.closed = true

	for topic, sub := range c.subs {
		sub.close()
		delete(c.subs, topic)
	}
}

// subscription internal methods
func (s *subscription[V]) add(ch chan V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[ch] = struct{}{}
}

// remove deletes ch from the subscriber set and closes it.
// It returns true if the set is now empty, checked atomically under the lock
// to prevent a TOCTOU race with notify's dead-channel cleanup.
func (s *subscription[V]) remove(ch chan V) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ch != nil {
		if _, ok := s.subs[ch]; ok {
			delete(s.subs, ch)
			func() {
				defer func() { recover() }()
				close(ch)
			}()
		}
	}
	return len(s.subs) == 0
}

func (s *subscription[V]) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for ch := range s.subs {
		close(ch)
	}
	s.subs = nil
}

func (s *subscription[V]) len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subs)
}
