package gochannel

import "sync"

// Channel is a generic, typed publish/subscribe message bus.
//
// K is the topic key type (must be comparable, e.g. string, int).
// V is the message value type (any type).
type Channel[K comparable, V any] struct {
	mu   sync.RWMutex
	subs map[K]*subscription[V]
}

// subscription holds all subscribers (channels) for a single topic.
type subscription[V any] struct {
	mu   sync.RWMutex
	subs map[chan V]chan V
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
	c.mu.Lock()
	defer c.mu.Unlock()

	sub, ok := c.subs[topic]
	if !ok {
		sub = &subscription[V]{}
		c.subs[topic] = sub
	}

	ch := make(chan V, buffer)
	sub.add(ch)
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

	// Convert to writable chan for internal tracking
	cch := c.channelLookup(sub, ch)
	sub.remove(cch)
	if sub.len() == 0 {
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
// It returns immediately (non-blocking) using a goroutine per subscriber.
// If a subscriber's channel is full, the message is dropped for that subscriber.
func (c *Channel[K, V]) Publish(topic K, msg V) {
	c.mu.RLock()
	sub, ok := c.subs[topic]
	c.mu.RUnlock()

	if ok {
		sub.notify(msg)
	}
}

// Broadcast sends a message to all subscribers across all topics.
func (c *Channel[K, V]) Broadcast(msg V) {
	c.mu.RLock()
	subs := make([]*subscription[V], 0, len(c.subs))
	for _, sub := range c.subs {
		subs = append(subs, sub)
	}
	c.mu.RUnlock()

	for _, sub := range subs {
		sub.notify(msg)
	}
}

// BroadcastExcept sends a message to all subscribers across all topics
// except the given topic.
func (c *Channel[K, V]) BroadcastExcept(topic K, msg V) {
	c.mu.RLock()
	subs := make([]*subscription[V], 0, len(c.subs))
	for id, sub := range c.subs {
		if id != topic {
			subs = append(subs, sub)
		}
	}
	c.mu.RUnlock()

	for _, sub := range subs {
		sub.notify(msg)
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

	for topic, sub := range c.subs {
		sub.close()
		delete(c.subs, topic)
	}
}

// subscription internal methods

func (s *subscription[V]) add(ch chan V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.subs == nil {
		s.subs = map[chan V]chan V{ch: ch}
	} else {
		s.subs[ch] = ch
	}
}

func (s *subscription[V]) remove(ch chan V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ch == nil {
		return
	}
	if _, ok := s.subs[ch]; ok {
		delete(s.subs, ch)
		func() {
			if r := recover(); r != nil {
				// channel already closed
			}
		}()
		close(ch)
	}
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

// notify sends a message to all subscriber channels concurrently.
// It recovers from panics caused by sending to a closed channel and
// removes dead subscribers.
func (s *subscription[V]) notify(msg V) {
	s.mu.RLock()
	channels := make([]chan V, 0, len(s.subs))
	for ch := range s.subs {
		channels = append(channels, ch)
	}
	s.mu.RUnlock()

	var (
		dead     []chan V
		deadLock sync.Mutex
		wg       sync.WaitGroup
	)

	for _, ch := range channels {
		wg.Add(1)
		go func(ch chan V) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					deadLock.Lock()
					dead = append(dead, ch)
					deadLock.Unlock()
				}
			}()

			select {
			case ch <- msg:
			default:
				// channel full, drop message
			}
		}(ch)
	}

	wg.Wait()

	if len(dead) > 0 {
		s.mu.Lock()
		for _, ch := range dead {
			delete(s.subs, ch)
		}
		s.mu.Unlock()
	}
}
