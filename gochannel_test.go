package gochannel

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Basic Tests
func TestNew(t *testing.T) {
	ch := New[string, string]()
	if ch == nil {
		t.Fatal("New returned nil")
	}
	if ch.TopicCount() != 0 {
		t.Fatal("expected 0 topics")
	}
}

func TestSubscribeAndPublish(t *testing.T) {
	ch := New[string, string]()

	sub := ch.Subscribe("test")
	defer ch.Unsubscribe("test", sub)

	ch.Publish("test", "hello")

	select {
	case msg := <-sub:
		if msg != "hello" {
			t.Fatalf("expected 'hello', got '%s'", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestPublishToNonexistentTopic(t *testing.T) {
	ch := New[string, string]()

	// Should not panic
	ch.Publish("nonexistent", "data")
}

func TestUnsubscribe(t *testing.T) {
	ch := New[string, string]()

	sub := ch.Subscribe("test")
	ch.Unsubscribe("test", sub)

	// Should not panic
	ch.Publish("test", "data")

	select {
	case _, ok := <-sub:
		if ok {
			t.Fatal("expected channel to be closed")
		}
	default:
	}
}

func TestMultipleSubscribers(t *testing.T) {
	ch := New[string, string]()

	sub1 := ch.Subscribe("test")
	sub2 := ch.Subscribe("test")
	defer ch.Unsubscribe("test", sub1)
	defer ch.Unsubscribe("test", sub2)

	ch.Publish("test", "broadcast")

	received := make(chan bool, 2)
	for _, sub := range []<-chan string{sub1, sub2} {
		go func(s <-chan string) {
			select {
			case <-s:
				received <- true
			case <-time.After(time.Second):
			}
		}(sub)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-received:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for subscriber", i)
		}
	}
}

func TestMultipleTopics(t *testing.T) {
	ch := New[string, string]()

	sub1 := ch.Subscribe("topic1")
	sub2 := ch.Subscribe("topic2")
	defer ch.Unsubscribe("topic1", sub1)
	defer ch.Unsubscribe("topic2", sub2)

	ch.Publish("topic1", "msg1")
	ch.Publish("topic2", "msg2")

	select {
	case msg := <-sub1:
		if msg != "msg1" {
			t.Fatalf("expected 'msg1', got '%s'", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for msg1")
	}

	select {
	case msg := <-sub2:
		if msg != "msg2" {
			t.Fatalf("expected 'msg2', got '%s'", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for msg2")
	}
}

// Broadcast Tests
func TestBroadcast(t *testing.T) {
	ch := New[string, string]()

	sub1 := ch.Subscribe("orders")
	sub2 := ch.Subscribe("notifications")
	defer ch.Unsubscribe("orders", sub1)
	defer ch.Unsubscribe("notifications", sub2)

	ch.Broadcast("system-wide message")

	var msgs sync.Map
	var wg sync.WaitGroup
	wg.Add(2)

	receive := func(sub <-chan string, id string) {
		defer wg.Done()
		select {
		case msg := <-sub:
			msgs.Store(id, msg)
		case <-time.After(time.Second):
		}
	}

	go receive(sub1, "orders")
	go receive(sub2, "notifications")
	wg.Wait()

	msgs.Range(func(key, value interface{}) bool {
		if value.(string) != "system-wide message" {
			t.Fatalf("unexpected message on %s: %s", key, value)
		}
		return true
	})
}

func TestBroadcastExcept(t *testing.T) {
	ch := New[string, string]()

	sub1 := ch.Subscribe("topic1")
	sub2 := ch.Subscribe("topic2")
	defer ch.Unsubscribe("topic1", sub1)
	defer ch.Unsubscribe("topic2", sub2)

	ch.BroadcastExcept("topic1", "secret")

	select {
	case <-sub1:
		t.Fatal("topic1 should not have received the broadcast")
	case <-time.After(200 * time.Millisecond):
		// expected
	}

	select {
	case msg := <-sub2:
		if msg != "secret" {
			t.Fatalf("expected 'secret', got '%s'", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for topic2")
	}
}

// HasSubscribers / SubscriberCount Tests

func TestHasSubscribers(t *testing.T) {
	ch := New[string, string]()

	if ch.HasSubscribers("test") {
		t.Fatal("expected no subscribers initially")
	}

	sub := ch.Subscribe("test")
	if !ch.HasSubscribers("test") {
		t.Fatal("expected subscribers after subscribing")
	}

	ch.Unsubscribe("test", sub)
	if ch.HasSubscribers("test") {
		t.Fatal("expected no subscribers after unsubscribing")
	}
}

func TestSubscriberCount(t *testing.T) {
	ch := New[string, string]()

	if n := ch.SubscriberCount("test"); n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}

	sub1 := ch.Subscribe("test")
	sub2 := ch.Subscribe("test")
	defer ch.Unsubscribe("test", sub1)
	defer ch.Unsubscribe("test", sub2)

	if n := ch.SubscriberCount("test"); n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}

	ch.Unsubscribe("test", sub1)
	if n := ch.SubscriberCount("test"); n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}
}

func TestTopicCount(t *testing.T) {
	ch := New[string, string]()

	if n := ch.TopicCount(); n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}

	sub1 := ch.Subscribe("topic1")
	sub2 := ch.Subscribe("topic2")
	defer ch.Unsubscribe("topic1", sub1)
	defer ch.Unsubscribe("topic2", sub2)

	if n := ch.TopicCount(); n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}
}

// Close Test

func TestClose(t *testing.T) {
	ch := New[string, string]()

	sub1 := ch.Subscribe("topic1")
	sub2 := ch.Subscribe("topic2")

	ch.Close()

	// All channels should be closed
	_, ok1 := <-sub1
	if ok1 {
		t.Fatal("expected sub1 to be closed")
	}
	_, ok2 := <-sub2
	if ok2 {
		t.Fatal("expected sub2 to be closed")
	}

	// Publish after close should not panic
	ch.Publish("topic1", "data")
}

// Buffer Test

func TestSubscribeWithBuffer(t *testing.T) {
	ch := New[string, int]()

	sub := ch.SubscribeWithBuffer("test", 100)
	defer ch.Unsubscribe("test", sub)

	for i := 0; i < 50; i++ {
		ch.Publish("test", i)
	}

	received := 0
	for i := 0; i < 50; i++ {
		select {
		case msg := <-sub:
			if msg != i {
				t.Fatalf("expected %d, got %d at index %d", i, msg, i)
			}
			received++
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message %d (received %d)", i, received)
		}
	}
}

// Concurrency Test

func TestConcurrentPublishSubscribe(t *testing.T) {
	ch := New[string, int]()

	const (
		numPublishers = 10
		numMessages   = 100
	)

	total := numPublishers * numMessages
	sub := ch.SubscribeWithBuffer("test", total)
	defer ch.Unsubscribe("test", sub)

	var wg sync.WaitGroup
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numMessages; j++ {
				ch.Publish("test", id*numMessages+j)
			}
		}(i)
	}

	wg.Wait()

	received := 0
	for i := 0; i < total; i++ {
		select {
		case <-sub:
			received++
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout: received %d of %d messages", received, total)
		}
	}

	if received != total {
		t.Fatalf("expected %d messages, got %d", total, received)
	}
}

func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	ch := New[string, string]()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := ch.Subscribe("test")
			ch.Publish("test", "data")
			ch.Unsubscribe("test", sub)
		}()
	}

	wg.Wait()

	// Everything should be cleaned up
	if ch.SubscriberCount("test") != 0 {
		t.Fatal("expected 0 subscribers after all goroutines finished")
	}
}

// Different Key/Value Types Test

func TestIntKey(t *testing.T) {
	ch := New[int, string]()

	sub := ch.Subscribe(42)
	defer ch.Unsubscribe(42, sub)

	ch.Publish(42, "meaning of life")

	select {
	case msg := <-sub:
		if msg != "meaning of life" {
			t.Fatalf("expected 'meaning of life', got '%s'", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestStructValue(t *testing.T) {
	type Event struct {
		ID   int
		Name string
	}

	ch := New[string, Event]()

	sub := ch.Subscribe("events")
	defer ch.Unsubscribe("events", sub)

	ch.Publish("events", Event{ID: 1, Name: "test"})

	select {
	case msg := <-sub:
		if msg.ID != 1 || msg.Name != "test" {
			t.Fatalf("unexpected event: %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// Drop Full Channel Test

func TestDropWhenChannelFull(t *testing.T) {
	ch := New[string, int]()

	// buffer = 2
	sub := ch.SubscribeWithBuffer("test", 2)
	defer ch.Unsubscribe("test", sub)

	// Publish 4 messages into a buffer of 2
	ch.Publish("test", 1)
	ch.Publish("test", 2)
	ch.Publish("test", 3)
	ch.Publish("test", 4)

	time.Sleep(200 * time.Millisecond)

	// At most 2 messages should be in the buffer (buffer size)
	var received []int
loop:
	for {
		select {
		case msg := <-sub:
			received = append(received, msg)
		default:
			break loop
		}
	}

	if len(received) > 2 {
		t.Fatalf("expected at most 2 messages (buffer size), got %d: %v", len(received), received)
	}
	if len(received) == 0 {
		t.Fatal("expected at least 1 message delivered")
	}
}

// Unsubscribe Non-Existent Topic

func TestUnsubscribeNonexistent(t *testing.T) {
	ch := New[string, string]()
	// Should not panic
	ch.Unsubscribe("nonexistent", make(chan string))
}

// Double Unsubscribe

func TestDoubleUnsubscribe(t *testing.T) {
	ch := New[string, string]()

	sub := ch.Subscribe("test")
	ch.Unsubscribe("test", sub)

	// Second unsubscribe should not panic
	ch.Unsubscribe("test", sub)
}

// Performance / Stress Test

func TestHighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high throughput test in short mode")
	}

	ch := New[string, int]()

	const numMessages = 100000

	sub := ch.SubscribeWithBuffer("test", numMessages*10)
	defer ch.Unsubscribe("test", sub)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numMessages; i++ {
			ch.Publish("test", i)
		}
	}()

	var count int32
	var rwg sync.WaitGroup
	rwg.Add(1)
	go func() {
		defer rwg.Done()
		for msg := range sub {
			atomic.AddInt32(&count, 1)
			_ = msg
			if atomic.LoadInt32(&count) >= numMessages {
				return
			}
		}
	}()

	wg.Wait()
	rwg.Wait()

	c := int(atomic.LoadInt32(&count))
	if c != numMessages {
		t.Fatalf("expected %d messages, got %d", numMessages, c)
	}
}

// Gochannel with Pointer Values

func TestPointerValues(t *testing.T) {
	ch := New[string, *int]()

	val := 42
	sub := ch.Subscribe("ptr")
	defer ch.Unsubscribe("ptr", sub)

	ch.Publish("ptr", &val)

	select {
	case msg := <-sub:
		if *msg != 42 {
			t.Fatalf("expected 42, got %d", *msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}
