package gochannel

import (
	"testing"
)

// BenchmarkPublish1Subscriber measures publish to a topic with 1 subscriber.
func BenchmarkPublish1Subscriber(b *testing.B) {
	ch := New[string, int]()
	sub := ch.Subscribe("test")
	defer ch.Unsubscribe("test", sub)

	// Drain the subscriber
	go func() {
		for range sub {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Publish("test", i)
	}
}

// BenchmarkPublish10Subscribers measures publish to a topic with 10 subscribers.
func BenchmarkPublish10Subscribers(b *testing.B) {
	ch := New[string, int]()

	subs := make([]<-chan int, 10)
	for i := 0; i < 10; i++ {
		subs[i] = ch.Subscribe("test")
	}

	// Drain all subscribers
	for _, sub := range subs {
		go func(s <-chan int) {
			for range s {
			}
		}(sub)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Publish("test", i)
	}

	// Cleanup
	for _, sub := range subs {
		ch.Unsubscribe("test", sub)
	}
}

// BenchmarkPublish100Subscribers measures publish to a topic with 100 subscribers.
func BenchmarkPublish100Subscribers(b *testing.B) {
	ch := New[string, int]()

	subs := make([]<-chan int, 100)
	for i := 0; i < 100; i++ {
		subs[i] = ch.Subscribe("test")
	}

	for _, sub := range subs {
		go func(s <-chan int) {
			for range s {
			}
		}(sub)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Publish("test", i)
	}

	for _, sub := range subs {
		ch.Unsubscribe("test", sub)
	}
}

// BenchmarkBroadcast10Topics measures broadcast across 10 topics with 1 subscriber each.
func BenchmarkBroadcast10Topics(b *testing.B) {
	ch := New[string, int]()

	subs := make([]<-chan int, 10)
	for i := 0; i < 10; i++ {
		topic := b.Name() + string(rune('0'+i))
		subs[i] = ch.Subscribe(topic)
	}

	for _, sub := range subs {
		go func(s <-chan int) {
			for range s {
			}
		}(sub)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Broadcast(i)
	}

	for _, sub := range subs {
		ch.Unsubscribe("test", sub)
	}
}

// BenchmarkPublishNoSubscriber measures publish to a nonexistent topic (fast path).
func BenchmarkPublishNoSubscriber(b *testing.B) {
	ch := New[string, int]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Publish("nonexistent", i)
	}
}

// BenchmarkSubscribeUnsubscribe measures the overhead of subscribe+unsubscribe.
func BenchmarkSubscribeUnsubscribe(b *testing.B) {
	ch := New[string, int]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub := ch.Subscribe("test")
		ch.Unsubscribe("test", sub)
	}
}

// BenchmarkConcurrentPublish measures concurrent publishing from multiple goroutines.
func BenchmarkConcurrentPublish(b *testing.B) {
	ch := New[string, int]()
	sub := ch.SubscribeWithBuffer("test", b.N*2)
	defer ch.Unsubscribe("test", sub)

	go func() {
		for range sub {
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch.Publish("test", 1)
		}
	})
}

// BenchmarkHasSubscribers measures the overhead of checking subscriber existence.
func BenchmarkHasSubscribers(b *testing.B) {
	ch := New[string, int]()
	sub := ch.Subscribe("test")
	defer ch.Unsubscribe("test", sub)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.HasSubscribers("test")
	}
}
