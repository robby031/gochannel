package gochannel

// Stress tests for lock contention, latency stability, and subscriber churn.
//
// Run with:
//
//	go test -run TestStress -v -timeout 10m ./...
//
// Each test skips under -short. Add -count=N to repeat runs.

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// msg carries a publish timestamp so receivers can measure end-to-end latency.
type msg struct {
	seq    int64
	sentAt time.Time
}

// latencyTracker collects nanosecond latency samples and computes percentiles.
type latencyTracker struct {
	mu      sync.Mutex
	samples []int64
}

func (t *latencyTracker) record(d time.Duration) {
	t.mu.Lock()
	t.samples = append(t.samples, d.Nanoseconds())
	t.mu.Unlock()
}

func (t *latencyTracker) report(name string) {
	t.mu.Lock()
	s := make([]int64, len(t.samples))
	copy(s, t.samples)
	t.mu.Unlock()

	if len(s) == 0 {
		fmt.Printf("  [%s] no samples\n", name)
		return
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })

	pct := func(p float64) time.Duration {
		idx := int(float64(len(s)-1) * p / 100)
		return time.Duration(s[idx])
	}

	fmt.Printf("  [%s] n=%d  p50=%v  p90=%v  p99=%v  p999=%v  max=%v\n",
		name,
		len(s),
		pct(50), pct(90), pct(99), pct(99.9),
		time.Duration(s[len(s)-1]),
	)
}

// Test 1 - Subscriber Churn Under Constant Publish Load
//
// Scenario: a single publisher fires at 10 kHz while thousands of goroutines
// concurrently subscribe, hold for a random duration, and unsubscribe.
// Objective: surface lock contention between Subscribe/Unsubscribe paths and
// the publish notify path; confirm no panics and zero goroutine leaks.

func TestStressSubscriberChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		testDuration    = 10 * time.Second
		publishInterval = 100 * time.Microsecond // 10 kHz
		maxConcurrent   = 2000                   // goroutines in flight at once
		topic           = "churn"
	)

	ch := New[string, msg]()
	defer ch.Close()

	var (
		published atomic.Int64
		received  atomic.Int64
		dropped   atomic.Int64
		subErrors atomic.Int64
	)

	// Publisher goroutine.
	stopPub := make(chan struct{})
	go func() {
		var seq int64
		ticker := time.NewTicker(publishInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopPub:
				return
			case <-ticker.C:
				seq++
				ch.Publish(topic, msg{seq: seq, sentAt: time.Now()})
				published.Add(1)
			}
		}
	}()

	deadline := time.Now().Add(testDuration)
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	for time.Now().Before(deadline) {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			bufSize := rand.IntN(20) + 1
			sub := ch.SubscribeWithBuffer(topic, bufSize)

			holdFor := time.Duration(rand.IntN(5)+1) * time.Millisecond
			timer := time.NewTimer(holdFor)
			defer timer.Stop()

			msgs := 0
		drain:
			for {
				select {
				case _, ok := <-sub:
					if !ok {
						break drain
					}
					received.Add(1)
					msgs++
				case <-timer.C:
					break drain
				}
			}
			_ = msgs

			ch.Unsubscribe(topic, sub)

			// Drain any messages buffered after unsubscribe closed the channel.
			for range sub {
				dropped.Add(1)
			}

			if ch.SubscriberCount(topic) < 0 {
				subErrors.Add(1)
			}
		}()

		// Throttle goroutine creation so we don't immediately saturate.
		time.Sleep(5 * time.Microsecond)
	}

	close(stopPub)
	wg.Wait()

	if subErrors.Load() > 0 {
		t.Errorf("SubscriberCount returned negative value %d times", subErrors.Load())
	}
	if ch.SubscriberCount(topic) != 0 {
		t.Errorf("expected 0 subscribers after all goroutines finished, got %d", ch.SubscriberCount(topic))
	}

	fmt.Printf("\n[TestStressSubscriberChurn]\n")
	fmt.Printf("  published=%d  received=%d  dropped=%d\n",
		published.Load(), received.Load(), dropped.Load())
	fmt.Printf("  final subscriber count: %d (expected 0)\n", ch.SubscriberCount(topic))
}

// Test 2 - Latency Stability Under Churn
//
// Scenario: a fixed cohort of 100 "stable" subscribers measures end-to-end
// latency while a background pool of goroutines churns subscribers on the same
// topic. Objective: determine whether churn degrades p99/p999 latency for
// stable subscribers (a sign of lock starvation or head-of-line blocking).

func TestStressLatencyUnderChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		testDuration    = 15 * time.Second
		warmup          = 1 * time.Second
		stableSubCount  = 100
		churnConcurrent = 500
		publishInterval = 1 * time.Millisecond // 1 kHz - slow enough to measure latency
		topic           = "latency"
	)

	ch := New[string, msg]()
	defer ch.Close()

	// Stable subscribers that measure latency.
	stable := make([]<-chan msg, stableSubCount)
	for i := range stable {
		stable[i] = ch.SubscribeWithBuffer(topic, 64)
	}
	defer func() {
		for _, s := range stable {
			ch.Unsubscribe(topic, s)
		}
	}()

	tracker := &latencyTracker{}
	var stableWg sync.WaitGroup

	for _, sub := range stable {
		stableWg.Add(1)
		go func(s <-chan msg) {
			defer stableWg.Done()
			for m := range s {
				tracker.record(time.Since(m.sentAt))
			}
		}(sub)
	}

	// Background churn goroutines.
	stopChurn := make(chan struct{})
	var churnWg sync.WaitGroup
	sem := make(chan struct{}, churnConcurrent)

	go func() {
		for {
			select {
			case <-stopChurn:
				return
			default:
			}
			sem <- struct{}{}
			churnWg.Add(1)
			go func() {
				defer churnWg.Done()
				defer func() { <-sem }()
				sub := ch.SubscribeWithBuffer(topic, 4)
				time.Sleep(time.Duration(rand.IntN(10)+1) * time.Millisecond)
				ch.Unsubscribe(topic, sub)
				for range sub {
				}
			}()
		}
	}()

	// Publisher.
	stopPub := make(chan struct{})
	var seq atomic.Int64
	go func() {
		ticker := time.NewTicker(publishInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopPub:
				return
			case <-ticker.C:
				ch.Publish(topic, msg{seq: seq.Add(1), sentAt: time.Now()})
			}
		}
	}()

	time.Sleep(warmup)
	tracker.mu.Lock()
	tracker.samples = tracker.samples[:0] // discard warmup samples
	tracker.mu.Unlock()

	time.Sleep(testDuration - warmup)

	close(stopPub)
	close(stopChurn)
	churnWg.Wait()

	// Close stable subs to unblock their receiver goroutines.
	for _, s := range stable {
		ch.Unsubscribe(topic, s)
	}
	stableWg.Wait()

	fmt.Printf("\n[TestStressLatencyUnderChurn]\n")
	fmt.Printf("  stable subscribers: %d  churn concurrency: %d\n", stableSubCount, churnConcurrent)
	fmt.Printf("  total publishes: %d\n", seq.Load())
	tracker.report("stable-sub latency")
}

// Test 3 - Peak Subscriber Count Latency
//
// Scenario: ramp subscribers from 0 to 10 000 in steps, measuring publish
// latency at each step. Objective: find the subscriber count at which latency
// starts degrading non-linearly (the bottleneck knee).

func TestStressPeakSubscriberLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		topic          = "peak"
		publishPerStep = 500
		bufSize        = 32
	)

	steps := []int{10, 100, 500, 1000, 2000, 5000, 10000}

	ch := New[string, msg]()
	defer ch.Close()

	fmt.Printf("\n[TestStressPeakSubscriberLatency]\n")
	fmt.Printf("  %-10s  %8s  %8s  %8s  %8s  %10s\n",
		"subs", "p50", "p90", "p99", "p999", "max")

	var allSubs []<-chan msg

	for _, targetCount := range steps {
		// Add subscribers until we reach targetCount.
		for len(allSubs) < targetCount {
			allSubs = append(allSubs, ch.SubscribeWithBuffer(topic, bufSize))
		}

		// Drain goroutines - one per subscriber, capped to avoid overhead.
		// We only measure latency via the first subscriber.
		for _, sub := range allSubs[1:] {
			go func(s <-chan msg) {
				for range s {
				}
			}(sub)
		}

		tracker := &latencyTracker{}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			recv := 0
			for m := range allSubs[0] {
				tracker.record(time.Since(m.sentAt))
				recv++
				if recv >= publishPerStep {
					return
				}
			}
		}()

		for i := 0; i < publishPerStep; i++ {
			ch.Publish(topic, msg{seq: int64(i), sentAt: time.Now()})
			time.Sleep(100 * time.Microsecond)
		}
		wg.Wait()

		s := tracker.samples
		if len(s) == 0 {
			fmt.Printf("  %-10d  (no samples)\n", targetCount)
			continue
		}
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		pct := func(p float64) time.Duration {
			return time.Duration(s[int(float64(len(s)-1)*p/100)])
		}
		fmt.Printf("  %-10d  %8v  %8v  %8v  %8v  %10v\n",
			targetCount,
			pct(50), pct(90), pct(99), pct(99.9),
			time.Duration(s[len(s)-1]),
		)
	}

	// Clean up all subscribers.
	for _, sub := range allSubs {
		ch.Unsubscribe(topic, sub)
	}
}

// Test 4 — Multi-Topic Fan-Out Under Concurrent Publishers
//
// Scenario: 100 topics x 50 subscribers each, with 20 concurrent publishers
// each picking a random topic, publishing for 10 s. Objective: detect
// contention on the top-level Channel.mu (RWMutex) when many goroutines
// compete to publish to different topics simultaneously.

func TestStressMultiTopicFanOut(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		numTopics     = 100
		subsPerTopic  = 50
		numPublishers = 20
		testDuration  = 10 * time.Second
		subBufSize    = 16
	)

	ch := New[int, msg]()
	defer ch.Close()

	// Set up all subscribers.
	subs := make([][]<-chan msg, numTopics)
	for t := range subs {
		subs[t] = make([]<-chan msg, subsPerTopic)
		for s := range subs[t] {
			subs[t][s] = ch.SubscribeWithBuffer(t, subBufSize)
		}
	}

	// Drain goroutines.
	var drainWg sync.WaitGroup
	for _, topicSubs := range subs {
		for _, sub := range topicSubs {
			drainWg.Add(1)
			go func(s <-chan msg) {
				defer drainWg.Done()
				for range s {
				}
			}(sub)
		}
	}

	var (
		totalPublished atomic.Int64
	)

	tracker := &latencyTracker{}
	stop := make(chan struct{})
	var pubWg sync.WaitGroup

	for i := 0; i < numPublishers; i++ {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					topic := rand.IntN(numTopics)
					ch.Publish(topic, msg{sentAt: time.Now()})
					totalPublished.Add(1)
				}
			}
		}()
	}

	// Measure latency from a single stable subscriber on topic 0.
	trackerSub := ch.SubscribeWithBuffer(0, 256)
	var measureWg sync.WaitGroup
	measureWg.Add(1)
	go func() {
		defer measureWg.Done()
		for m := range trackerSub {
			tracker.record(time.Since(m.sentAt))
		}
	}()

	time.Sleep(testDuration)
	close(stop)
	pubWg.Wait()

	ch.Unsubscribe(0, trackerSub)
	measureWg.Wait()

	// Unsubscribe all.
	for t, topicSubs := range subs {
		for _, sub := range topicSubs {
			ch.Unsubscribe(t, sub)
		}
	}
	drainWg.Wait()

	fmt.Printf("\n[TestStressMultiTopicFanOut]\n")
	fmt.Printf("  topics=%d  subs/topic=%d  publishers=%d\n",
		numTopics, subsPerTopic, numPublishers)
	fmt.Printf("  total published: %d  (~%.0f msg/s)\n",
		totalPublished.Load(),
		float64(totalPublished.Load())/testDuration.Seconds(),
	)
	tracker.report("latency on topic-0 (1 sub, uncontended read)")
}

// Test 5 — Thundering Herd
//
// Scenario: 5 000 goroutines all subscribe to one topic simultaneously, wait
// for one broadcast, then immediately unsubscribe. Objective: measure how the
// library handles a burst of concurrent Subscribe + Broadcast + Unsubscribe
// on a single hot topic without deadlock, livelock, or panic.

func TestStressThunderingHerd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		herdSize = 5000
		topic    = "herd"
	)

	ch := New[string, msg]()
	defer ch.Close()

	ready := make(chan struct{})
	var received atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < herdSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := ch.SubscribeWithBuffer(topic, 1)

			// Signal ready after subscribing.
			ready <- struct{}{}

			select {
			case _, ok := <-sub:
				if ok {
					received.Add(1)
				}
			case <-time.After(5 * time.Second):
			}

			ch.Unsubscribe(topic, sub)
			for range sub {
			}
		}()
	}

	// Wait for all goroutines to subscribe.
	for i := 0; i < herdSize; i++ {
		<-ready
	}

	start := time.Now()
	ch.Broadcast(msg{sentAt: time.Now()})
	broadcastDuration := time.Since(start)

	wg.Wait()

	if ch.SubscriberCount(topic) != 0 {
		t.Errorf("expected 0 subscribers after herd, got %d", ch.SubscriberCount(topic))
	}

	fmt.Printf("\n[TestStressThunderingHerd]\n")
	fmt.Printf("  herd size: %d\n", herdSize)
	fmt.Printf("  broadcast duration: %v\n", broadcastDuration)
	fmt.Printf("  received: %d / %d (%.1f%%)\n",
		received.Load(), herdSize,
		float64(received.Load())/float64(herdSize)*100,
	)
}
