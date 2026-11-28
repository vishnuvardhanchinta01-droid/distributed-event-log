package main

import (
	"testing"
	"time"
)

func TestBackoffGrows(t *testing.T) {
	for _, c := range []struct {
		attempt  int
		min, max time.Duration
	}{
		{1, 25 * time.Millisecond, 50 * time.Millisecond},
		{2, 50 * time.Millisecond, 100 * time.Millisecond},
		{4, 200 * time.Millisecond, 400 * time.Millisecond},
		{10, time.Second, 2 * time.Second},  // capped
		{100, time.Second, 2 * time.Second}, // shifting this far overflows: still capped
	} {
		for i := 0; i < 50; i++ {
			if got := backoff(c.attempt); got < c.min || got > c.max {
				t.Fatalf("backoff(%d) = %v, want between %v and %v", c.attempt, got, c.min, c.max)
			}
		}
	}
}

// Jitter: 20 producers on the same attempt should NOT all pick the same wait.
func TestBackoffJitter(t *testing.T) {
	seen := map[time.Duration]bool{}
	for i := 0; i < 20; i++ {
		seen[backoff(5)] = true
	}
	if len(seen) < 5 {
		t.Fatalf("20 calls gave only %d different waits: no jitter, everyone comes back together", len(seen))
	}
}
