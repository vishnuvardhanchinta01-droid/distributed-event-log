package broker

import "time"

// backpressure = "only N orders in the kitchen at once; the rest get BUSY".
//
// Think of b.slots as a box with room for N tokens.
//   - A produce PUTS a token in the box before it starts (takes an oven).
//   - When it finishes, it TAKES its token back out (frees the oven).
//   - If the box is already full, there is no free oven: answer BUSY right away
//     instead of waiting in a queue that can grow forever.
//
// Go detail: a channel made with make(chan struct{}, N) holds up to N items.
// struct{} is an "empty thing" (0 bytes): we only care about HOW MANY are in the box.

// SetMaxInFlight turns the limit on: at most n produce requests at once. n <= 0 = no limit.
func (b *Broker) SetMaxInFlight(n int) {
	if n <= 0 {
		b.slots = nil
		return
	}
	b.slots = make(chan struct{}, n)
}

// tryAcquire takes an oven if one is free. It must NEVER wait.
// Returns true = you got one (call release() when done), false = all busy.
func (b *Broker) tryAcquire() bool {
	if b.slots == nil {
		return true // no limit set
	}
	select {
	case b.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// release gives the oven back. Only call it after tryAcquire returned true.
func (b *Broker) release() {
	if b.slots == nil {
		return
	}
	<-b.slots
}

// SetSlowDisk makes every write wait d first, like a slow disk. Only for the backpressure demo.
func (b *Broker) SetSlowDisk(d time.Duration) { b.slowDisk = d }
