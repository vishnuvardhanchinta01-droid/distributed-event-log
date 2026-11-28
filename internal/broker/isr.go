package broker

import (
	"log"
	"sort"
)

// The ISR (in-sync replicas) = the leader + every follower that has ALL the records.
// Only ISR followers count for acks, and only ISR brokers may become the next leader.

// noteResult is called after EVERY replicate call, with its result.
// It's the ISR rule: fail → off the list; succeed while off the list → back on it.
func (b *Broker) noteResult(f *follower, err error) {
	b.isrMu.Lock()
	defer b.isrMu.Unlock()

	if err != nil && f.inISR {
		f.inISR = false
		log.Printf("broker %s: follower %s dropped from ISR (%v)", b.ID, f.id, err)
		b.publishISR()
	}
	if err == nil && !f.inISR {
		f.inISR = true
		log.Printf("broker %s: follower %s caught up, back in ISR", b.ID, f.id)
		b.publishISR()
	}
}

// publishISR tells main.go (→ ZooKeeper) the new list: me + in-sync followers.
// Call it with b.isrMu held.
func (b *Broker) publishISR() {
	if b.onISRChange != nil {
		b.onISRChange(b.isrLocked())
	}
}

// ISR returns the current in-sync list, leader first.
func (b *Broker) ISR() []string {
	b.isrMu.Lock()
	defer b.isrMu.Unlock()
	return b.isrLocked()
}

func (b *Broker) isrLocked() []string {
	var ids []string
	for _, f := range b.followers {
		if f.inISR {
			ids = append(ids, f.id)
		}
	}
	sort.Strings(ids)
	return append([]string{b.ID}, ids...)
}

// OnISRChange registers who to tell when the ISR changes (main.go saves it in ZooKeeper).
func (b *Broker) OnISRChange(fn func(ids []string)) {
	b.isrMu.Lock()
	defer b.isrMu.Unlock()
	b.onISRChange = fn
}
