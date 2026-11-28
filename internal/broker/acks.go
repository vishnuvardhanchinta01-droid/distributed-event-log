package broker

import "fmt"

// SetAcks chooses how many followers must confirm before the producer hears "saved".
//
//	"1"      = only the leader itself (fast, can lose data)
//	"quorum" = a majority of all brokers, counting the leader (2 of 3)
//	"all"    = every follower (safest, one dead follower blocks writes)
func (b *Broker) SetAcks(mode string) error {
	switch mode {
	case "1", "quorum", "all":
		b.acks = mode
		return nil
	}
	return fmt.Errorf("unknown acks mode %q (use 1, quorum or all)", mode)
}

// needed = how many FOLLOWER acks we wait for in the current mode,
// when n followers count (n = the followers in the ISR).
func (b *Broker) needed(n int) int {
	switch b.acks {
	case "1":
		return 0
	case "quorum":
		// majority of (n followers + 1 leader) is (n+1)/2+1 brokers;
		// the leader already has it, so we need (n+1)/2 followers.
		return (n + 1) / 2
	default: // "all" (also the default when nothing was set)
		return n
	}
}

// finishInBackground waits for the remaining follower answers, and only THEN lets the
// next customer in. This keeps line 7 ahead of line 8, and keeps each phone line
// used by one customer at a time, even when we already answered the producer.
func (b *Broker) finishInBackground(results chan answer, remaining int) {
	go func() {
		for i := 0; i < remaining; i++ {
			<-results
		}
		b.replMu.Unlock()
	}()
}
