package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"eventlog/internal/commitlog"
	"eventlog/internal/protocol"
)

// follower is the leader's "phone line" to one follower broker.
type follower struct {
	id    string
	addr  string
	conn  net.Conn // nil = not connected yet (or the last call failed)
	inISR bool     // is it on the in-sync list? (guarded by b.isrMu)
}

// AddFollowers makes this broker a leader that copies every PRODUCE to these addresses.
func (b *Broker) AddFollowers(addrs ...string) {
	for _, a := range addrs {
		b.followers = append(b.followers, &follower{id: a, addr: a, inISR: true}) // static mode: all count
	}
}

// errBehind = the follower's sticky note: "I'm behind, I expect offset next".
type errBehind struct{ next uint64 }

func (e errBehind) Error() string {
	return fmt.Sprintf("follower is behind, expects offset %d", e.next)
}

// empty reports whether ALL partitions are still empty (nothing for a newcomer to miss).
func (b *Broker) empty() bool {
	for _, l := range b.parts {
		if l.Len() > 0 {
			return false
		}
	}
	return true
}

// SetLeader marks this broker as leader (true) or follower (false).
// A broker without ZooKeeper never calls it, so it stays a leader (static mode).
func (b *Broker) SetLeader(yes bool) { b.notLeader.Store(!yes) }

// IsLeader reports whether this broker may accept PRODUCE right now.
func (b *Broker) IsLeader() bool { return !b.notLeader.Load() }

// SetFollowers replaces the follower list with the live brokers from the ZooKeeper board (id -> addr).
// Followers that stay keep their connection and ISR state; a NEW follower starts in the ISR only if
// startInISR says so (a fresh broker must first prove it's caught up).
func (b *Broker) SetFollowers(live map[string]string, startInISR map[string]bool) {
	b.replMu.Lock() // never change the list in the middle of a produce
	defer b.replMu.Unlock()
	b.isrMu.Lock()
	defer b.isrMu.Unlock()
	old := make(map[string]*follower)
	for _, f := range b.followers {
		old[f.id] = f
	}
	ids := make([]string, 0, len(live))
	for id := range live {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var next []*follower
	for _, id := range ids {
		if f, ok := old[id]; ok && f.addr == live[id] {
			next = append(next, f)
			delete(old, id)
		} else {
			// empty leader log = nothing to miss, so a newcomer is trivially in sync
			next = append(next, &follower{id: id, addr: live[id], inISR: startInISR[id] || b.empty()})
		}
	}
	for _, f := range old { // gone from the board: hang up
		if f.conn != nil {
			f.conn.Close()
		}
	}
	b.followers = next
	b.publishISR()
}

// replicate sends one record to the follower and waits for its ACK.
// If the follower says BEHIND, it first resends the missing records from the leader's own log.
// It dials if needed, and hangs up after a failed call so the next one redials.
func (f *follower) replicate(store *commitlog.Log, p int, off uint64, data []byte) error {
	if f.conn == nil {
		conn, err := net.DialTimeout("tcp", f.addr, 2*time.Second)
		if err != nil {
			return err
		}
		f.conn = conn
	}
	err := f.send(p, off, data)
	var behind errBehind
	if errors.As(err, &behind) { // "I'm at page N" → fill pages N..off
		err = f.catchUp(store, p, behind.next, off, data)
	}
	if err != nil {
		f.conn.Close()
		f.conn = nil
	}
	return err
}

// catchUp resends records from..off-1 (read from the leader's own notebook), then the current one.
func (f *follower) catchUp(store *commitlog.Log, p int, from, off uint64, data []byte) error {
	for o := from; o < off; o++ {
		old, err := store.Read(o)
		if err != nil {
			return err
		}
		if err := f.send(p, o, old); err != nil {
			return err
		}
	}
	return f.send(p, off, data)
}

// send = one "phone call" to the assistant: "write data on line off".
func (f *follower) send(p int, off uint64, data []byte) error {
	f.conn.SetDeadline(time.Now().Add(2 * time.Second)) // never wait forever

	// [1-byte partition][8-byte offset][data]
	payload := make([]byte, 1+8+len(data))
	payload[0] = byte(p)
	binary.BigEndian.PutUint64(payload[1:9], off)
	copy(payload[9:], data)
	if err := protocol.WriteFrame(f.conn, protocol.TypeReplicate, payload); err != nil {
		return err
	}
	typ, resp, err := protocol.ReadFrame(f.conn)
	if err != nil {
		return err
	}
	if typ == protocol.TypeError {
		return fmt.Errorf("follower says: %s", resp)
	}
	// BEHIND = "I expect offset N", turn it into an errBehind
	if typ == protocol.TypeReplicateBehind && len(resp) == 8 {
		return errBehind{next: binary.BigEndian.Uint64(resp)}
	}
	if typ != protocol.TypeReplicateAck || len(resp) != 8 || binary.BigEndian.Uint64(resp) != off {
		return fmt.Errorf("bad ack (type %d)", typ)
	}
	return nil
}

// answer = one follower's reply to one REPLICATE, dropped on the tray.
type answer struct {
	f   *follower
	err error
}

// produce = the head clerk's job: write it myself, call ALL assistants at the same time,
// and answer the customer as soon as enough IN-SYNC assistants said "done" (see --acks).
// Only followers in the ISR are counted.
func (b *Broker) produce(p int, data []byte) (uint64, error) {
	store := b.parts[p] // the notebook (partition) this message goes into
	b.replMu.Lock()     // one customer at a time (unlocked later by finishInBackground)

	if b.slowDisk > 0 { // pretend the disk is slow
		time.Sleep(b.slowDisk)
	}
	off, err := store.Append(data)
	if err != nil {
		b.replMu.Unlock()
		return 0, err
	}

	// who COUNTS for acks this time? the followers on the in-sync list right now
	b.isrMu.Lock()
	counts := make(map[*follower]bool)
	for _, f := range b.followers {
		if f.inISR {
			counts[f] = true
		}
	}
	b.isrMu.Unlock()

	// call EVERYONE (an out-of-sync follower still gets the record: that's how it catches up)
	results := make(chan answer, len(b.followers)) // 📥 the tray
	for _, f := range b.followers {
		go func(f *follower) {
			err := f.replicate(store, p, off, data)
			b.noteResult(f, err) // ISR rule (isr.go)
			results <- answer{f, err}
		}(f)
	}

	n := len(counts)
	need := b.needed(n)
	ok, failed, seen := 0, 0, 0
	var lastErr error
	for ok < need {
		a := <-results
		seen++
		if !counts[a.f] {
			continue // not in sync: its answer doesn't count
		}
		if a.err == nil {
			ok++
		} else {
			failed++
			lastErr = a.err
			if failed > n-need { // can never reach `need` now
				b.finishInBackground(results, len(b.followers)-seen)
				return off, fmt.Errorf("only %d of %d needed in-sync followers acked (last error: %w)", ok, need, lastErr)
			}
		}
	}
	// enough "done" notes: answer the customer now; the rest finish in the background
	b.finishInBackground(results, len(b.followers)-seen)
	return off, nil
}
