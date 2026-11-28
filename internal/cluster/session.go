package cluster

import (
	"log"

	"github.com/go-zookeeper/zk"
)

// The ZooKeeper library tells us about our session on this channel:
// connected, disconnected, EXPIRED (ZooKeeper gave up on us), new session...
//
// Expired is the dangerous one for a leader: ZooKeeper has deleted our /leader note,
// someone else may already be leader, but our own code still THINKS it's leader.
// That broker is a "zombie leader".

// watchSession reads every session event and calls the hooks.
func (m *Member) watchSession(events <-chan zk.Event) {
	expired := false
	for ev := range events {
		if ev.Type != zk.EventSession {
			continue
		}
		m.hooksMu.Lock()
		onExpired, onNew := m.onExpired, m.onNew
		m.hooksMu.Unlock()
		switch ev.State {
		case zk.StateExpired:
			log.Printf("zookeeper: session EXPIRED for broker %s (our notes are gone)", m.ID)
			expired = true
			if onExpired != nil {
				onExpired()
			}
		case zk.StateHasSession:
			if expired { // the library made us a NEW session after the old one died
				expired = false
				log.Printf("zookeeper: broker %s has a new session", m.ID)
				if onNew != nil {
					go onNew()
				}
			}
		}
	}
}

// OnSessionExpired / OnNewSession register what to do on those events.
func (m *Member) OnSessionExpired(fn func()) { m.hooksMu.Lock(); m.onExpired = fn; m.hooksMu.Unlock() }
func (m *Member) OnNewSession(fn func())     { m.hooksMu.Lock(); m.onNew = fn; m.hooksMu.Unlock() }

// Rejoin pins our /brokers/ids note again on the new session.
func (m *Member) Rejoin() error { return m.register() }
