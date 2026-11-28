package cluster

import (
	"log"
	"time"

	"github.com/go-zookeeper/zk"
)

// Leader election = musical chairs with ONE chair: the ephemeral note /leader.

// TryLead tries to grab the chair: create the ephemeral note /leader containing my ID.
// Returns true if I'm the leader now, false if someone else already has it.
func (m *Member) TryLead() (bool, error) {
	_, err := m.conn.Create(m.leaderPath, []byte(m.ID), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Elect keeps trying until this broker becomes leader, then calls becameLeader() once.
// Run it in a goroutine: a follower waits here (maybe forever) for its turn.
func (m *Member) Elect(becameLeader func()) {
	warned := false
	for {
		// only in-sync brokers may race
		allowed, err := m.MayLead()
		if err == zk.ErrClosing || err == zk.ErrConnectionClosed {
			return
		}
		if err != nil || !allowed {
			if err == nil && !warned {
				log.Printf("election: %s is not in the ISR, not allowed to lead (waiting)", m.ID)
				warned = true
			}
			time.Sleep(time.Second) // check again later: maybe a leader adds us back to the ISR
			continue
		}
		won, err := m.TryLead()
		if err == zk.ErrClosing || err == zk.ErrConnectionClosed {
			return // we're shutting down
		}
		if err != nil {
			log.Printf("election: %v (retrying)", err)
			time.Sleep(time.Second)
			continue
		}
		if won {
			becameLeader()
			return
		}

		exists, _, bell, err := m.conn.ExistsW(m.leaderPath)

		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if !exists {
			continue
		}
		ev := <-bell
		if ev.Type == zk.EventNotWatching {
			return
		}
	}
}

// LeaderID reads who has the chair right now ("" = nobody).
func (m *Member) LeaderID() (string, error) {
	data, _, err := m.conn.Get(m.leaderPath)
	if err == zk.ErrNoNode {
		return "", nil
	}
	return string(data), err
}

// LeaderAddr returns the leader's address, e.g. "localhost:9002" ("" = no leader right now).
func (m *Member) LeaderAddr() (string, error) {
	id, err := m.LeaderID()
	if err != nil || id == "" {
		return "", err
	}
	data, _, err := m.conn.Get(brokersPath + "/" + id)
	if err == zk.ErrNoNode {
		return "", nil // the leader just died
	}
	return string(data), err
}

// WatchBrokers calls onChange with the live brokers now, and again every time
// someone joins or leaves /brokers/ids. The leader uses it to keep its follower list fresh.
func (m *Member) WatchBrokers(onChange func(live map[string]string)) {
	go func() {
		for {
			_, _, bell, err := m.conn.ChildrenW(brokersPath)
			if err == zk.ErrClosing || err == zk.ErrConnectionClosed {
				return
			}
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			if live, err := m.Brokers(); err == nil {
				onChange(live)
			}
			if ev := <-bell; ev.Type == zk.EventNotWatching {
				return
			}
		}
	}()
}
