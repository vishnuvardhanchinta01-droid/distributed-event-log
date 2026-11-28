package cluster

import (
	"strings"

	"github.com/go-zookeeper/zk"
)

// ISR reads the in-sync list from the board (nil = no list yet: a brand-new cluster).
func (m *Member) ISR() ([]string, error) {
	data, _, err := m.conn.Get(m.isrPath())
	if err == zk.ErrNoNode {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return []string{}, nil
	}
	return strings.Split(string(data), ","), nil
}

// SetISR saves the in-sync list (only the leader calls this).
// It's a PERSISTENT note: it must survive the leader dying, because the
// next election needs to know who was in sync.
func (m *Member) SetISR(ids []string) error {
	data := []byte(strings.Join(ids, ","))
	_, err := m.conn.Set(m.isrPath(), data, -1) // -1 = "any version"
	if err == zk.ErrNoNode {
		_, err = m.conn.Create(m.isrPath(), data, 0, zk.WorldACL(zk.PermAll))
	}
	return err
}

// ResetISR deletes the list (for a fresh cluster with empty logs).
func (m *Member) ResetISR() error {
	err := m.conn.Delete(m.isrPath(), -1)
	if err == zk.ErrNoNode {
		return nil
	}
	return err
}

// MayLead: only a broker on the in-sync list may run for leader,
// because only it is guaranteed to have every "saved" record.
func (m *Member) MayLead() (bool, error) {
	ids, err := m.ISR()
	if err != nil {
		return false, err
	}
	if ids == nil {
		return true, nil
	}
	for _, id := range ids {
		if id == m.ID {
			return true, nil
		}
	}
	return false, nil
}

// "/leader-isr" on the real cluster: a persistent note "1,2,3" = who is in sync (written by the leader).
// (tests get their own path, like for /leader)
func (m *Member) isrPath() string { return m.leaderPath + "-isr" }
