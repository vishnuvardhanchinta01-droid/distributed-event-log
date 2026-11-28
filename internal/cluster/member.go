// Package cluster talks to ZooKeeper, the shared "notice board" all brokers trust.
package cluster

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

const brokersPath = "/brokers/ids" // one note per live broker: /brokers/ids/<id> = "localhost:9001"

// Member = one broker's connection (session) to ZooKeeper.
// While the session is alive, ZooKeeper keeps this broker's ephemeral notes.
type Member struct {
	conn       *zk.Conn
	ID         string
	Addr       string
	leaderPath string // "/leader" (tests use their own path so they don't fight the real cluster)

	hooksMu   sync.Mutex
	onExpired func() // our session died: all our ephemeral notes are GONE
	onNew     func() // we got a brand-new session after that
}

// Connect opens a session WITHOUT registering (for tools like cmd/board).
func Connect(zkAddr string) (*Member, error) {
	conn, events, err := zk.Connect([]string{zkAddr}, 4*time.Second, zk.WithLogInfo(false))
	if err != nil {
		return nil, err
	}
	m := &Member{conn: conn, leaderPath: "/leader"}
	go m.watchSession(events) // notice when our session is lost (session.go)
	return m, nil
}

// Join connects to ZooKeeper and pins this broker's note on the board.
func Join(zkAddr, id, addr string) (*Member, error) {
	// 4s session timeout: if our heartbeat stops for 4s, ZooKeeper deletes our ephemeral notes.
	m, err := Connect(zkAddr)
	if err != nil {
		return nil, err
	}
	conn := m.conn
	m.ID, m.Addr = id, addr
	for _, p := range []string{"/brokers", brokersPath} {
		if err := m.ensure(p); err != nil {
			conn.Close()
			return nil, fmt.Errorf("zookeeper %s: %w (is it running? try: zkServer start)", zkAddr, err)
		}
	}
	// After a crash (kill -9) our OLD session may still hold /brokers/ids/<id> for a few seconds,
	// until ZooKeeper expires it. So keep trying for up to ~10s.
	for tries := 1; ; tries++ {
		err := m.register()
		if err == nil {
			break
		}
		if tries == 10 {
			conn.Close()
			return nil, err
		}
		log.Printf("%v: waiting for ZooKeeper to expire the old session (%d/10)", err, tries)
		time.Sleep(time.Second)
	}
	return m, nil
}

// ensure creates a normal (persistent) note if it doesn't exist yet.
func (m *Member) ensure(path string) error {
	_, err := m.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		return nil // someone made it already: fine
	}
	return err
}

// register pins MY ephemeral note: /brokers/ids/<my id> = my address.
func (m *Member) register() error {
	path := brokersPath + "/" + m.ID
	_, err := m.conn.Create(path, []byte(m.Addr), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		return fmt.Errorf("broker %s is already registered (is an old one still running?)", m.ID)
	}
	return err
}

// Brokers returns the live brokers as id -> address, read from the board.
func (m *Member) Brokers() (map[string]string, error) {
	ids, _, err := m.conn.Children(brokersPath)
	if err != nil {
		return nil, err
	}
	sort.Strings(ids)
	out := make(map[string]string, len(ids))
	for _, id := range ids {
		data, _, err := m.conn.Get(brokersPath + "/" + id)
		if err == zk.ErrNoNode {
			continue // it died between Children and Get
		}
		if err != nil {
			return nil, err
		}
		out[id] = string(data)
	}
	return out, nil
}

// Close ends the session cleanly: ZooKeeper deletes our ephemeral notes RIGHT AWAY
// (a crash instead makes ZooKeeper wait for the 4s session timeout).
func (m *Member) Close() { m.conn.Close() }
