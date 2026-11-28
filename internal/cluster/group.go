package cluster

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/go-zookeeper/zk"
)

// A consumer group lives on the board as:
//   /groups/<group>/members/<consumer id>   ephemeral: "I'm in the team" (vanishes when I die)

func membersPath(group string) string { return "/groups/" + group + "/members" }

// JoinGroup pins my ephemeral note in the group's member list.
func (m *Member) JoinGroup(group, consumerID string) error {
	for _, p := range []string{"/groups", "/groups/" + group, membersPath(group)} {
		if err := m.ensure(p); err != nil {
			return err
		}
	}
	path := membersPath(group) + "/" + consumerID
	for tries := 1; ; tries++ { // after a crash our old note may linger for a few seconds
		_, err := m.conn.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err == nil || tries == 10 {
			return err
		}
		time.Sleep(time.Second)
	}
}

// GroupMembers lists who is in the group right now, sorted.
func (m *Member) GroupMembers(group string) ([]string, error) {
	ids, _, err := m.conn.Children(membersPath(group))
	if err == zk.ErrNoNode {
		return nil, nil
	}
	sort.Strings(ids)
	return ids, err
}

// Groups lists all groups on the board.
func (m *Member) Groups() ([]string, error) {
	gs, _, err := m.conn.Children("/groups")
	if err == zk.ErrNoNode {
		return nil, nil
	}
	sort.Strings(gs)
	return gs, err
}

// WatchGroup calls onChange with the member list now, and again every time someone
// joins or leaves the group. That's the trigger for a REBALANCE.
func (m *Member) WatchGroup(group string, onChange func(members []string)) {
	go func() {
		for {
			ids, _, bell, err := m.conn.ChildrenW(membersPath(group))
			if err == zk.ErrClosing || err == zk.ErrConnectionClosed {
				return
			}
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			sort.Strings(ids)
			onChange(ids)
			if ev := <-bell; ev.Type == zk.EventNotWatching {
				return
			}
		}
	}()
}

// Bookmarks (committed offsets) live on the board too:
//   /groups/<group>/offsets/<partition> = "17"   persistent: "next page to read is 17"
// Persistent, because the bookmark must survive the consumer that wrote it.

func offsetPath(group string, p int) string { return fmt.Sprintf("/groups/%s/offsets/%d", group, p) }

// CommitOffset saves the bookmark: the NEXT offset this group should read in partition p.
func (m *Member) CommitOffset(group string, p int, next uint64) error {
	if err := m.ensure("/groups/" + group + "/offsets"); err != nil {
		return err
	}
	data := []byte(strconv.FormatUint(next, 10))
	_, err := m.conn.Set(offsetPath(group, p), data, -1)
	if err == zk.ErrNoNode {
		_, err = m.conn.Create(offsetPath(group, p), data, 0, zk.WorldACL(zk.PermAll))
	}
	return err
}

// CommittedOffset reads the bookmark (0 if the group never read partition p).
func (m *Member) CommittedOffset(group string, p int) (uint64, error) {
	data, _, err := m.conn.Get(offsetPath(group, p))
	if err == zk.ErrNoNode {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(data), 10, 64)
}
