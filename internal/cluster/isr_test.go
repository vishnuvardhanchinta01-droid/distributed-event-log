package cluster

import (
	"fmt"
	"os"
	"testing"
)

// Only a broker on the in-sync list may run for leader.
func TestMayLead(t *testing.T) {
	needZK(t)
	tag := fmt.Sprintf("itest-%d", os.Getpid())
	m, err := Join(zkAddr, tag, "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.leaderPath = "/" + tag + "-leader" // own notes, not the real cluster's
	defer m.ResetISR()

	check := func(when string, want bool) {
		t.Helper()
		got, err := m.MayLead()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s: MayLead = %v, want %v", when, got, want)
		}
	}
	check("no ISR yet (new cluster)", true)
	m.SetISR([]string{"other-1", "other-2"})
	check("ISR without me", false)
	m.SetISR([]string{"other-1", tag})
	check("ISR with me", true)
}
