// Package group decides which consumer in a group reads which partition.
package group

import "sort"

// Assign splits the partitions between the members, like dealing cards:
// partition 0 to the 1st member, 1 to the 2nd, ..., then around again.
//
//	Assign([]string{"bob", "ann"}, 3)  →  ann: [0 2], bob: [1]
//
// EVERY consumer runs this on its own, with the same member list from ZooKeeper,
// so they must all get the SAME answer: that's why we sort first.
func Assign(members []string, partitions int) map[string][]int {
	out := make(map[string][]int)
	if len(members) == 0 {
		return out
	}
	sorted := append([]string(nil), members...) // a copy: don't reorder the caller's slice
	sort.Strings(sorted)

	for p := 0; p < partitions; p++ {
		who := sorted[p%len(sorted)]
		out[who] = append(out[who], p)
	}
	return out
}
