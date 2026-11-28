package main

import "testing"

func TestPartitionFor(t *testing.T) {
	seen := map[int]bool{}
	for _, key := range []string{"table1", "table2", "table3", "table4", "table5", "table6", "table7", "table8"} {
		p := partitionFor(key, 3)
		if p < 0 || p > 2 {
			t.Fatalf("partitionFor(%q, 3) = %d, want 0, 1 or 2", key, p)
		}
		if again := partitionFor(key, 3); again != p {
			t.Fatalf("partitionFor(%q) gave %d then %d: the same key must always give the same partition", key, p, again)
		}
		seen[p] = true
	}
	if len(seen) < 2 {
		t.Fatalf("8 different keys all went to partition(s) %v: the keys should spread out", seen)
	}
}
