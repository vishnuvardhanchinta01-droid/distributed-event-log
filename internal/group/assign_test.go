package group

import (
	"fmt"
	"testing"
)

func TestAssign(t *testing.T) {
	cases := []struct {
		members []string
		want    string
	}{
		{[]string{"ann"}, "map[ann:[0 1 2]]"},                                  // one cook: all rails
		{[]string{"bob", "ann"}, "map[ann:[0 2] bob:[1]]"},                     // order given doesn't matter
		{[]string{"ann", "bob", "cat"}, "map[ann:[0] bob:[1] cat:[2]]"},        // one each
		{[]string{"ann", "bob", "cat", "dan"}, "map[ann:[0] bob:[1] cat:[2]]"}, // dan stands idle
		{nil, "map[]"},
	}
	for _, c := range cases {
		if got := fmt.Sprint(Assign(c.members, 3)); got != c.want {
			t.Errorf("Assign(%v, 3) = %s, want %s", c.members, got, c.want)
		}
	}
}
