package commitlog

import "testing"

// Simulates a broker restart: write, close, open again, read.
func TestReopen(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"apple", "banana", "cherry"} {
		if _, err := l.Append([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	l.Close() // "broker stops"

	l2, err := Open(dir) // "broker starts again"
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	got, err := l2.Read(1)
	if err != nil || string(got) != "banana" {
		t.Fatalf("after reopen, Read(1) = %q, %v; want banana", got, err)
	}
	off, err := l2.Append([]byte("date"))
	if err != nil || off != 3 {
		t.Fatalf("after reopen, Append got offset %d, %v; want 3", off, err)
	}
}
