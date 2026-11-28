package main

import (
	"errors"
	"math/rand/v2"
	"time"
)

// errBusy = the broker answered BUSY: it is alive but full, nothing was saved, try again later.
var errBusy = errors.New("broker busy")

const (
	firstWait = 50 * time.Millisecond // wait after the 1st BUSY
	maxWait   = 2 * time.Second       // never wait longer than this
)

// backoff says how long to wait after the attempt-th BUSY (attempt = 1, 2, 3, ...).
//
// Exponential: double the wait every time:  50ms, 100ms, 200ms, 400ms, ... up to 2s.
//
//	Like knocking on a busy shop's door: if they're still busy, wait longer before knocking again.
//
// Jitter: add a bit of randomness so 100 producers that got BUSY at the same moment
//
//	don't all come back at the same moment (the herd effect).
//	We wait somewhere between half and all of the wait:  [wait/2, wait].
func backoff(attempt int) time.Duration {
	wait := firstWait << (attempt - 1)
	if wait > maxWait || wait <= 0 {
		wait = maxWait
	}

	return wait/2 + rand.N(wait/2+1)
}
