// Package owntime counts the time the SDK's own code takes on the worker, per
// env, in buckets of microseconds, for the whole process.
package owntime

import (
	"sync"
	"time"
)

// EdgesUs are the upper edges, in microseconds, of the buckets a measure is
// counted in. A count is kept per edge, and one more for the measures above
// the last.
var EdgesUs = []int64{
	5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
}

var (
	mu   sync.Mutex
	kept = map[string][]int64{}
)

// Spent counts one measure of d under env.
func Spent(env string, d time.Duration) {
	us := float64(d) / float64(time.Microsecond)
	bucket := 0
	for bucket < len(EdgesUs) && us > float64(EdgesUs[bucket]) {
		bucket++
	}
	mu.Lock()
	defer mu.Unlock()
	counts := kept[env]
	if counts == nil {
		counts = make([]int64, len(EdgesUs)+1)
		kept[env] = counts
	}
	counts[bucket]++
}

// Take takes the counts of env kept since the last take, nil when there are
// none.
func Take(env string) []int64 {
	mu.Lock()
	defer mu.Unlock()
	counts := kept[env]
	delete(kept, env)
	return counts
}

// Now is a reading of a monotonic clock, for Since.
func Now() time.Time { return time.Now() }
