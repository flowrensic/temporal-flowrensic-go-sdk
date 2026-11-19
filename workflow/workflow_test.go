package workflow

import (
	"testing"
	"time"
)

// FPP-140-C59: the queries are counted per namespace, workflow type, query
// name and minute, answered and failed apart.
func TestFPP140C59QueryCounts(t *testing.T) {
	at := time.Date(2026, 11, 18, 10, 0, 30, 0, time.UTC)
	got := countsOf([]answered{
		{"default", "Waits", "status", true, at},
		{"default", "Waits", "status", true, at.Add(10 * time.Second)},
		{"default", "Waits", "status", false, at.Add(20 * time.Second)},
		{"default", "Waits", "status", true, at.Add(time.Minute)},
		{"default", "Waits", "boom", false, at},
	})
	if len(got) != 3 {
		t.Fatalf("%d counts: %v", len(got), got)
	}
	first := got[0]
	if first.GetQueryName() != "status" || first.GetAnswered() != 2 || first.GetFailed() != 1 || !first.GetMinute().AsTime().Equal(at.Truncate(time.Minute)) {
		t.Errorf("first count %v", first)
	}
	if got[1].GetAnswered() != 1 || got[2].GetQueryName() != "boom" || got[2].GetFailed() != 1 {
		t.Errorf("other counts %v %v", got[1], got[2])
	}
}
