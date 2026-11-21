package worker

import (
	"fmt"
	"testing"
)

// FPP-140-C73: past 10,000 runs the run dropped is the one heard of least
// lately: an attempt of a run moves it to the back, so a run still at work is
// kept.
func TestFPP140C73PastTenThousandRunsTheLeastLatelyHeardIsDropped(t *testing.T) {
	s := startAgainstStandIn(t, &testEngine{})
	see(s.telemetry, "at-work")
	for i := 0; i < 9_999; i++ {
		see(s.telemetry, fmt.Sprintf("other-%04d", i))
	}
	see(s.telemetry, "at-work")
	see(s.telemetry, "one-more")
	held := map[string]bool{}
	runs := s.telemetry.sink.runs()
	for _, one := range runs {
		held[one.runID] = true
	}
	if len(runs) != 10_000 || !held["at-work"] || held["other-0000"] || !held["other-0001"] || !held["one-more"] {
		t.Fatalf("held %d runs: at-work %v, other-0000 %v, other-0001 %v, one-more %v",
			len(runs), held["at-work"], held["other-0000"], held["other-0001"], held["one-more"])
	}
	if last := runs[len(runs)-1].runID; last != "one-more" || runs[len(runs)-2].runID != "at-work" {
		t.Fatalf("the last runs are %s and %s, want at-work then one-more", runs[len(runs)-2].runID, last)
	}
}
