package worker

import (
	"context"
	"testing"

	"connectrpc.com/connect"
)

// FPP-140-C67: a tick whose WorkerBeat Flowrensic does not take does nothing
// more: no scrape of the engine or the worker, no forwarding call and no
// history read from the engine. Once the beat is taken, the tick does all of
// it again.
func TestFPP140C67ATickWhoseBeatFailsDoesNothingMore(t *testing.T) {
	engine := &testEngine{}
	engine.set("r1", History{Events: 2})
	s := startAgainstStandIn(t, engine)
	see(s.telemetry, "r1")
	s.flow.answer("WorkerBeat", connect.NewError(connect.CodeUnavailable, nil))
	ctx := context.Background()
	s.telemetry.collect(ctx, ctx, false)
	if got := s.scrapes.count(); got != 0 {
		t.Errorf("%d scrapes while the beat failed", got)
	}
	if got := engine.readsOf("r1"); got != 0 {
		t.Errorf("%d history reads while the beat failed", got)
	}
	for _, name := range []string{"IngestEphemeralRunMetrics", "IngestWorkerMetrics", "IngestEngineHistory"} {
		if got := s.flow.count(name); got != 0 {
			t.Errorf("%d calls of %s while the beat failed", got, name)
		}
	}

	s.flow.answer("WorkerBeat", nil)
	s.telemetry.collect(ctx, ctx, false)
	if s.scrapes.count() != 2 || engine.readsOf("r1") != 1 || s.flow.count("IngestEphemeralRunMetrics") != 1 ||
		s.flow.count("IngestWorkerMetrics") != 1 || s.flow.count("IngestEngineHistory") != 1 {
		t.Errorf("once the beat is taken: %d scrapes, %d reads, calls %v", s.scrapes.count(), engine.readsOf("r1"), s.flow.calls)
	}
}
