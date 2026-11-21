package worker

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
)

// testClock is a clock a test moves itself.
type testClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

var start = time.Unix(1_000_000, 0)

// onTheTestClock sets the clock of the telemetry to one the test moves to
// a second, and gives the function that moves it.
func onTheTestClock(c *Telemetry) func(second int) {
	clock := &testClock{at: start}
	c.mu.Lock()
	c.now = clock.now
	c.mu.Unlock()
	return func(second int) {
		clock.mu.Lock()
		clock.at = start.Add(time.Duration(second) * time.Second)
		clock.mu.Unlock()
	}
}

// readsOver runs a tick at each second from..to and gives the seconds at
// which the engine read the run.
func readsOver(s started, engine *testEngine, at func(int), runID string, from, to int) []int {
	ctx := context.Background()
	out := []int{}
	for second := from; second <= to; second++ {
		at(second)
		before := engine.readsOf(runID)
		s.telemetry.collect(ctx, ctx, false)
		if engine.readsOf(runID) > before {
			out = append(out, second)
		}
	}
	return out
}

// FPP-140-C69: a run whose describe finds its history unchanged waits twice
// as long before the next describe, from 2 ticks up to 60 seconds; an attempt
// of the run and a history that grew end the wait; the shutdown reads every
// run, waiting or not.
func TestFPP140C69AnUnchangedRunIsReadLessOften(t *testing.T) {
	engine := &testEngine{}
	engine.set("r1", History{Events: 2})
	s := startAgainstStandIn(t, engine)
	at := onTheTestClock(s.telemetry)
	see(s.telemetry, "r1")
	if got := readsOver(s, engine, at, "r1", 0, 12); !reflect.DeepEqual(got, []int{0, 1, 3, 7}) {
		t.Fatalf("reads at %v, want [0 1 3 7]", got)
	}
	if got := s.flow.sent(); !reflect.DeepEqual(got, []string{"r1"}) {
		t.Fatalf("sent %v", got)
	}

	at(13)
	see(s.telemetry, "r1")
	if got := readsOver(s, engine, at, "r1", 13, 14); !reflect.DeepEqual(got, []int{13}) {
		t.Fatalf("after an attempt, reads at %v, want [13]", got)
	}
	engine.set("r1", History{Events: 3})
	if got := readsOver(s, engine, at, "r1", 15, 17); !reflect.DeepEqual(got, []int{15, 16}) {
		t.Fatalf("after the history grew, reads at %v, want [15 16]", got)
	}

	if got := readsOver(s, engine, at, "r1", 18, 300); !reflect.DeepEqual(got, []int{18, 22, 30, 46, 78, 138, 198, 258}) {
		t.Fatalf("over a long wait, reads at %v, want waits doubling up to 60 seconds", got)
	}

	before := engine.readsOf("r1")
	if err := s.telemetry.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if engine.readsOf("r1") != before+1 {
		t.Fatalf("the shutdown did not read the waiting run")
	}
}

// FPP-140-C70: a history whose size the engine gives as over 3 MiB, or whose
// encoded bytes are over 3 MiB, is not sent, and the run is given up: it is
// not read again, and an attempt of it later does not bring it back.
func TestFPP140C70AHistoryOverThreeMiBIsNotSent(t *testing.T) {
	engine := &testEngine{}
	engine.set("described", History{Events: 2, TooLarge: true})
	engine.set("encoded", History{Events: 2, Bytes: []byte(strings.Repeat("x", 3*1024*1024+1))})
	engine.set("small", History{Events: 2, Bytes: []byte(strings.Repeat("x", 3*1024*1024))})
	s := startAgainstStandIn(t, engine)
	for _, id := range []string{"described", "encoded", "small"} {
		see(s.telemetry, id)
	}
	ctx := context.Background()
	s.telemetry.collect(ctx, ctx, false)
	if engine.maxBytes != 3*1024*1024 {
		t.Fatalf("the engine was asked for at most %d bytes", engine.maxBytes)
	}
	see(s.telemetry, "described")
	see(s.telemetry, "encoded")
	s.telemetry.collect(ctx, ctx, false)
	_ = s.telemetry.Shutdown(ctx)
	if got := s.flow.sent(); !reflect.DeepEqual(got, []string{"small"}) {
		t.Fatalf("sent %v, want only the small one", got)
	}
	for _, id := range []string{"described", "encoded"} {
		if got := engine.readsOf(id); got != 1 {
			t.Errorf("%s was read %d times, want once", id, got)
		}
	}
}

// FPP-140-C71: a run whose history Flowrensic refuses as resource exhausted
// is given up: it is not read again, and an attempt of it later does not
// bring it back.
func TestFPP140C71ARunRefusedAsTooLargeIsGivenUp(t *testing.T) {
	engine := &testEngine{}
	engine.set("r1", History{Events: 2})
	s := startAgainstStandIn(t, engine)
	s.flow.answer("IngestEngineHistory", connect.NewError(connect.CodeResourceExhausted, nil))
	see(s.telemetry, "r1")
	ctx := context.Background()
	s.telemetry.collect(ctx, ctx, false)
	see(s.telemetry, "r1")
	s.telemetry.collect(ctx, ctx, false)
	_ = s.telemetry.Shutdown(ctx)
	if got := engine.readsOf("r1"); got != 1 {
		t.Fatalf("read %d times, want once", got)
	}
	if got := s.flow.count("IngestEngineHistory"); got != 1 {
		t.Fatalf("sent %d times, want once", got)
	}
}

// FPP-140-C72: at most 10,000 runs are given up, the oldest forgotten first.
func TestFPP140C72TenThousandRunsAreGivenUpAtMost(t *testing.T) {
	s := startAgainstStandIn(t, &testEngine{})
	sink := s.telemetry.sink
	for i := 0; i <= 10_000; i++ {
		sink.giveUp(runName(i))
	}
	see(s.telemetry, runName(0))
	see(s.telemetry, runName(1))
	see(s.telemetry, runName(10_000))
	got := []string{}
	for _, one := range sink.runs() {
		got = append(got, one.runID)
	}
	if !reflect.DeepEqual(got, []string{runName(0)}) {
		t.Fatalf("held %v, want only the run given up first", got)
	}
}

func runName(i int) string { return fmt.Sprintf("run-%05d", i) }
