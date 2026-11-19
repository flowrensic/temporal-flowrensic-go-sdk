package buffer

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/owntime"
)

// fake is a Flowrensic that takes sends and reports, and answers each with
// the error answer gives, nil when it is nil.
type fake struct {
	mu      sync.Mutex
	sends   [][]int
	ids     []SendID
	reports [][]*flowrensicv1.WorkerBufferReport
	answer  func(records []int) error
	hold    chan struct{}
}

func (f *fake) target(env string) Target[int] {
	return Target[int]{
		Send: func(ctx context.Context, records []int, id SendID) error {
			if f.hold != nil {
				select {
				case <-f.hold:
				case <-ctx.Done():
					return connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			f.sends = append(f.sends, append([]int(nil), records...))
			f.ids = append(f.ids, id)
			if f.answer != nil {
				return f.answer(records)
			}
			return nil
		},
		EnvOf:  func(int) string { return env },
		SizeOf: func(int) int { return 10 },
		Report: func(_ context.Context, reports []*flowrensicv1.WorkerBufferReport, _ SendID) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.reports = append(f.reports, reports)
			return nil
		},
		Env: env,
	}
}

func (f *fake) sizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []int{}
	for _, one := range f.sends {
		out = append(out, len(one))
	}
	return out
}

var quiet = Options{BatchSize: 1_000_000, FlushInterval: time.Hour}

// FPP-140-C15: at the count limit nothing is dropped, one past it drops the
// oldest and counts it; a backlog leaves in batches of at most the batch
// maximum, one send at a time; at the byte limit the oldest goes.
func TestFPP140C15BufferLimits(t *testing.T) {
	ctx := context.Background()
	f := &fake{}
	b := New(f.target("c15"), Options{BatchSize: 1_000_000, FlushInterval: time.Hour, MaxWaiting: 1000, MaxBatchSize: 400})
	for i := 0; i < 1000; i++ {
		b.Record(i)
	}
	if got := b.Counts(); got.Dropped != 0 || got.Waiting != 1000 || got.WaitingBytes != 10_000 {
		t.Fatalf("at the limit: %+v", got)
	}
	b.Record(1000)
	if got := b.Counts(); got.Dropped != 1 || got.Waiting != 1000 {
		t.Fatalf("one past the limit: %+v", got)
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := f.sizes(); !reflect.DeepEqual(got, []int{400, 400, 200}) {
		t.Fatalf("batches %v", got)
	}
	if f.sends[0][0] != 1 || f.sends[2][199] != 1000 {
		t.Fatalf("the oldest was not the one dropped: first %d, last %d", f.sends[0][0], f.sends[2][199])
	}
	for i, id := range f.ids {
		if id.SendNumber != int64(i+1) {
			t.Fatalf("send %d has number %d", i, id.SendNumber)
		}
	}
	_ = b.Close(ctx)

	bytes := New(f.target("c15-bytes"), Options{BatchSize: 1_000_000, FlushInterval: time.Hour, MaxWaitingBytes: 100})
	for i := 0; i < 11; i++ {
		bytes.Record(i)
	}
	if got := bytes.Counts(); got.Dropped != 1 || got.WaitingBytes != 100 {
		t.Fatalf("past the byte limit: %+v", got)
	}
	_ = bytes.Close(ctx)
}

// FPP-140-C16: a send answered unavailable is kept whole and sent again first
// with the same sender and number, and the limit drops only newer records; a
// send refused with permission denied is dropped and counted; a send given up
// at its timeout is counted and sent again.
func TestFPP140C16BufferSendsAgain(t *testing.T) {
	ctx := context.Background()
	f := &fake{}
	unavailable := true
	f.answer = func([]int) error {
		if unavailable {
			return connect.NewError(connect.CodeUnavailable, nil)
		}
		return nil
	}
	b := New(f.target("c16"), Options{BatchSize: 1_000_000, FlushInterval: time.Hour, MaxWaiting: 5})
	for i := 0; i < 5; i++ {
		b.Record(i)
	}
	_ = b.Flush(ctx)
	for i := 5; i < 15; i++ {
		b.Record(i)
	}
	if got := b.Counts(); got.Dropped != 5 || got.Waiting != 10 {
		t.Fatalf("while kept: %+v", got)
	}
	f.mu.Lock()
	unavailable = false
	f.mu.Unlock()
	_ = b.Flush(ctx)
	if !reflect.DeepEqual(f.sends[0], f.sends[1]) || f.ids[0] != f.ids[1] {
		t.Fatalf("the kept send was not sent again whole: %v %v", f.sends[:2], f.ids[:2])
	}
	if !reflect.DeepEqual(f.sends[2], []int{10, 11, 12, 13, 14}) {
		t.Fatalf("the newer records sent: %v", f.sends[2])
	}
	_ = b.Close(ctx)

	refused := &fake{answer: func([]int) error { return connect.NewError(connect.CodePermissionDenied, nil) }}
	r := New(refused.target("c16-refused"), quiet)
	for i := 0; i < 10; i++ {
		r.Record(i)
	}
	_ = r.Flush(ctx)
	if got := r.Counts(); got.Dropped != 10 || got.Waiting != 0 || len(refused.sends) != 1 {
		t.Fatalf("refused: %+v after %d sends", got, len(refused.sends))
	}
	_ = r.Close(ctx)

	slow := &fake{hold: make(chan struct{})}
	s := New(slow.target("c16-slow"), Options{BatchSize: 1_000_000, FlushInterval: time.Hour, Timeout: 50 * time.Millisecond})
	s.Record(1)
	_ = s.Flush(ctx)
	if got := s.Counts(); got.TimedOut != 1 || got.Waiting != 1 {
		t.Fatalf("timed out: %+v", got)
	}
	close(slow.hold)
	_ = s.Flush(ctx)
	if got := s.Counts(); got.TimedOut != 1 || got.Waiting != 0 || got.Dropped != 0 {
		t.Fatalf("sent again: %+v", got)
	}
	_ = s.Close(ctx)
}

// FPP-140-C17: a report goes when a count changed, with what was counted
// since the last answered report and the own time of the env of the buffer;
// none goes when nothing changed; close drops what still waits and reports it.
func TestFPP140C17BufferReports(t *testing.T) {
	ctx := context.Background()
	f := &fake{answer: func([]int) error { return connect.NewError(connect.CodePermissionDenied, nil) }}
	b := New(f.target("c17"), quiet)
	b.Record(1)
	b.Spent(time.Microsecond)
	_ = b.Flush(ctx)
	if len(f.reports) != 1 {
		t.Fatalf("reports: %d", len(f.reports))
	}
	got := f.reports[0][0]
	if got.GetEnv() != "c17" || got.GetRecordsDropped() != 1 || got.GetRecordsWaiting() != 0 || len(got.GetOwnTimeCounts()) != len(owntime.EdgesUs)+1 {
		t.Fatalf("report: %v", got)
	}
	_ = b.Flush(ctx)
	if len(f.reports) != 1 {
		t.Fatalf("a report went with nothing changed: %d", len(f.reports))
	}

	f.answer = nil
	f.hold = make(chan struct{})
	b2 := New(f.target("c17-close"), Options{BatchSize: 1_000_000, FlushInterval: time.Hour, Timeout: 20 * time.Millisecond})
	b2.Record(1)
	b2.Record(2)
	_ = b2.Close(ctx)
	if c := b2.Counts(); c.Dropped != 2 || c.Waiting != 0 {
		t.Fatalf("after close: %+v", c)
	}
	close(f.hold)
}

// FPP-140-C15: ApproxSize counts two bytes per character of a text.
func TestFPP140C15ApproxSize(t *testing.T) {
	short := &flowrensicv1.ActivityAttempt{WorkflowType: "a", StartedAt: timestamppb.Now()}
	long := &flowrensicv1.ActivityAttempt{WorkflowType: "ab", StartedAt: timestamppb.Now()}
	if d := ApproxSize(long) - ApproxSize(short); d != 2 {
		t.Fatalf("one more character adds %d bytes", d)
	}
	if ApproxSize(nil) != 8 {
		t.Fatal("nil is not 8")
	}
}
