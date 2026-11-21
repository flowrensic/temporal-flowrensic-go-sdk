package buffer

import (
	"errors"
	"net/http"
	"reflect"
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

func (c *testClock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = at
}

var start = time.Unix(1_000_000, 0)

// manual is a buffer of f with a tick of a second that only the test runs,
// on the clock of the test and with random giving its random numbers.
func manual(t *testing.T, f *fake, env string, random float64) (*Buffer[int], *testClock) {
	t.Helper()
	b := New(f.target(env), Options{BatchSize: 1_000_000, FlushInterval: time.Second})
	b.stopOnce.Do(func() { close(b.stop) })
	clock := &testClock{at: start}
	b.mu.Lock()
	b.now = clock.now
	b.random = func() float64 { return random }
	b.mu.Unlock()
	return b, clock
}

// settle waits until no send and no report of b is at work.
func settle(t *testing.T, b *Buffer[int]) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		b.mu.Lock()
		idle := b.sending == nil && b.reporting == nil && !b.scheduled
		b.mu.Unlock()
		if idle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the buffer did not settle")
		}
		time.Sleep(time.Millisecond)
	}
}

func (f *fake) counts() (sends, reports int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sends), len(f.reports)
}

// ticks runs the tick at each second from..to and gives the seconds at which
// a send and a report went.
func ticks(t *testing.T, b *Buffer[int], clock *testClock, f *fake, from, to int) (sentAt, reportedAt []int) {
	t.Helper()
	for second := from; second <= to; second++ {
		clock.set(start.Add(time.Duration(second) * time.Second))
		sends, reports := f.counts()
		b.onTick()
		settle(t, b)
		nowSends, nowReports := f.counts()
		if nowSends > sends {
			sentAt = append(sentAt, second)
		}
		if nowReports > reports {
			reportedAt = append(reportedAt, second)
		}
	}
	return sentAt, reportedAt
}

func unavailable([]int) error { return connect.NewError(connect.CodeUnavailable, nil) }

// FPP-140-C64: the first four failed sends in a row are tried again at the
// next tick; from the fifth on the wait doubles from two ticks, a random part
// of it between half and all, at most 60 seconds; no report goes while sends
// wait; a send that is answered clears the wait.
func TestFPP140C64BufferWaitsLongerAfterFourFailedSends(t *testing.T) {
	f := &fake{answer: unavailable}
	b, clock := manual(t, f, "c64", 1)
	for i := 0; i < 10; i++ {
		b.Record(i)
	}
	settle(t, b)
	sentAt, reportedAt := ticks(t, b, clock, f, 1, 40)
	if want := []int{1, 2, 3, 4, 5, 7, 11, 19, 35}; !reflect.DeepEqual(sentAt, want) {
		t.Fatalf("sends at %v, want %v", sentAt, want)
	}
	for _, second := range reportedAt {
		if second > 5 && !contains(sentAt, second) {
			t.Fatalf("a report went at %d while the sends waited (reports at %v)", second, reportedAt)
		}
	}

	f.mu.Lock()
	f.answer = nil
	f.mu.Unlock()
	sentAt, _ = ticks(t, b, clock, f, 41, 67)
	if want := []int{67}; !reflect.DeepEqual(sentAt, want) {
		t.Fatalf("the send after the wait at %v, want %v", sentAt, want)
	}
	f.mu.Lock()
	f.answer = unavailable
	f.mu.Unlock()
	b.Record(10)
	settle(t, b)
	sentAt, _ = ticks(t, b, clock, f, 68, 70)
	if want := []int{68, 69, 70}; !reflect.DeepEqual(sentAt, want) {
		t.Fatalf("after an answered send the sends at %v, want %v", sentAt, want)
	}

	half := &fake{answer: unavailable}
	h, hclock := manual(t, half, "c64-half", 0)
	h.Record(1)
	settle(t, h)
	sentAt, _ = ticks(t, h, hclock, half, 1, 12)
	if want := []int{1, 2, 3, 4, 5, 6, 8, 12}; !reflect.DeepEqual(sentAt, want) {
		t.Fatalf("with the least random wait, sends at %v, want %v", sentAt, want)
	}

	for _, one := range []struct {
		interval time.Duration
		failures int
		random   float64
		want     time.Duration
	}{
		{time.Second, 4, 1, time.Second},
		{time.Second, 5, 1, 2 * time.Second},
		{time.Second, 5, 0, time.Second},
		{time.Second, 6, 0.5, 3 * time.Second},
		{time.Second, 12, 1, time.Minute},
		{time.Second, 60, 1, time.Minute},
		{5 * time.Second, 6, 1, 20 * time.Second},
	} {
		if got := backoff(one.interval, one.failures, one.random); got != one.want {
			t.Errorf("backoff(%v, %d, %v) = %v, want %v", one.interval, one.failures, one.random, got, one.want)
		}
	}
}

func contains(list []int, one int) bool {
	for _, x := range list {
		if x == one {
			return true
		}
	}
	return false
}

func askedToWait(value string) func([]int) error {
	return func([]int) error {
		err := connect.NewError(connect.CodeUnavailable, errors.New("busy"))
		err.Meta().Set("Retry-After", value)
		return err
	}
}

// FPP-140-C65: a send answered with a Retry-After header, in seconds or as an
// HTTP date, is not tried again before that time, and at most 5 minutes
// later.
func TestFPP140C65BufferKeepsToRetryAfter(t *testing.T) {
	for _, one := range []struct {
		name  string
		value string
		want  []int
	}{
		{"seconds", "3", []int{1, 4, 7}},
		{"date", start.Add(11 * time.Second).UTC().Format(http.TimeFormat), []int{1, 11, 12, 13}},
		{"over five minutes", "3600", []int{1, 301}},
	} {
		t.Run(one.name, func(t *testing.T) {
			f := &fake{answer: askedToWait(one.value)}
			b, clock := manual(t, f, "c65", 1)
			b.Record(1)
			settle(t, b)
			last := one.want[len(one.want)-1]
			sentAt, _ := ticks(t, b, clock, f, 1, last)
			if !reflect.DeepEqual(sentAt, one.want) {
				t.Fatalf("Retry-After %q: sends at %v, want %v", one.value, sentAt, one.want)
			}
		})
	}
}

// FPP-140-C66: a send answered with an error of Flowrensic's own, internal or
// unknown, goes back and is sent again; one answered so five times is dropped
// and counted.
func TestFPP140C66BufferDropsASendFlowrensicFailedFiveTimes(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeInternal, connect.CodeUnknown} {
		t.Run(code.String(), func(t *testing.T) {
			failures := 2
			f := &fake{}
			f.answer = func([]int) error {
				if failures > 0 {
					failures--
					return connect.NewError(code, nil)
				}
				return nil
			}
			b, clock := manual(t, f, "c66", 1)
			for i := 0; i < 20; i++ {
				b.Record(i)
			}
			settle(t, b)
			sentAt, _ := ticks(t, b, clock, f, 1, 3)
			if got := b.Counts(); !reflect.DeepEqual(sentAt, []int{1, 2, 3}) || got.Dropped != 0 || got.Waiting != 0 {
				t.Fatalf("after two failures: sends at %v, counts %+v", sentAt, got)
			}

			f.mu.Lock()
			failures = 1_000
			f.mu.Unlock()
			for i := 0; i < 30; i++ {
				b.Record(i)
			}
			settle(t, b)
			sentAt, _ = ticks(t, b, clock, f, 4, 12)
			if got := b.Counts(); !reflect.DeepEqual(sentAt, []int{4, 5, 6, 7, 8}) || got.Dropped != 30 || got.Waiting != 0 {
				t.Fatalf("after five failures: sends at %v, counts %+v", sentAt, got)
			}
		})
	}
}
