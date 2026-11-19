// Package buffer holds the records of a worker until they are sent to
// Flowrensic in batches, with the rules every SDK keeps: one send at a time, a
// limit on the records and bytes waiting, a send with no answer sent again
// whole with its number, and the counts sent as reports.
package buffer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"connectrpc.com/connect"

	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/owntime"
)

// The defaults of Options, the same in every SDK.
const (
	DefaultBatchSize       = 64
	DefaultFlushInterval   = time.Second
	DefaultTimeout         = 5 * time.Second
	DefaultMaxWaiting      = 10_000
	DefaultMaxBatchSize    = 500
	DefaultMaxWaitingBytes = 16 * 1024 * 1024
)

// Options is how records wait and leave. A zero field takes its default.
type Options struct {
	BatchSize       int
	FlushInterval   time.Duration
	Timeout         time.Duration
	MaxWaiting      int
	MaxBatchSize    int
	MaxWaitingBytes int
}

func (o Options) withDefaults() Options {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = DefaultFlushInterval
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.MaxWaiting <= 0 {
		o.MaxWaiting = DefaultMaxWaiting
	}
	if o.MaxBatchSize <= 0 {
		o.MaxBatchSize = DefaultMaxBatchSize
	}
	if o.MaxWaitingBytes <= 0 {
		o.MaxWaitingBytes = DefaultMaxWaitingBytes
	}
	return o
}

// SendID is which send a call is: the buffer that sends it and the number of
// the send. A send sent again keeps its number.
type SendID struct {
	Sender     string
	SendNumber int64
}

// Target is what a buffer needs from the sink that owns it.
type Target[T any] struct {
	// Send sends one batch.
	Send func(ctx context.Context, records []T, id SendID) error
	// EnvOf is the env a record is counted under.
	EnvOf func(T) string
	// SizeOf is the size of a record, as ApproxSize gives it.
	SizeOf func(T) int
	// Report sends the counts. Nil, the counts stay in the worker.
	Report func(ctx context.Context, reports []*flowrensicv1.WorkerBufferReport, id SendID) error
	// Env is the env of the sink: the time of its own code is counted under
	// it, and a sink that reports sends that time with its report.
	Env string
}

// Counts is what a buffer lost and holds, over every env.
type Counts struct {
	// Dropped is the records dropped since the buffer was made.
	Dropped int64
	// TimedOut is the sends given up at their timeout since the buffer was made.
	TimedOut int64
	// Waiting is the records waiting to be sent now.
	Waiting int
	// WaitingBytes is the size of the records waiting now.
	WaitingBytes int
}

type entry[T any] struct {
	record T
	size   int
}

type keptSend[T any] struct {
	entries []entry[T]
	id      SendID
	bytes   int
}

type keptReport struct {
	reports  []*flowrensicv1.WorkerBufferReport
	id       SendID
	dropped  map[string]int64
	timedOut map[string]int64
	waiting  map[string]int64
}

// Buffer holds records and sends them. Record never waits and never panics.
type Buffer[T any] struct {
	target  Target[T]
	options Options

	mu          sync.Mutex
	queue       []entry[T]
	head        int
	queuedBytes int
	kept        *keptSend[T]
	keptReport  *keptReport

	recordsSender string
	reportsSender string
	sendNumber    int64
	reportNumber  int64

	waitingByEnv    map[string]int64
	dropped         map[string]int64
	timedOut        map[string]int64
	reportedWaiting map[string]int64
	droppedTotal    int64
	timedOutTotal   int64

	sending   chan struct{}
	reporting chan struct{}
	scheduled bool
	failing   bool

	stop     chan struct{}
	stopOnce sync.Once
}

func senderID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// New makes a buffer and starts its tick.
func New[T any](target Target[T], options Options) *Buffer[T] {
	id := senderID()
	b := &Buffer[T]{
		target:          target,
		options:         options.withDefaults(),
		recordsSender:   id + "/records",
		reportsSender:   id + "/reports",
		waitingByEnv:    map[string]int64{},
		dropped:         map[string]int64{},
		timedOut:        map[string]int64{},
		reportedWaiting: map[string]int64{},
		stop:            make(chan struct{}),
	}
	go b.tick()
	return b
}

func add(counts map[string]int64, env string, n int64) {
	now := counts[env] + n
	if now == 0 {
		delete(counts, env)
	} else {
		counts[env] = now
	}
}

func (b *Buffer[T]) tick() {
	t := time.NewTicker(b.options.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-t.C:
			b.mu.Lock()
			b.startSend()
			b.report()
			b.mu.Unlock()
		}
	}
}

func (b *Buffer[T]) queued() int { return len(b.queue) - b.head }
func (b *Buffer[T]) waitingCount() int {
	n := b.queued()
	if b.kept != nil {
		n += len(b.kept.entries)
	}
	return n
}

func (b *Buffer[T]) waitingBytes() int {
	n := b.queuedBytes
	if b.kept != nil {
		n += b.kept.bytes
	}
	return n
}

func (b *Buffer[T]) drop(env string, n int64) {
	add(b.dropped, env, n)
	b.droppedTotal += n
}

func (b *Buffer[T]) take(count int) []entry[T] {
	out := make([]entry[T], 0, count)
	var zero entry[T]
	for i := 0; i < count && b.head < len(b.queue); i++ {
		one := b.queue[b.head]
		b.queue[b.head] = zero
		b.head++
		add(b.waitingByEnv, b.target.EnvOf(one.record), -1)
		b.queuedBytes -= one.size
		out = append(out, one)
	}
	return out
}

// compact gives back the slots before head once a batch leaves.
func (b *Buffer[T]) compact() {
	if b.head > 1024 && b.head*2 > len(b.queue) {
		b.queue = append([]entry[T](nil), b.queue[b.head:]...)
		b.head = 0
	}
}

// dropOldest drops the oldest queued records past either limit, never the
// kept send, which Flowrensic may already hold. The kept send does not count
// toward the count limit, and counts toward the memory limit.
func (b *Buffer[T]) dropOldest() {
	if excess := b.queued() - b.options.MaxWaiting; excess > 0 {
		for _, one := range b.take(excess) {
			b.drop(b.target.EnvOf(one.record), 1)
		}
	}
	for b.queued() > 0 && b.waitingBytes() > b.options.MaxWaitingBytes {
		for _, one := range b.take(1) {
			b.drop(b.target.EnvOf(one.record), 1)
		}
	}
}

func sizeOf[T any](entries []entry[T]) int {
	n := 0
	for _, one := range entries {
		n += one.size
	}
	return n
}

func (b *Buffer[T]) putBack(entries []entry[T], id SendID) {
	b.kept = &keptSend[T]{entries: entries, id: id, bytes: sizeOf(entries)}
	for _, one := range entries {
		add(b.waitingByEnv, b.target.EnvOf(one.record), 1)
	}
	b.dropOldest()
}

func (b *Buffer[T]) nextSend() ([]entry[T], SendID) {
	if b.kept != nil {
		out := b.kept
		b.kept = nil
		for _, one := range out.entries {
			add(b.waitingByEnv, b.target.EnvOf(one.record), -1)
		}
		return out.entries, out.id
	}
	entries := b.take(min(b.options.MaxBatchSize, b.queued()))
	b.compact()
	b.sendNumber++
	return entries, SendID{Sender: b.recordsSender, SendNumber: b.sendNumber}
}

// sendAgain is whether Flowrensic did not answer or answered try again later.
func sendAgain(err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeResourceExhausted:
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}

func timedOut(err error) bool {
	return connect.CodeOf(err) == connect.CodeDeadlineExceeded || errors.Is(err, context.DeadlineExceeded)
}

// startSend starts the next send when none is at work and records wait. Once
// it is answered the next starts right away. A send with no answer, or
// answered try again later, goes back and is tried again at the next tick. A
// send refused for any other reason is dropped. b.mu is held.
func (b *Buffer[T]) startSend() {
	if b.sending != nil || b.waitingCount() == 0 {
		return
	}
	began := owntime.Now()
	entries, id := b.nextSend()
	records := make([]T, len(entries))
	for i, one := range entries {
		records[i] = one.record
	}
	done := make(chan struct{})
	b.sending = done
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), b.options.Timeout)
		err := b.safeSend(ctx, records, id)
		cancel()
		b.mu.Lock()
		defer b.mu.Unlock()
		if err == nil {
			b.failing = false
		} else {
			if timedOut(err) {
				envs := map[string]bool{}
				for _, one := range records {
					envs[b.target.EnvOf(one)] = true
				}
				for env := range envs {
					add(b.timedOut, env, 1)
				}
				b.timedOutTotal++
			}
			if sendAgain(err) {
				b.failing = true
				b.putBack(entries, id)
			} else {
				for _, one := range records {
					b.drop(b.target.EnvOf(one), 1)
				}
			}
		}
		b.sending = nil
		close(done)
		if !b.failing {
			b.startSend()
		}
	}()
	owntime.Spent(b.target.Env, time.Since(began))
}

// safeSend calls Send, and turns a panic in it into an error.
func (b *Buffer[T]) safeSend(ctx context.Context, records []T, id SendID) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("flowrensic: the send panicked")
		}
	}()
	return b.target.Send(ctx, records, id)
}

func (b *Buffer[T]) schedule() {
	if b.scheduled || b.failing || b.sending != nil {
		return
	}
	b.scheduled = true
	go func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.scheduled = false
		b.startSend()
	}()
}

// nextReport is a new report when a count changed since the last report that
// was answered, with what was counted since, the time of the SDK's own code
// under the env of the buffer included. b.mu is held.
func (b *Buffer[T]) nextReport() *keptReport {
	own := owntime.Take(b.target.Env)
	envs := []string{}
	seen := map[string]bool{}
	note := func(m map[string]int64) {
		for env := range m {
			if !seen[env] {
				seen[env] = true
				envs = append(envs, env)
			}
		}
	}
	note(b.dropped)
	note(b.timedOut)
	note(b.waitingByEnv)
	note(b.reportedWaiting)
	if own != nil && !seen[b.target.Env] {
		seen[b.target.Env] = true
		envs = append(envs, b.target.Env)
	}
	changed := own != nil
	for _, env := range envs {
		if b.dropped[env] != 0 || b.timedOut[env] != 0 || b.waitingByEnv[env] != b.reportedWaiting[env] {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	copyOf := func(m map[string]int64) map[string]int64 {
		out := make(map[string]int64, len(m))
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	one := &keptReport{dropped: copyOf(b.dropped), timedOut: copyOf(b.timedOut), waiting: copyOf(b.waitingByEnv)}
	for _, env := range envs {
		r := &flowrensicv1.WorkerBufferReport{
			Env:            env,
			RecordsDropped: one.dropped[env],
			SendsTimedOut:  one.timedOut[env],
			RecordsWaiting: one.waiting[env],
		}
		if env == b.target.Env && own != nil {
			r.OwnTimeCounts = own
		}
		one.reports = append(one.reports, r)
	}
	b.reportNumber++
	one.id = SendID{Sender: b.reportsSender, SendNumber: b.reportNumber}
	return one
}

// report sends at most one report at a time. A report that was not answered
// is sent again as it was. b.mu is held.
func (b *Buffer[T]) report() {
	if b.target.Report == nil || b.reporting != nil {
		return
	}
	if b.keptReport == nil {
		b.keptReport = b.nextReport()
	}
	if b.keptReport == nil {
		return
	}
	one := b.keptReport
	done := make(chan struct{})
	b.reporting = done
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), b.options.Timeout)
		err := b.safeReport(ctx, one)
		cancel()
		b.mu.Lock()
		defer b.mu.Unlock()
		if err == nil {
			for env, n := range one.dropped {
				add(b.dropped, env, -n)
			}
			for env, n := range one.timedOut {
				add(b.timedOut, env, -n)
			}
			b.reportedWaiting = one.waiting
			b.keptReport = nil
		}
		b.reporting = nil
		close(done)
	}()
}

func (b *Buffer[T]) safeReport(ctx context.Context, one *keptReport) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("flowrensic: the report panicked")
		}
	}()
	return b.target.Report(ctx, one.reports, one.id)
}

// Record adds one record. It never waits on Flowrensic.
func (b *Buffer[T]) Record(one T) {
	size := b.target.SizeOf(one)
	env := b.target.EnvOf(one)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queue = append(b.queue, entry[T]{record: one, size: size})
	b.queuedBytes += size
	add(b.waitingByEnv, env, 1)
	b.dropOldest()
	if b.waitingCount() >= b.options.BatchSize {
		b.schedule()
	}
}

func wait(ctx context.Context, done chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush sends what waits and the counts, and waits for them to land. A send
// that goes back ends it, after one more try when the sends were already
// going back.
func (b *Buffer[T]) Flush(ctx context.Context) error {
	tried := false
	for {
		b.mu.Lock()
		if b.sending != nil {
			done := b.sending
			b.mu.Unlock()
			if err := wait(ctx, done); err != nil {
				return err
			}
			continue
		}
		if b.waitingCount() > 0 && (!b.failing || !tried) {
			tried = true
			b.failing = false
			b.startSend()
			b.mu.Unlock()
			continue
		}
		b.mu.Unlock()
		break
	}
	return b.flushReport(ctx)
}

func (b *Buffer[T]) flushReport(ctx context.Context) error {
	b.mu.Lock()
	done := b.reporting
	b.mu.Unlock()
	if done != nil {
		if err := wait(ctx, done); err != nil {
			return err
		}
	}
	b.mu.Lock()
	b.report()
	done = b.reporting
	b.mu.Unlock()
	if done != nil {
		return wait(ctx, done)
	}
	return nil
}

// Close stops the tick, sends what waits, drops what still waits after, and
// sends the last report.
func (b *Buffer[T]) Close(ctx context.Context) error {
	b.stopOnce.Do(func() { close(b.stop) })
	err := b.Flush(ctx)
	b.mu.Lock()
	if b.kept != nil {
		for _, one := range b.kept.entries {
			env := b.target.EnvOf(one.record)
			add(b.waitingByEnv, env, -1)
			b.drop(env, 1)
		}
		b.kept = nil
	}
	for _, one := range b.take(b.queued()) {
		b.drop(b.target.EnvOf(one.record), 1)
	}
	b.compact()
	b.mu.Unlock()
	if rerr := b.flushReport(ctx); err == nil {
		err = rerr
	}
	return err
}

// Counts is what the buffer lost and holds.
func (b *Buffer[T]) Counts() Counts {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Counts{Dropped: b.droppedTotal, TimedOut: b.timedOutTotal, Waiting: b.waitingCount(), WaitingBytes: b.waitingBytes()}
}

// Spent counts d of the SDK's own code under the env of the buffer.
func (b *Buffer[T]) Spent(d time.Duration) {
	owntime.Spent(b.target.Env, d)
}
