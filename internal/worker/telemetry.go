// Package worker connects a worker to the Flowrensic server, whatever its
// engine: the hello and the worker telemetry, the sinks, the no answer
// strategies and the names of the interceptors. The engine's SDK gives it the
// tag its calls carry and the hook that reads the history of a run.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"

	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/transport"
)

// DefaultTimeout is how long one call to Flowrensic may take when Options
// carries no Timeout.
const DefaultTimeout = 5 * time.Second

// DefaultShutdownTimeout is how long Shutdown may send histories when Options
// carries no ShutdownTimeout.
const DefaultShutdownTimeout = 20 * time.Second

// defaultInterval is the interval a worker forwards on when no answer carried
// one, in seconds.
const defaultInterval = 5

// The interval of an answer the worker takes, in seconds: the range the
// server itself takes.
const (
	minInterval = 1
	maxInterval = 60
)

// historiesAtOnce is how many histories a worker reads and sends at once.
const historiesAtOnce = 16

// seenCap is the most runs the worker holds to send the histories of. Past it
// the oldest is dropped.
const seenCap = 10_000

// Options is what a worker tells Flowrensic about itself, and where its
// telemetry reads and sends.
type Options struct {
	// ServerBaseURL is the Flowrensic server, such as http://flowrensic:8080.
	ServerBaseURL string
	// CurrentEnv is the env this worker runs in, as the Flowrensic env list
	// names it, sent in the Flowrensic-Current-Env header of every call.
	CurrentEnv string
	// EnvAccessKey is the access key of that env, sent in the
	// Flowrensic-Env-Access-Key header of every call.
	EnvAccessKey string
	// CurrentEphemeralRun is which run of that env this is, sent in the
	// Flowrensic-Current-Ephemeral-Run header of every call. Empty, none.
	CurrentEphemeralRun string
	// WorkerID names this worker among the workers of the ephemeral run, sent
	// in the Flowrensic-Worker-Id header of every call. Empty, the host name
	// and the process id joined by a dash.
	WorkerID string
	// WorkerName is the name of this worker, the same for every copy of it,
	// which the sink sends with each attempt. Empty, none.
	WorkerName string
	// Version is the version this worker serves, the same one the activity
	// interceptor is given. Empty, none.
	Version string
	// WorkflowInterceptors is the names of the workflow interceptors this
	// worker registers, their Go type names or InterceptorNames of them. Sent
	// with the hello and every beat. Nil sends no list, which is not an empty
	// list.
	WorkflowInterceptors []string
	// MetricsAddress is the engine's Prometheus endpoint, as host:port, when
	// the worker's config carries one. Empty, none.
	MetricsAddress string
	// WorkerMetricsAddress is the worker's own Prometheus endpoint, as
	// host:port. Empty, the worker's own metrics are not forwarded.
	WorkerMetricsAddress string
	// Engine reads the history of a run from the engine. Nil, no history is
	// sent.
	Engine Engine
	// Tag names the SDK and its engine on every call.
	Tag transport.Tag
	// NoAnswer is what to do when the hello gets no answer. Zero, KeepTrying.
	NoAnswer NoAnswerStrategy
	// Timeout is how long one call to Flowrensic may take: the hello, each
	// forwarding call, the handover and each send of the sink. Zero,
	// DefaultTimeout.
	Timeout time.Duration
	// ShutdownTimeout is how long Shutdown may send histories, counted from
	// when it starts. Zero, DefaultShutdownTimeout.
	ShutdownTimeout time.Duration
	// Buffer is how the attempts of the sink wait and leave. Its Timeout is
	// Timeout when zero.
	Buffer BufferOptions
	// HTTPClient sends the calls and the scrapes. Nil, http.DefaultClient.
	HTTPClient *http.Client
}

// Answer is what Flowrensic answered the hello with.
type Answer struct {
	// Forward is whether the worker is asked to send the engine's metrics.
	Forward bool
	// IntervalSeconds is how often it sends.
	IntervalSeconds int32
	// SkipHistories is whether Flowrensic pulls the histories itself.
	SkipHistories bool
	// EngineMetricNames is the metrics of the engine Flowrensic reads. Empty,
	// every metric is sent.
	EngineMetricNames []string
}

// History is one read of the history of a run, as the engine's hook gives it.
type History struct {
	// Bytes is the history as the engine encodes it.
	Bytes []byte
	// Events is how many events it holds.
	Events int64
	// Closed is whether its last event ends the run, so nothing is added after
	// it.
	Closed bool
	// Namespace is where the run lives in the engine.
	Namespace string
	// TaskAttempt is the attempt of the run's pending workflow task, 0 for
	// none.
	TaskAttempt int32
}

// Engine is the hook of the engine's SDK that reads the history of a run.
type Engine interface {
	// ReadHistory reads the history of one run. When the engine holds no more
	// events for it than sentEvents, it returns changed false and reads
	// nothing more.
	ReadHistory(ctx context.Context, workflowID, runID string, sentEvents int64) (history History, changed bool, err error)
	// Close closes what ReadHistory opened. Called once, at shutdown.
	Close()
}

// Telemetry is a worker's connection to the Flowrensic server and its
// telemetry: on the interval of the answer it beats, forwards the engine's and
// its own metrics, and sends the histories of the runs its sink saw.
type Telemetry struct {
	options Options
	clients transport.Clients
	http    *http.Client
	inner   AttemptSink
	sink    *seeingSink

	mu               sync.Mutex
	answered         bool
	unanswered       bool
	forwarding       bool
	intervalSeconds  int
	sendingHistories bool
	metricNames      map[string]bool
	answer           Answer
	ticker           *time.Ticker
	working          *tickWork

	stopTicker  chan struct{}
	stopAsking  context.CancelFunc
	askingDone  chan struct{}
	shutdownOne sync.Once
}

type tickWork struct {
	done   chan struct{}
	cancel context.CancelFunc
}

// Start says hello to the Flowrensic server, so the ephemeral run and the
// worker show up there, and starts the telemetry. With no answer it does what
// options.NoAnswer says: by default it returns at once and asks again in the
// background. A refused hello returns the error, whose code connect.CodeOf
// reads, and so does a hello with no answer under Fail or Retry. Call Shutdown
// when the worker stops.
func Start(ctx context.Context, options Options) (*Telemetry, error) {
	if options.ServerBaseURL == "" {
		return nil, errors.New("flowrensic: ServerBaseURL is required")
	}
	if options.WorkerID == "" {
		options.WorkerID = DefaultWorkerID()
	}
	if options.Timeout <= 0 {
		options.Timeout = DefaultTimeout
	}
	if options.ShutdownTimeout <= 0 {
		options.ShutdownTimeout = DefaultShutdownTimeout
	}
	if options.Buffer.Timeout <= 0 {
		options.Buffer.Timeout = options.Timeout
	}
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	c := &Telemetry{
		options: options,
		clients: transport.New(transport.Context{
			ServerBaseURL:       options.ServerBaseURL,
			CurrentEnv:          options.CurrentEnv,
			EnvAccessKey:        options.EnvAccessKey,
			CurrentEphemeralRun: options.CurrentEphemeralRun,
			WorkerID:            options.WorkerID,
			HTTPClient:          httpClient,
			Tag:                 options.Tag,
		}),
		http:             httpClient,
		sendingHistories: true,
		stopTicker:       make(chan struct{}),
		askingDone:       make(chan struct{}),
	}
	c.inner = NewAttemptSink(SinkOptions{
		ServerBaseURL:       options.ServerBaseURL,
		CurrentEnv:          options.CurrentEnv,
		EnvAccessKey:        options.EnvAccessKey,
		CurrentEphemeralRun: options.CurrentEphemeralRun,
		WorkerID:            options.WorkerID,
		WorkerName:          options.WorkerName,
		Buffer:              options.Buffer,
		HTTPClient:          httpClient,
	}, options.Tag)
	c.sink = &seeingSink{inner: c.inner, client: c, seen: map[string]*seenRun{}}

	res, err := c.handshake(ctx)
	strategy := options.NoAnswer
	switch {
	case err == nil:
		c.take(res)
	case strategy.kind == keepTrying && noAnswer(err):
		c.unanswered = true
	case strategy.kind == giveUpQuietly && noAnswer(err):
	case strategy.kind == forwardAnyway && noAnswer(err):
		c.forwarding = options.MetricsAddress != "" && options.CurrentEphemeralRun != ""
		c.intervalSeconds = defaultInterval
	default:
		_ = c.inner.Close(context.Background())
		return nil, fmt.Errorf("flowrensic: hello: %w", err)
	}
	c.mu.Lock()
	c.startTimer()
	c.mu.Unlock()

	asking, stop := context.WithCancel(context.Background())
	c.stopAsking = stop
	if c.unanswered {
		go c.keepAsking(asking)
	} else {
		close(c.askingDone)
	}
	return c, nil
}

func (c *Telemetry) ask(ctx context.Context) (*flowrensicv1.EphemeralRunHandshakeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()
	res, err := c.clients.Ingest.EphemeralRunHandshake(ctx, connect.NewRequest(&flowrensicv1.EphemeralRunHandshakeRequest{
		MetricsAddress:       c.options.MetricsAddress,
		WorkerId:             c.options.WorkerID,
		Version:              c.options.Version,
		WorkflowInterceptors: InterceptorList(c.options.WorkflowInterceptors),
	}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

// handshake asks once, or under Retry as many times as it says while there is
// no answer.
func (c *Telemetry) handshake(ctx context.Context) (*flowrensicv1.EphemeralRunHandshakeResponse, error) {
	strategy := c.options.NoAnswer
	attempts := 1
	if strategy.kind == retry {
		attempts = max(1, strategy.attempts)
	}
	for attempt := 1; ; attempt++ {
		res, err := c.ask(ctx)
		if err == nil || attempt >= attempts || !noAnswer(err) {
			return res, err
		}
		select {
		case <-time.After(strategy.delay):
		case <-ctx.Done():
			return nil, err
		}
	}
}

// checkedInterval is the interval of an answer, or the default when it is out
// of range, which is said in the log.
func checkedInterval(seconds int32) int {
	if seconds >= minInterval && seconds <= maxInterval {
		return int(seconds)
	}
	log.Printf("flowrensic: Flowrensic sent a scrape interval of %d seconds, out of %d to %d; the worker keeps %d seconds",
		seconds, minInterval, maxInterval, defaultInterval)
	return defaultInterval
}

// take acts on an answer.
func (c *Telemetry) take(res *flowrensicv1.EphemeralRunHandshakeResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.answered = true
	c.forwarding = res.GetForward()
	c.intervalSeconds = checkedInterval(res.GetIntervalSeconds())
	c.sendingHistories = !res.GetSkipHistories()
	c.metricNames = map[string]bool{}
	for _, one := range res.GetEngineMetricNames() {
		c.metricNames[one] = true
	}
	c.answer = Answer{
		Forward:           res.GetForward(),
		IntervalSeconds:   res.GetIntervalSeconds(),
		SkipHistories:     res.GetSkipHistories(),
		EngineMetricNames: res.GetEngineMetricNames(),
	}
	if !c.sendingHistories {
		c.sink.forget()
	}
}

// keepAsking asks again in the background until an answer, a refusal or
// Shutdown, and then starts as if the answer had come at start.
func (c *Telemetry) keepAsking(ctx context.Context) {
	defer close(c.askingDone)
	delay := c.options.NoAnswer.keepTryingDelay()
	for {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
		res, err := c.ask(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			c.take(res)
			c.mu.Lock()
			c.startTimer()
			c.mu.Unlock()
			c.tick()
			return
		}
		if !noAnswer(err) {
			return
		}
	}
}

// startTimer starts the tick on the interval, when there is one. c.mu is held.
func (c *Telemetry) startTimer() {
	if c.intervalSeconds <= 0 || c.ticker != nil {
		return
	}
	t := time.NewTicker(time.Duration(c.intervalSeconds) * time.Second)
	c.ticker = t
	go func() {
		for {
			select {
			case <-c.stopTicker:
				return
			case <-t.C:
				c.tick()
			}
		}
	}()
}

// tick starts one collect, unless the one before still works: a tick is
// skipped, not queued.
func (c *Telemetry) tick() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.working != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &tickWork{done: make(chan struct{}), cancel: cancel}
	c.working = w
	go func() {
		defer close(w.done)
		c.collect(ctx, ctx)
		cancel()
		c.mu.Lock()
		c.working = nil
		c.mu.Unlock()
	}()
}

// collect sends a beat, the engine's scrape when told to forward, the
// worker's own scrape when it has the address, and the histories of the runs
// the sink saw. A part that fails waits for the next tick. ctx cuts the calls
// short; starting stops new histories from starting.
func (c *Telemetry) collect(ctx, starting context.Context) {
	c.call(ctx, func(ctx context.Context) error {
		_, err := c.clients.Ingest.WorkerBeat(ctx, connect.NewRequest(&flowrensicv1.WorkerBeatRequest{
			WorkerId: c.options.WorkerID, Version: c.options.Version, WorkflowInterceptors: InterceptorList(c.options.WorkflowInterceptors),
		}))
		return err
	})
	c.mu.Lock()
	forwarding, sending, names := c.forwarding, c.sendingHistories, c.metricNames
	c.mu.Unlock()
	if forwarding && c.options.MetricsAddress != "" {
		c.call(ctx, func(ctx context.Context) error {
			page, err := c.scrape(ctx, c.options.MetricsAddress)
			if err != nil {
				return err
			}
			_, err = c.clients.Ingest.IngestEphemeralRunMetrics(ctx, connect.NewRequest(&flowrensicv1.IngestEphemeralRunMetricsRequest{
				WorkerId: c.options.WorkerID, Exposition: onlyNamed(page, names),
			}))
			return err
		})
	}
	if c.options.WorkerMetricsAddress != "" {
		c.call(ctx, func(ctx context.Context) error {
			page, err := c.scrape(ctx, c.options.WorkerMetricsAddress)
			if err != nil {
				return err
			}
			_, err = c.clients.Ingest.IngestWorkerMetrics(ctx, connect.NewRequest(&flowrensicv1.IngestWorkerMetricsRequest{
				WorkerId: c.options.WorkerID, Exposition: page,
			}))
			return err
		})
	}
	if !sending {
		return
	}
	runs := c.sink.runs()
	next := make(chan *seenRun, len(runs))
	for _, one := range runs {
		next <- one
	}
	close(next)
	var wg sync.WaitGroup
	for range min(historiesAtOnce, len(runs)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for one := range next {
				if starting.Err() != nil || ctx.Err() != nil {
					return
				}
				_ = c.forwardHistory(ctx, one)
			}
		}()
	}
	wg.Wait()
}

// call runs one call with the timeout, and drops its error.
func (c *Telemetry) call(ctx context.Context, job func(context.Context) error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()
	_ = job(ctx)
}

func (c *Telemetry) scrape(ctx context.Context, address string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/metrics", nil)
	if err != nil {
		return "", err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metrics endpoint returned %d", res.StatusCode)
	}
	body, err := io.ReadAll(res.Body)
	return string(body), err
}

// histogramSeries are the suffixes of the series of a histogram, which its
// HELP and TYPE lines leave out.
var histogramSeries = []string{"_bucket", "_sum", "_count"}

// onlyNamed keeps the lines of the engine's page the server reads: a series
// line whose name is named, and a HELP or TYPE line whose name, or that name
// with a histogram suffix, is named. No names keeps the whole page.
func onlyNamed(page string, names map[string]bool) string {
	if len(names) == 0 {
		return page
	}
	var kept []string
	for _, line := range strings.Split(page, "\n") {
		text := strings.TrimSpace(line)
		if strings.HasPrefix(text, "#") {
			words := strings.Fields(text)
			if len(words) >= 3 && (words[1] == "HELP" || words[1] == "TYPE") {
				ok := names[words[2]]
				for _, s := range histogramSeries {
					ok = ok || names[words[2]+s]
				}
				if ok {
					kept = append(kept, line)
				}
			}
			continue
		}
		name := text
		if end := strings.IndexAny(text, " \t{"); end >= 0 {
			name = text[:end]
		}
		if names[name] {
			kept = append(kept, line)
		}
	}
	if len(kept) == 0 {
		return ""
	}
	return strings.Join(kept, "\n") + "\n"
}

// forwardHistory sends the history of one run, only when the engine holds
// more events for it than were sent last. A run whose history was sent with
// the event that ends it is not sent again.
func (c *Telemetry) forwardHistory(ctx context.Context, run *seenRun) error {
	if c.options.Engine == nil {
		return nil
	}
	history, changed, err := c.options.Engine.ReadHistory(ctx, run.workflowID, run.runID, run.sentEvents())
	if err != nil || !changed {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()
	_, err = c.clients.Ingest.IngestEngineHistory(callCtx, connect.NewRequest(&flowrensicv1.IngestEngineHistoryRequest{
		RunId:       run.runID,
		Namespace:   history.Namespace,
		WorkflowId:  run.workflowID,
		TaskAttempt: history.TaskAttempt,
		WorkerId:    c.options.WorkerID,
		WorkerName:  c.options.WorkerName,
		History:     history.Bytes,
	}))
	if err != nil {
		return err
	}
	run.sent(history.Events)
	if history.Events > 0 && history.Closed {
		c.sink.done(run.runID)
	}
	return nil
}

// WorkerID is the worker id the hello carried.
func (c *Telemetry) WorkerID() string { return c.options.WorkerID }

// Answer is what Flowrensic answered the hello with, the zero Answer while it
// has not answered.
func (c *Telemetry) Answer() Answer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.answer
}

// Answered is whether Flowrensic answered the hello, at start or later in the
// background.
func (c *Telemetry) Answered() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.answered
}

// WaitAnswered waits until Flowrensic answers, and is true then. It is false
// when Flowrensic refuses in the background, the strategy ran with no answer,
// Shutdown came first, or ctx ended.
func (c *Telemetry) WaitAnswered(ctx context.Context) bool {
	select {
	case <-c.askingDone:
	case <-ctx.Done():
	}
	return c.Answered()
}

// Forwarding is whether the worker sends the engine's metrics.
func (c *Telemetry) Forwarding() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.forwarding
}

// IntervalSeconds is the interval the worker sends on, 0 for none.
func (c *Telemetry) IntervalSeconds() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.intervalSeconds
}

// SendingHistories is whether the worker sends the histories of the runs it
// saw. False when the answer said Flowrensic pulls them itself.
func (c *Telemetry) SendingHistories() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sendingHistories
}

// Sink is the sink to give the activity interceptor. It notes the run of each
// attempt, so its history is sent.
func (c *Telemetry) Sink() AttemptSink { return c.sink }

// Shutdown runs the last tick, closes the sink, and hands the ephemeral run
// over. Histories that did not start within ShutdownTimeout are left out. A
// worker under KeepTrying that never got an answer hands nothing over.
func (c *Telemetry) Shutdown(ctx context.Context) error {
	var err error
	c.shutdownOne.Do(func() { err = c.shutdown(ctx) })
	return err
}

func (c *Telemetry) shutdown(ctx context.Context) error {
	starting, stopStarting := context.WithTimeout(ctx, c.options.ShutdownTimeout)
	defer stopStarting()
	c.stopAsking()
	<-c.askingDone
	close(c.stopTicker)
	c.mu.Lock()
	if c.ticker != nil {
		c.ticker.Stop()
	}
	w := c.working
	c.mu.Unlock()
	if w != nil {
		w.cancel()
		<-w.done
	}
	c.mu.Lock()
	handOver := c.answered || !c.unanswered
	c.mu.Unlock()
	if handOver {
		c.collect(ctx, starting)
	}
	err := c.inner.Close(ctx)
	if handOver {
		c.call(ctx, func(ctx context.Context) error {
			_, err := c.clients.Ingest.EphemeralRunHandover(ctx, connect.NewRequest(&flowrensicv1.EphemeralRunHandoverRequest{
				WorkerId: c.options.WorkerID, Version: c.options.Version,
			}))
			return err
		})
	}
	if c.options.Engine != nil {
		c.options.Engine.Close()
	}
	return err
}

type seenRun struct {
	runID, workflowID string
	mu                sync.Mutex
	events            int64
}

func (r *seenRun) sentEvents() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.events
}

func (r *seenRun) sent(n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = n
}

// seeingSink notes the run of each attempt, at most seenCap runs, the oldest
// dropped past it, while the worker sends histories.
type seeingSink struct {
	inner  AttemptSink
	client *Telemetry
	mu     sync.Mutex
	seen   map[string]*seenRun
	order  []string
}

func (s *seeingSink) Record(one *ActivityAttempt) {
	if one.GetRunKey() != "" && one.GetWorkflowExecutionId() != "" && s.client.SendingHistories() {
		s.mu.Lock()
		if _, ok := s.seen[one.GetRunKey()]; !ok {
			s.seen[one.GetRunKey()] = &seenRun{runID: one.GetRunKey(), workflowID: one.GetWorkflowExecutionId()}
			s.order = append(s.order, one.GetRunKey())
			for len(s.seen) > seenCap {
				oldest := s.order[0]
				s.order = s.order[1:]
				delete(s.seen, oldest)
			}
		}
		s.mu.Unlock()
	}
	s.inner.Record(one)
}

func (s *seeingSink) Flush(ctx context.Context) error { return s.inner.Flush(ctx) }

func (s *seeingSink) Close(ctx context.Context) error { return s.inner.Close(ctx) }

func (s *seeingSink) Counts() SinkCounts { return s.inner.Counts() }

// Spent counts the time of the SDK's own code under the env of the sink.
func (s *seeingSink) Spent(d time.Duration) {
	if t, ok := s.inner.(TimeSpender); ok {
		t.Spent(d)
	}
}

func (s *seeingSink) runs() []*seenRun {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*seenRun, 0, len(s.order))
	for _, id := range s.order {
		if one, ok := s.seen[id]; ok {
			out = append(out, one)
		}
	}
	return out
}

func (s *seeingSink) done(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.seen, runID)
	for i, id := range s.order {
		if id == runID {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

func (s *seeingSink) forget() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = map[string]*seenRun{}
	s.order = nil
}
