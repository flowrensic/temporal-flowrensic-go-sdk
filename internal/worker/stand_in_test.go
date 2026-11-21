package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1/flowrensicv1connect"
)

// standIn is a Flowrensic for the tests of the telemetry: it answers the
// hello with forwarding on a 1 second interval, notes every call by its
// name, and answers a call with the error answers holds for its name.
type standIn struct {
	flowrensicv1connect.UnimplementedIngestServiceHandler
	mu        sync.Mutex
	calls     []string
	histories []*flowrensicv1.IngestEngineHistoryRequest
	answers   map[string]error
}

func (s *standIn) note(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, name)
	return s.answers[name]
}

func (s *standIn) answer(name string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.answers == nil {
		s.answers = map[string]error{}
	}
	s.answers[name] = err
}

func (s *standIn) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, one := range s.calls {
		if one == name {
			n++
		}
	}
	return n
}

// sent is the run ids of the histories it took, in order.
func (s *standIn) sent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []string{}
	for _, one := range s.histories {
		out = append(out, one.GetRunId())
	}
	return out
}

func (s *standIn) EphemeralRunHandshake(context.Context, *connect.Request[flowrensicv1.EphemeralRunHandshakeRequest]) (*connect.Response[flowrensicv1.EphemeralRunHandshakeResponse], error) {
	if err := s.note("EphemeralRunHandshake"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&flowrensicv1.EphemeralRunHandshakeResponse{Forward: true, IntervalSeconds: 1}), nil
}

func (s *standIn) WorkerBeat(context.Context, *connect.Request[flowrensicv1.WorkerBeatRequest]) (*connect.Response[flowrensicv1.WorkerBeatResponse], error) {
	if err := s.note("WorkerBeat"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&flowrensicv1.WorkerBeatResponse{}), nil
}

func (s *standIn) IngestEphemeralRunMetrics(context.Context, *connect.Request[flowrensicv1.IngestEphemeralRunMetricsRequest]) (*connect.Response[flowrensicv1.IngestEphemeralRunMetricsResponse], error) {
	if err := s.note("IngestEphemeralRunMetrics"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&flowrensicv1.IngestEphemeralRunMetricsResponse{}), nil
}

func (s *standIn) IngestWorkerMetrics(context.Context, *connect.Request[flowrensicv1.IngestWorkerMetricsRequest]) (*connect.Response[flowrensicv1.IngestWorkerMetricsResponse], error) {
	if err := s.note("IngestWorkerMetrics"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&flowrensicv1.IngestWorkerMetricsResponse{}), nil
}

func (s *standIn) IngestEngineHistory(_ context.Context, req *connect.Request[flowrensicv1.IngestEngineHistoryRequest]) (*connect.Response[flowrensicv1.IngestEngineHistoryResponse], error) {
	if err := s.note("IngestEngineHistory"); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.histories = append(s.histories, req.Msg)
	s.mu.Unlock()
	return connect.NewResponse(&flowrensicv1.IngestEngineHistoryResponse{}), nil
}

func (s *standIn) EphemeralRunHandover(context.Context, *connect.Request[flowrensicv1.EphemeralRunHandoverRequest]) (*connect.Response[flowrensicv1.EphemeralRunHandoverResponse], error) {
	if err := s.note("EphemeralRunHandover"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&flowrensicv1.EphemeralRunHandoverResponse{}), nil
}

func (s *standIn) IngestActivityEvents(context.Context, *connect.Request[flowrensicv1.IngestActivityEventsRequest]) (*connect.Response[flowrensicv1.IngestActivityEventsResponse], error) {
	if err := s.note("IngestActivityEvents"); err != nil {
		return nil, err
	}
	return connect.NewResponse(&flowrensicv1.IngestActivityEventsResponse{}), nil
}

// scrapes is a Prometheus endpoint that counts its scrapes.
type scrapes struct {
	mu sync.Mutex
	n  int
}

func (s *scrapes) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	_, _ = w.Write([]byte("up 1\n"))
}

func (s *scrapes) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// testEngine is an engine whose runs are in histories, by run id. A run not
// in it is unchanged. It notes each read.
type testEngine struct {
	mu        sync.Mutex
	reads     []string
	histories map[string]History
	maxBytes  int64
}

func (e *testEngine) set(runID string, h History) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.histories == nil {
		e.histories = map[string]History{}
	}
	e.histories[runID] = h
}

func (e *testEngine) read(runID string, sentEvents int64) (History, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reads = append(e.reads, runID)
	h, ok := e.histories[runID]
	if !ok || h.Events == sentEvents {
		return History{}, false
	}
	return h, true
}

func (e *testEngine) readsOf(runID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, one := range e.reads {
		if one == runID {
			n++
		}
	}
	return n
}

func (e *testEngine) Close() {}

// started is a telemetry pointed at a stand-in, with the engine and the
// metrics endpoint given, whose tick only the test runs.
type started struct {
	telemetry *Telemetry
	flow      *standIn
	scrapes   *scrapes
}

func startAgainstStandIn(t *testing.T, engine Engine) started {
	t.Helper()
	flow := &standIn{}
	path, handler := flowrensicv1connect.NewIngestServiceHandler(flow)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	metrics := &scrapes{}
	metricsServer := httptest.NewServer(metrics)
	t.Cleanup(metricsServer.Close)
	address := strings.TrimPrefix(metricsServer.URL, "http://")
	c, err := Start(context.Background(), Options{
		ServerBaseURL:        server.URL,
		CurrentEnv:           "test",
		CurrentEphemeralRun:  "1",
		WorkerID:             "w",
		MetricsAddress:       address,
		WorkerMetricsAddress: address,
		Engine:               engine,
		NoAnswer:             Fail(),
	})
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.ticker.Stop()
	c.mu.Unlock()
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })
	return started{telemetry: c, flow: flow, scrapes: metrics}
}

// see records an attempt of the run, as the activity interceptor does.
func see(c *Telemetry, runID string) {
	c.Sink().Record(&ActivityAttempt{RunKey: runID, WorkflowExecutionId: "wf-" + runID, Env: "test"})
}

func (e *testEngine) ReadHistory(_ context.Context, read HistoryRead) (History, bool, error) {
	e.mu.Lock()
	e.maxBytes = read.MaxBytes
	e.mu.Unlock()
	h, changed := e.read(read.RunID, read.SentEvents)
	return h, changed, nil
}
