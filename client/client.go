// Package client connects a Go worker and a Go client to the Flowrensic
// server, beside go.temporal.io/sdk/client: the hello and the worker
// telemetry, the sinks, the client interceptor, and the replay set and deploy
// calls of a CI job.
package client

import (
	"context"
	"net/http"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	tclient "go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/engine"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/transport"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/worker"
)

// The headers every Flowrensic call carries.
const (
	// CurrentEnvHeader carries the env.
	CurrentEnvHeader = transport.CurrentEnvHeader
	// EnvAccessKeyHeader carries the env access key.
	EnvAccessKeyHeader = transport.EnvAccessKeyHeader
	// CurrentEphemeralRunHeader carries the ephemeral run.
	CurrentEphemeralRunHeader = transport.CurrentEphemeralRunHeader
	// SdkHeader carries the language and version of this SDK.
	SdkHeader = transport.SdkHeader
	// TemporalSdkHeader carries the version of the Temporal Go SDK.
	TemporalSdkHeader = engine.TemporalSdkHeader
	// WorkerIDHeader carries the worker id on every call of a worker, so a
	// call refused before its body is read still names its worker.
	WorkerIDHeader = transport.WorkerIDHeader
)

// SdkHeaderValue is go, a slash and the version of this SDK: the tag of the
// module in the build info of the program, dev when the module has no
// version there or is replaced by a folder.
var SdkHeaderValue = engine.SdkHeaderValue

// TemporalSdkHeaderValue is the version of go.temporal.io/sdk in the build
// info of the program, with no leading v, empty when not known. Empty sends no
// header.
var TemporalSdkHeaderValue = engine.TemporalSdkHeaderValue

// DefaultTimeout is how long one call to Flowrensic may take when Options
// carries no Timeout.
const DefaultTimeout = worker.DefaultTimeout

// DefaultShutdownTimeout is how long Shutdown may send histories when Options
// carries no ShutdownTimeout.
const DefaultShutdownTimeout = worker.DefaultShutdownTimeout

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
	// TemporalClient is the worker's own Temporal client, used to read the
	// histories of the runs it saw. Needed for an engine reached over TLS or
	// with an API key, such as Temporal Cloud. It is not closed at shutdown.
	// Nil, a plain client to TemporalAddress and Namespace is dialed the first
	// time a history is read, and closed at shutdown.
	TemporalClient tclient.Client
	// TemporalAddress is the gRPC address of Temporal, for TemporalClient nil.
	TemporalAddress string
	// Namespace is the namespace the worker polls.
	Namespace string
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
type Answer = worker.Answer

// Client is a worker's connection to the Flowrensic server and its
// telemetry: on the interval of the answer it beats, forwards the engine's and
// its own metrics, and sends the histories of the runs its sink saw.
type Client struct {
	telemetry *worker.Telemetry
	temporal  *temporalHistories
}

// Connect says hello to the Flowrensic server, so the ephemeral run and the
// worker show up there, and starts the telemetry. With no answer it does what
// options.NoAnswer says: by default it returns at once and asks again in the
// background. A refused hello returns the error, whose code connect.CodeOf
// reads, and so does a hello with no answer under Fail or Retry. Call Shutdown
// when the worker stops.
func Connect(ctx context.Context, options Options) (*Client, error) {
	histories := &temporalHistories{client: options.TemporalClient, address: options.TemporalAddress, namespace: options.Namespace}
	telemetry, err := worker.Start(ctx, worker.Options{
		ServerBaseURL:        options.ServerBaseURL,
		CurrentEnv:           options.CurrentEnv,
		EnvAccessKey:         options.EnvAccessKey,
		CurrentEphemeralRun:  options.CurrentEphemeralRun,
		WorkerID:             options.WorkerID,
		WorkerName:           options.WorkerName,
		Version:              options.Version,
		WorkflowInterceptors: options.WorkflowInterceptors,
		MetricsAddress:       options.MetricsAddress,
		WorkerMetricsAddress: options.WorkerMetricsAddress,
		Engine:               histories,
		Tag:                  engine.Tag(),
		NoAnswer:             options.NoAnswer,
		Timeout:              options.Timeout,
		ShutdownTimeout:      options.ShutdownTimeout,
		Buffer:               options.Buffer,
		HTTPClient:           options.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return &Client{telemetry: telemetry, temporal: histories}, nil
}

// WorkerID is the worker id the hello carried.
func (c *Client) WorkerID() string { return c.telemetry.WorkerID() }

// Answer is what Flowrensic answered the hello with, the zero Answer while it
// has not answered.
func (c *Client) Answer() Answer { return c.telemetry.Answer() }

// Answered is whether Flowrensic answered the hello, at start or later in the
// background.
func (c *Client) Answered() bool { return c.telemetry.Answered() }

// WaitAnswered waits until Flowrensic answers, and is true then. It is false
// when Flowrensic refuses in the background, the strategy ran with no answer,
// Shutdown came first, or ctx ended.
func (c *Client) WaitAnswered(ctx context.Context) bool { return c.telemetry.WaitAnswered(ctx) }

// Forwarding is whether the worker sends the engine's metrics.
func (c *Client) Forwarding() bool { return c.telemetry.Forwarding() }

// IntervalSeconds is the interval the worker sends on, 0 for none.
func (c *Client) IntervalSeconds() int { return c.telemetry.IntervalSeconds() }

// SendingHistories is whether the worker sends the histories of the runs it
// saw. False when the answer said Flowrensic pulls them itself.
func (c *Client) SendingHistories() bool { return c.telemetry.SendingHistories() }

// TemporalDialed is whether the telemetry dialed Temporal itself to read a
// history.
func (c *Client) TemporalDialed() bool { return c.temporal.wasDialed() }

// Sink is the sink to give the activity interceptor. It notes the run of each
// attempt, so its history is sent.
func (c *Client) Sink() AttemptSink { return c.telemetry.Sink() }

// Shutdown runs the last tick, closes the sink, and hands the ephemeral run
// over. Histories that did not start within ShutdownTimeout are left out. A
// worker under KeepTrying that never got an answer hands nothing over.
func (c *Client) Shutdown(ctx context.Context) error { return c.telemetry.Shutdown(ctx) }

// temporalHistories reads the history of a run from Temporal, with the
// worker's own client, or one dialed the first time a history is read.
type temporalHistories struct {
	client    tclient.Client
	address   string
	namespace string

	mu     sync.Mutex
	dialed tclient.Client
	tried  bool
}

// temporalClient is the worker's Temporal client, or one dialed the first
// time a history is read. One that failed to dial is tried again next time.
func (h *temporalHistories) temporalClient() (tclient.Client, error) {
	if h.client != nil {
		return h.client, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.dialed != nil {
		return h.dialed, nil
	}
	h.tried = true
	tc, err := tclient.Dial(tclient.Options{HostPort: h.address, Namespace: h.namespace})
	if err != nil {
		return nil, err
	}
	h.dialed = tc
	return tc, nil
}

func (h *temporalHistories) wasDialed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.tried
}

// closing is whether the event ends the run, so nothing is added after it.
func closing(e *historypb.HistoryEvent) bool {
	switch e.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		return true
	}
	return false
}

// ReadHistory reads the history of one run, only when Temporal holds more
// events for it than were sent last.
func (h *temporalHistories) ReadHistory(ctx context.Context, workflowID, runID string, sentEvents int64) (worker.History, bool, error) {
	tc, err := h.temporalClient()
	if err != nil {
		return worker.History{}, false, err
	}
	described, err := tc.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return worker.History{}, false, err
	}
	length := described.GetWorkflowExecutionInfo().GetHistoryLength()
	if length > 0 && length == sentEvents {
		return worker.History{}, false, nil
	}
	var events []*historypb.HistoryEvent
	it := tc.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for it.HasNext() {
		e, err := it.Next()
		if err != nil {
			return worker.History{}, false, err
		}
		events = append(events, e)
	}
	bytes, err := proto.Marshal(&historypb.History{Events: events})
	if err != nil {
		return worker.History{}, false, err
	}
	return worker.History{
		Bytes:       bytes,
		Events:      int64(len(events)),
		Closed:      len(events) > 0 && closing(events[len(events)-1]),
		Namespace:   h.namespace,
		TaskAttempt: described.GetPendingWorkflowTask().GetAttempt(),
	}, true, nil
}

// Close closes the client dialed to read histories, not the worker's own.
func (h *temporalHistories) Close() {
	h.mu.Lock()
	tc := h.dialed
	h.mu.Unlock()
	if tc != nil {
		tc.Close()
	}
}
