package worker

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"

	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/buffer"
	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/transport"
)

// ActivityAttempt is one attempt of an activity, as the activity interceptor
// records it.
type ActivityAttempt = flowrensicv1.ActivityAttempt

// Outcome is how an attempt ended.
type Outcome = flowrensicv1.Outcome

// The outcomes of an attempt.
const (
	OutcomeCompleted = flowrensicv1.Outcome_OUTCOME_COMPLETED
	OutcomeFailed    = flowrensicv1.Outcome_OUTCOME_FAILED
	OutcomeCanceled  = flowrensicv1.Outcome_OUTCOME_CANCELED
)

// BufferOptions is how records wait and leave, the same for every sink. A
// zero field takes its default.
type BufferOptions struct {
	// BatchSize sends once this many records wait. Zero, 64.
	BatchSize int
	// FlushInterval sends at least this often. Zero, a second.
	FlushInterval time.Duration
	// Timeout is how long one send may take before it is given up. Zero, 5
	// seconds.
	Timeout time.Duration
	// MaxWaiting is how many records may wait. Past it the oldest waiting
	// record is dropped. Zero, 10,000.
	MaxWaiting int
	// MaxBatchSize is how many records one send carries at most. Zero, 500.
	MaxBatchSize int
	// MaxWaitingBytes is how many bytes the records waiting may take. Past it
	// the oldest waiting record is dropped. Zero, 16 MiB.
	MaxWaitingBytes int
}

// BufferConfig is the options of the core buffer for o.
func BufferConfig(o BufferOptions) buffer.Options { return o.internal() }

// CountsOf is the counts of a core buffer as a sink gives them.
func CountsOf(c buffer.Counts) SinkCounts {
	return SinkCounts{Dropped: c.Dropped, TimedOut: c.TimedOut, Waiting: c.Waiting, WaitingBytes: c.WaitingBytes}
}

func (o BufferOptions) internal() buffer.Options {
	return buffer.Options{
		BatchSize:       o.BatchSize,
		FlushInterval:   o.FlushInterval,
		Timeout:         o.Timeout,
		MaxWaiting:      o.MaxWaiting,
		MaxBatchSize:    o.MaxBatchSize,
		MaxWaitingBytes: o.MaxWaitingBytes,
	}
}

// SinkCounts is what a sink lost and holds, over every env.
type SinkCounts struct {
	// Dropped is the records dropped since the sink was made: past a limit,
	// in a send that was refused, or left at close.
	Dropped int64
	// TimedOut is the sends given up at their timeout since the sink was made.
	TimedOut int64
	// Waiting is the records waiting to be sent now.
	Waiting int
	// WaitingBytes is the size of the records waiting now, in bytes.
	WaitingBytes int
}

// RecordSink buffers records and sends them to Flowrensic in batches.
type RecordSink[T any] interface {
	// Record never waits on Flowrensic.
	Record(one T)
	// Flush sends what waits and the counts, and waits for them to land.
	Flush(ctx context.Context) error
	// Close sends what waits and stops; what still waits is dropped.
	Close(ctx context.Context) error
	// Counts is what the sink lost and holds.
	Counts() SinkCounts
}

// AttemptSink is the sink the activity interceptor records attempts to.
type AttemptSink = RecordSink[*ActivityAttempt]

// TimeSpender is a sink that counts the time of the SDK's own code under its
// env.
type TimeSpender interface {
	Spent(d time.Duration)
}

// SinkOptions is where an attempt sink sends and how.
type SinkOptions struct {
	// ServerBaseURL is the Flowrensic server to send to.
	ServerBaseURL string
	// CurrentEnv is the env the attempts are for, sent with its key and
	// ephemeral run in the headers of every send.
	CurrentEnv string
	// EnvAccessKey is the access key of that env.
	EnvAccessKey string
	// CurrentEphemeralRun is the ephemeral run of that env. Empty, none.
	CurrentEphemeralRun string
	// WorkerID is which worker this is. Empty, the host name and the process id.
	WorkerID string
	// WorkerName is the name of this worker, the same for every copy of it,
	// such as orders-worker. Empty, none.
	WorkerName string
	// Buffer is how the attempts wait and leave.
	Buffer BufferOptions
	// HTTPClient sends the calls. Nil, http.DefaultClient.
	HTTPClient *http.Client
}

type sink[T any] struct {
	b *buffer.Buffer[T]
}

func (s *sink[T]) Record(one T) { s.b.Record(one) }

func (s *sink[T]) Flush(ctx context.Context) error { return s.b.Flush(ctx) }

func (s *sink[T]) Close(ctx context.Context) error { return s.b.Close(ctx) }

func (s *sink[T]) Spent(d time.Duration) { s.b.Spent(d) }

func (s *sink[T]) Counts() SinkCounts {
	return CountsOf(s.b.Counts())
}

// NewAttemptSink makes the sink the activity interceptor records to. It sends
// the attempts with IngestActivityEvents, and the counts of what it dropped,
// gave up and holds with the time of the SDK's own code in reports. tag names
// the SDK and its engine on every send.
func NewAttemptSink(options SinkOptions, tag transport.Tag) AttemptSink {
	workerID := options.WorkerID
	if workerID == "" {
		workerID = DefaultWorkerID()
	}
	clients := transport.New(transport.Context{
		ServerBaseURL:       options.ServerBaseURL,
		CurrentEnv:          options.CurrentEnv,
		EnvAccessKey:        options.EnvAccessKey,
		CurrentEphemeralRun: options.CurrentEphemeralRun,
		WorkerID:            workerID,
		HTTPClient:          options.HTTPClient,
		Tag:                 tag,
	})
	return &sink[*ActivityAttempt]{b: buffer.New(buffer.Target[*ActivityAttempt]{
		Send: func(ctx context.Context, events []*ActivityAttempt, id buffer.SendID) error {
			_, err := clients.Ingest.IngestActivityEvents(ctx, connect.NewRequest(&flowrensicv1.IngestActivityEventsRequest{
				Events: events, WorkerId: workerID, WorkerName: options.WorkerName, Sender: id.Sender, SendNumber: id.SendNumber,
			}))
			return err
		},
		EnvOf:  func(*ActivityAttempt) string { return options.CurrentEnv },
		SizeOf: func(a *ActivityAttempt) int { return buffer.ApproxSize(a) },
		Report: func(ctx context.Context, reports []*flowrensicv1.WorkerBufferReport, id buffer.SendID) error {
			_, err := clients.Ingest.IngestActivityEvents(ctx, connect.NewRequest(&flowrensicv1.IngestActivityEventsRequest{
				WorkerId: workerID, Reports: reports, Sender: id.Sender, SendNumber: id.SendNumber,
			}))
			return err
		},
		Env: options.CurrentEnv,
	}, options.Buffer.internal())}
}

// DefaultWorkerID is the worker id the SDK sends when none is given: the host
// name and the process id, joined by a dash.
func DefaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
