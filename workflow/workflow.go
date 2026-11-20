// Package workflow holds the Flowrensic workflow interceptor for a Temporal Go
// worker, beside go.temporal.io/sdk/workflow: it counts the queries the
// workflows answer.
package workflow

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.flowrensic.com/temporal-flowrensic-go-sdk/client"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/buffer"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/engine"
	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/transport"
)

// QueryOptions is where the query interceptor sends and how.
type QueryOptions struct {
	// ServerBaseURL is the Flowrensic server to send to.
	ServerBaseURL string
	// CurrentEnv is the env the worker runs in, sent with its key and
	// ephemeral run in the headers of every send.
	CurrentEnv string
	// EnvAccessKey is the access key of that env.
	EnvAccessKey string
	// CurrentEphemeralRun is the ephemeral run of that env. Empty, none.
	CurrentEphemeralRun string
	// Buffer is how the counts wait and leave.
	Buffer client.BufferOptions
	// HTTPClient sends the calls. Nil, http.DefaultClient.
	HTTPClient *http.Client
}

// answered is one query a workflow answered, ok when its handler returned no
// error.
type answered struct {
	namespace, workflowType, name string
	ok                            bool
	at                            time.Time
}

// QueryInterceptor counts every query a workflow of the worker answers, per
// workflow type, query name and minute, and sends the counts in batches. It
// only reads: the answer and the error of the query are the handler's own, and
// it adds no command, so a workflow runs and replays the same. Put it in
// worker.Options.Interceptors, and call Close before the process ends.
type QueryInterceptor struct {
	interceptor.WorkerInterceptorBase
	buffer *buffer.Buffer[answered]
}

// NewQueryInterceptor makes the query interceptor.
func NewQueryInterceptor(options QueryOptions) *QueryInterceptor {
	clients := transport.New(transport.Context{
		ServerBaseURL:       options.ServerBaseURL,
		CurrentEnv:          options.CurrentEnv,
		EnvAccessKey:        options.EnvAccessKey,
		CurrentEphemeralRun: options.CurrentEphemeralRun,
		HTTPClient:          options.HTTPClient,
		Tag:                 engine.Tag(),
	})
	b := buffer.New(buffer.Target[answered]{
		Send: func(ctx context.Context, records []answered, id buffer.SendID) error {
			_, err := clients.Ingest.IngestQueryCounts(ctx, connect.NewRequest(&flowrensicv1.IngestQueryCountsRequest{
				Counts: countsOf(records), Sender: id.Sender, SendNumber: id.SendNumber,
			}))
			return err
		},
		EnvOf:  func(answered) string { return options.CurrentEnv },
		SizeOf: func(a answered) int { return 32 + 2*(len(a.namespace)+len(a.workflowType)+len(a.name)) + 16 },
		Env:    options.CurrentEnv,
	}, buffer.Options{
		BatchSize:       options.Buffer.BatchSize,
		FlushInterval:   options.Buffer.FlushInterval,
		Timeout:         options.Buffer.Timeout,
		MaxWaiting:      options.Buffer.MaxWaiting,
		MaxBatchSize:    options.Buffer.MaxBatchSize,
		MaxWaitingBytes: options.Buffer.MaxWaitingBytes,
	})
	return &QueryInterceptor{buffer: b}
}

// countsOf is one QueryCount per namespace, workflow type, query name and
// minute of the records, in the order each first came.
func countsOf(records []answered) []*flowrensicv1.QueryCount {
	byKey := map[string]*flowrensicv1.QueryCount{}
	var out []*flowrensicv1.QueryCount
	for _, one := range records {
		minute := one.at.UTC().Truncate(time.Minute)
		key := fmt.Sprintf("%s\x00%s\x00%s\x00%d", one.namespace, one.workflowType, one.name, minute.Unix())
		count := byKey[key]
		if count == nil {
			count = &flowrensicv1.QueryCount{
				Namespace:    one.namespace,
				WorkflowType: one.workflowType,
				QueryName:    one.name,
				Minute:       timestamppb.New(minute),
			}
			byKey[key] = count
			out = append(out, count)
		}
		if one.ok {
			count.Answered++
		} else {
			count.Failed++
		}
	}
	return out
}

// InterceptWorkflow puts the inbound interceptor in front of next.
func (q *QueryInterceptor) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	in := &workflowInbound{q: q}
	in.Next = next
	return in
}

// Flush sends what waits and waits for it to land.
func (q *QueryInterceptor) Flush(ctx context.Context) error { return q.buffer.Flush(ctx) }

// Close sends what waits and stops.
func (q *QueryInterceptor) Close(ctx context.Context) error { return q.buffer.Close(ctx) }

// Counts is what the buffer lost and holds.
func (q *QueryInterceptor) Counts() client.SinkCounts {
	n := q.buffer.Counts()
	return client.SinkCounts{Dropped: n.Dropped, TimedOut: n.TimedOut, Waiting: n.Waiting, WaitingBytes: n.WaitingBytes}
}

type workflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	q *QueryInterceptor
}

// HandleQuery runs the handler and notes the query, ok when the handler
// returned no error. Temporal's own queries, such as __stack_trace, do not
// come here.
func (w *workflowInbound) HandleQuery(ctx workflow.Context, in *interceptor.HandleQueryInput) (any, error) {
	result, err := w.Next.HandleQuery(ctx, in)
	w.tell(ctx, in.QueryType, err == nil)
	return result, err
}

// tell notes one query, and drops a panic of its own.
func (w *workflowInbound) tell(ctx workflow.Context, name string, ok bool) {
	began := time.Now()
	defer func() {
		_ = recover()
		w.q.buffer.Spent(time.Since(began))
	}()
	info := workflow.GetInfo(ctx)
	w.q.buffer.Record(answered{namespace: info.Namespace, workflowType: info.WorkflowType.Name, name: name, ok: ok, at: time.Now()})
}
