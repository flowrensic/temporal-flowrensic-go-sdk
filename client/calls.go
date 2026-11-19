package client

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"unicode"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/buffer"
	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/transport"
)

// ClientCall is one query or signal of a Temporal client that failed.
type ClientCall = flowrensicv1.ClientCall

// DefaultNamespace is the namespace a Temporal client talks to when its
// options name none.
const DefaultNamespace = "default"

// ClientInterceptorOptions is where the client interceptor sends and how.
type ClientInterceptorOptions struct {
	// ServerBaseURL is the Flowrensic server to send to.
	ServerBaseURL string
	// CurrentEnv is the env of the client, sent with its key and ephemeral run
	// in the headers of every send.
	CurrentEnv string
	// EnvAccessKey is the access key of that env.
	EnvAccessKey string
	// CurrentEphemeralRun is the ephemeral run of that env. Empty, none.
	CurrentEphemeralRun string
	// Namespace is the namespace of the Temporal client, the same as its
	// client.Options.Namespace. Empty, default.
	Namespace string
	// Buffer is how the calls wait and leave.
	Buffer BufferOptions
	// HTTPClient sends the calls. Nil, http.DefaultClient.
	HTTPClient *http.Client
}

// ClientInterceptor sends every query and signal of a Temporal client that
// fails to Flowrensic, with its name, the workflow it was for and whether it
// ended on its deadline. A call that works is not sent. The calls wait in a
// buffer and are sent in batches, so sending never changes what the caller
// gets or how long it waits. Put it in client.Options.Interceptors, and call
// Close before the process ends.
type ClientInterceptor struct {
	interceptor.ClientInterceptorBase
	buffer    *buffer.Buffer[*ClientCall]
	namespace string
}

// NewClientInterceptor makes the client interceptor.
func NewClientInterceptor(options ClientInterceptorOptions) *ClientInterceptor {
	clients := transport.New(transport.Context{
		ServerBaseURL:       options.ServerBaseURL,
		CurrentEnv:          options.CurrentEnv,
		EnvAccessKey:        options.EnvAccessKey,
		CurrentEphemeralRun: options.CurrentEphemeralRun,
		HTTPClient:          options.HTTPClient,
	})
	namespace := options.Namespace
	if namespace == "" {
		namespace = DefaultNamespace
	}
	return &ClientInterceptor{
		namespace: namespace,
		buffer: buffer.New(buffer.Target[*ClientCall]{
			Send: func(ctx context.Context, calls []*ClientCall, id buffer.SendID) error {
				_, err := clients.Ingest.IngestClientCalls(ctx, connect.NewRequest(&flowrensicv1.IngestClientCallsRequest{
					Calls: calls, Sender: id.Sender, SendNumber: id.SendNumber,
				}))
				return err
			},
			EnvOf:  func(*ClientCall) string { return options.CurrentEnv },
			SizeOf: func(c *ClientCall) int { return buffer.ApproxSize(c) },
		}, options.Buffer.internal()),
	}
}

// InterceptClient puts the interceptor in front of next.
func (c *ClientInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	out := &clientOutbound{c: c}
	out.Next = next
	return out
}

// Flush sends what waits and waits for it to land.
func (c *ClientInterceptor) Flush(ctx context.Context) error { return c.buffer.Flush(ctx) }

// Close sends what waits and stops.
func (c *ClientInterceptor) Close(ctx context.Context) error { return c.buffer.Close(ctx) }

// Counts is what the buffer lost and holds.
func (c *ClientInterceptor) Counts() SinkCounts {
	n := c.buffer.Counts()
	return SinkCounts{Dropped: n.Dropped, TimedOut: n.TimedOut, Waiting: n.Waiting, WaitingBytes: n.WaitingBytes}
}

type clientOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	c *ClientInterceptor
}

func (o *clientOutbound) QueryWorkflow(ctx context.Context, in *interceptor.ClientQueryWorkflowInput) (converter.EncodedValue, error) {
	res, err := o.Next.QueryWorkflow(ctx, in)
	if err != nil {
		o.c.record(ctx, "query", in.QueryType, in.WorkflowID, in.RunID, err)
	}
	return res, err
}

func (o *clientOutbound) SignalWorkflow(ctx context.Context, in *interceptor.ClientSignalWorkflowInput) error {
	err := o.Next.SignalWorkflow(ctx, in)
	if err != nil {
		o.c.record(ctx, "signal", in.SignalName, in.WorkflowID, in.RunID, err)
	}
	return err
}

// record buffers a call that failed, and drops a panic of its own, so the
// caller gets the error Temporal gave.
func (c *ClientInterceptor) record(ctx context.Context, kind, name, workflowID, runID string, err error) {
	defer func() { _ = recover() }()
	outcome := flowrensicv1.ClientCallOutcome_CLIENT_CALL_OUTCOME_FAILED
	if endedOnDeadline(ctx, err) {
		outcome = flowrensicv1.ClientCallOutcome_CLIENT_CALL_OUTCOME_TIMED_OUT
	}
	c.buffer.Record(&ClientCall{
		Namespace:  c.namespace,
		WorkflowId: workflowID,
		RunId:      runID,
		Kind:       kind,
		Name:       name,
		Outcome:    outcome,
		ErrorType:  callErrorType(err),
		EndedAt:    timestamppb.Now(),
	})
}

// grpcCode is the gRPC code err carries, and whether it carries one.
func grpcCode(err error) (codes.Code, bool) {
	var se serviceerror.ServiceError
	if errors.As(err, &se) {
		return se.Status().Code(), true
	}
	if s, ok := status.FromError(err); ok && s.Code() != codes.Unknown {
		return s.Code(), true
	}
	return codes.OK, false
}

// endedOnDeadline is whether the call ended on its deadline: DEADLINE_EXCEEDED,
// or CANCELLED while the caller's context was not done, which is a cancel of
// the server.
func endedOnDeadline(ctx context.Context, err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	code, ok := grpcCode(err)
	if !ok {
		return false
	}
	return code == codes.DeadlineExceeded || (code == codes.Canceled && ctx.Err() == nil)
}

// callErrorType is the gRPC code name, such as NOT_FOUND, when err carries a
// gRPC status, as the Java SDK sends it, and else the name of the Go type of
// err without the pointer.
func callErrorType(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "DEADLINE_EXCEEDED"
	}
	if code, ok := grpcCode(err); ok {
		return codeName(code)
	}
	t := reflect.TypeOf(err)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Name() == "" {
		return "Error"
	}
	return t.Name()
}

// codeName is the canonical name of a gRPC code, in capitals with underscores,
// such as INVALID_ARGUMENT.
func codeName(code codes.Code) string {
	switch code {
	case codes.OK:
		return "OK"
	case codes.Canceled:
		return "CANCELLED"
	}
	var out strings.Builder
	for i, r := range code.String() {
		if i > 0 && unicode.IsUpper(r) {
			out.WriteByte('_')
		}
		out.WriteRune(unicode.ToUpper(r))
	}
	return out.String()
}
