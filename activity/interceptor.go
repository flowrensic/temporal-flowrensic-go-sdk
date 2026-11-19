// Package activity holds the Flowrensic activity interceptor for a Temporal Go
// worker, beside go.temporal.io/sdk/activity.
package activity

import (
	"context"
	"errors"
	"reflect"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.flowrensic.com/temporal-flowrensic-go-sdk/client"
)

// Options is what the worker says of itself on each attempt. The env and the
// ephemeral run are not here: the sink sends them in the headers of its calls.
type Options struct {
	// Version is the version this worker serves. Empty, none, and a run with
	// no version is on neither side of an overlap.
	Version string
}

// NewInterceptor returns the Flowrensic interceptor, to put in
// worker.Options.Interceptors. It records one attempt per activity execution
// to sink, and passes every call through unchanged, inbound and outbound. It
// leaves workflow calls alone. The namespace of each attempt is the one the
// activity info carries.
func NewInterceptor(sink client.AttemptSink, options Options) interceptor.WorkerInterceptor {
	return &workerInterceptor{sink: sink, options: options}
}

type workerInterceptor struct {
	interceptor.WorkerInterceptorBase
	sink    client.AttemptSink
	options Options
}

// InterceptActivity puts the inbound interceptor in front of next.
func (w *workerInterceptor) InterceptActivity(
	_ context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	in := &activityInbound{w: w}
	in.Next = next
	return in
}

type activityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	w *workerInterceptor
}

// Init puts the outbound interceptor in front of outbound.
func (a *activityInbound) Init(outbound interceptor.ActivityOutboundInterceptor) error {
	out := &activityOutbound{}
	out.Next = outbound
	return a.Next.Init(out)
}

// ExecuteActivity runs the activity and records its attempt. Recording never
// changes what the activity returns, the error it fails with, or a panic.
func (a *activityInbound) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (result any, err error) {
	began := time.Now()
	info := activity.GetInfo(ctx)
	startedAt := time.Now().Truncate(time.Millisecond)
	before := time.Since(began)
	var after time.Time
	finish := func(outcome client.Outcome, errorType string) {
		a.emit(info, startedAt, outcome, errorType)
		a.spent(before + time.Since(after))
	}
	defer func() {
		if r := recover(); r != nil {
			after = time.Now()
			finish(client.OutcomeFailed, "PanicError")
			panic(r)
		}
	}()
	result, err = a.Next.ExecuteActivity(ctx, in)
	after = time.Now()
	if err != nil {
		finish(classify(ctx, err))
	} else {
		finish(client.OutcomeCompleted, "")
	}
	return result, err
}

// classify is the outcome and error type of a failed attempt: canceled with
// the type CancelledFailure, the name the Node SDK sends, for a canceled
// activity; failed with the type of an application error; and else failed
// with the type Temporal's Go SDK gives the failure, Error for a plain error
// that has none.
func classify(ctx context.Context, err error) (client.Outcome, string) {
	if temporal.IsCanceledError(err) || (errors.Is(err, context.Canceled) && ctx.Err() != nil) {
		return client.OutcomeCanceled, "CancelledFailure"
	}
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		if app.Type() != "" {
			return client.OutcomeFailed, app.Type()
		}
		return client.OutcomeFailed, "ApplicationError"
	}
	return client.OutcomeFailed, errType(err)
}

// errType is the name of the type of err without its pointers, Error for the
// type errors.New makes and for a type with no name.
func errType(err error) string {
	t := reflect.TypeOf(err)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeOf(errors.New("")).Elem() || t.Name() == "" {
		return "Error"
	}
	return t.Name()
}

// emit records the attempt, and drops a panic of the sink.
func (a *activityInbound) emit(info activity.Info, startedAt time.Time, outcome client.Outcome, errorType string) {
	defer func() { _ = recover() }()
	endedAt := time.Now().Truncate(time.Millisecond)
	attempt := &client.ActivityAttempt{
		Namespace:             info.Namespace,
		Version:               a.w.options.Version,
		WorkflowExecutionId:   info.WorkflowExecution.ID,
		RunKey:                info.WorkflowExecution.RunID,
		ActivityType:          info.ActivityType.Name,
		ActivityId:            info.ActivityID,
		Attempt:               info.Attempt,
		TaskQueue:             info.TaskQueue,
		Outcome:               outcome,
		ErrorType:             errorType,
		StartedAt:             timestamppb.New(startedAt),
		EndedAt:               timestamppb.New(endedAt),
		DurationMs:            endedAt.Sub(startedAt).Milliseconds(),
		StartToCloseTimeoutMs: info.StartToCloseTimeout.Milliseconds(),
		HeartbeatTimeoutMs:    info.HeartbeatTimeout.Milliseconds(),
		ScheduledAt:           timestamppb.New(info.ScheduledTime),
	}
	if info.WorkflowType != nil {
		attempt.WorkflowType = info.WorkflowType.Name
	}
	if info.RetryPolicy != nil {
		attempt.MaximumAttempts = info.RetryPolicy.MaximumAttempts
	}
	a.w.sink.Record(attempt)
}

// spent counts the time of the interceptor's own code under the env of the
// sink, when the sink counts time.
func (a *activityInbound) spent(d time.Duration) {
	defer func() { _ = recover() }()
	if t, ok := a.w.sink.(client.TimeSpender); ok {
		t.Spent(d)
	}
}

type activityOutbound struct {
	interceptor.ActivityOutboundInterceptorBase
}
