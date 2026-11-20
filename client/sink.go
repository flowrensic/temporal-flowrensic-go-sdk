package client

import (
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/engine"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/worker"
)

// ActivityAttempt is one attempt of an activity, as the activity interceptor
// records it.
type ActivityAttempt = worker.ActivityAttempt

// Outcome is how an attempt ended.
type Outcome = worker.Outcome

// The outcomes of an attempt.
const (
	OutcomeCompleted = worker.OutcomeCompleted
	OutcomeFailed    = worker.OutcomeFailed
	OutcomeCanceled  = worker.OutcomeCanceled
)

// BufferOptions is how records wait and leave, the same for every sink. A
// zero field takes its default.
type BufferOptions = worker.BufferOptions

// SinkCounts is what a sink lost and holds, over every env.
type SinkCounts = worker.SinkCounts

// RecordSink buffers records and sends them to Flowrensic in batches.
type RecordSink[T any] = worker.RecordSink[T]

// AttemptSink is the sink the activity interceptor records attempts to.
type AttemptSink = worker.AttemptSink

// TimeSpender is a sink that counts the time of the SDK's own code under its
// env.
type TimeSpender = worker.TimeSpender

// SinkOptions is where an attempt sink sends and how.
type SinkOptions = worker.SinkOptions

// NewAttemptSink makes the sink the activity interceptor records to. It sends
// the attempts with IngestActivityEvents, and the counts of what it dropped,
// gave up and holds with the time of the SDK's own code in reports.
func NewAttemptSink(options SinkOptions) AttemptSink {
	return worker.NewAttemptSink(options, engine.Tag())
}

// DefaultWorkerID is the worker id the SDK sends when none is given: the host
// name and the process id, joined by a dash.
func DefaultWorkerID() string { return worker.DefaultWorkerID() }
