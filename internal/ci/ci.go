// Package ci holds the calls of a CI job to Flowrensic: the deploy report,
// the replay set download and the replay verdicts.
package ci

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"connectrpc.com/connect"

	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/transport"
)

// CIError is the error a CI call returns: its text says that Flowrensic could
// not be reached in time, or what it answered, and it wraps the error of the
// call, so connect.CodeOf reads the code of an answer.
type CIError struct {
	message string
	err     error
}

func (e *CIError) Error() string { return e.message }

// Unwrap is the error of the call.
func (e *CIError) Unwrap() error { return e.err }

// notReached is whether a call failed because nothing answered in time or no
// connection could be made.
func notReached(err error) bool {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return true
	}
	switch ce.Code() {
	case connect.CodeDeadlineExceeded, connect.CodeCanceled:
		return true
	}
	return !connect.IsWireError(ce)
}

// ciFailure is the message a CI call stops with: that Flowrensic could not be
// reached within the time limit, or what it answered, the code in brackets as
// Connect writes it and the message. what names the call, such as deploy
// report.
func ciFailure(what, serverBaseURL string, timeout time.Duration, err error) string {
	seconds := strconv.FormatFloat(timeout.Seconds(), 'f', -1, 64)
	var ce *connect.Error
	if notReached(err) {
		cause := err.Error()
		if errors.As(err, &ce) {
			cause = ce.Message()
		}
		return fmt.Sprintf("%s: Flowrensic at %s could not be reached within %s seconds: %s", what, serverBaseURL, seconds, cause)
	}
	errors.As(err, &ce)
	return fmt.Sprintf("%s: Flowrensic at %s answered [%s] %s", what, serverBaseURL, ce.Code(), ce.Message())
}

func ciError(what, serverBaseURL string, timeout time.Duration, err error) error {
	return &CIError{message: ciFailure(what, serverBaseURL, timeout, err), err: err}
}

// ciClients are the clients of a CI call: the env, its key and the ephemeral
// run in the headers.
func ciClients(serverBaseURL, env, key, run string, httpClient *http.Client, tag transport.Tag) transport.Clients {
	return transport.New(transport.Context{
		ServerBaseURL:       serverBaseURL,
		CurrentEnv:          env,
		EnvAccessKey:        key,
		CurrentEphemeralRun: run,
		HTTPClient:          httpClient,
		Tag:                 tag,
	})
}

// DefaultDeployReportTimeout is how long a deploy report may take when its
// options carry no Timeout.
const DefaultDeployReportTimeout = 10 * time.Second

// DeployStageOptions is one stage of a deploy to report.
type DeployStageOptions struct {
	// ServerBaseURL is the Flowrensic server to send to.
	ServerBaseURL string
	// CurrentEnv is the env of the deploy, sent in the header
	// Flowrensic-Current-Env.
	CurrentEnv string
	// EnvAccessKey is the access key of the env.
	EnvAccessKey string
	// CurrentEphemeralRun is the ephemeral run of the env, such as the CI
	// build number. Empty, none.
	CurrentEphemeralRun string
	DeployID            string
	// Stage is initiated, started, replay, testsStarted, testsDone or finished.
	Stage             string
	DeclaredStages    []string
	RollingOutVersion string
	Outcome           string
	// Timeout is how long the report may take. Zero,
	// DefaultDeployReportTimeout.
	Timeout time.Duration
	// FailOnError returns the error of a report that failed, so the CI job
	// fails. False, the result says sent false and the message.
	FailOnError bool
	// HTTPClient sends the call. Nil, http.DefaultClient.
	HTTPClient *http.Client
}

// DeployStageResult is what a deploy report did.
type DeployStageResult struct {
	// Sent is whether Flowrensic took the write.
	Sent bool
	// Message is what happened, in one line, for the CI log.
	Message string
}

// ReportDeployStage sends one stage of a deploy to Flowrensic, with a time
// limit, and never hangs. tag names the SDK on the call.
func ReportDeployStage(ctx context.Context, options DeployStageOptions, tag transport.Tag) (DeployStageResult, error) {
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = DefaultDeployReportTimeout
	}
	clients := ciClients(options.ServerBaseURL, options.CurrentEnv, options.EnvAccessKey, options.CurrentEphemeralRun, options.HTTPClient, tag)
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := clients.Ingest.IngestDeployStage(callCtx, connect.NewRequest(&flowrensicv1.IngestDeployStageRequest{
		DeployId:          options.DeployID,
		Stage:             options.Stage,
		DeclaredStages:    options.DeclaredStages,
		RollingOutVersion: options.RollingOutVersion,
		Outcome:           options.Outcome,
	}))
	if err != nil {
		message := ciFailure("deploy report", options.ServerBaseURL, timeout, err)
		if options.FailOnError {
			return DeployStageResult{Message: message}, &CIError{message: message, err: err}
		}
		return DeployStageResult{Message: message}, nil
	}
	return DeployStageResult{
		Sent:    true,
		Message: fmt.Sprintf("deploy report: Flowrensic at %s took the stage %s of %s", options.ServerBaseURL, options.Stage, options.DeployID),
	}, nil
}
