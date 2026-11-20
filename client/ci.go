package client

import (
	"context"

	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/ci"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/engine"
)

// CIError is the error a CI call returns: its text says that Flowrensic could
// not be reached in time, or what it answered, and it wraps the error of the
// call, so connect.CodeOf reads the code of an answer.
type CIError = ci.CIError

// DefaultDeployReportTimeout is how long a deploy report may take when its
// options carry no Timeout.
const DefaultDeployReportTimeout = ci.DefaultDeployReportTimeout

// DeployStageOptions is one stage of a deploy to report.
type DeployStageOptions = ci.DeployStageOptions

// DeployStageResult is what a deploy report did.
type DeployStageResult = ci.DeployStageResult

// ReportDeployStage sends one stage of a deploy to Flowrensic, with a time
// limit, and never hangs.
func ReportDeployStage(ctx context.Context, options DeployStageOptions) (DeployStageResult, error) {
	return ci.ReportDeployStage(ctx, options, engine.Tag())
}
