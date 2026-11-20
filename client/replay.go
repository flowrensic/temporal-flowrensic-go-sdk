package client

import (
	"context"

	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/ci"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/engine"
)

// IndexName is the manifest written beside the histories, the same one the
// CLI and the other SDKs write.
const IndexName = ci.IndexName

// ReplaySetEntry is one history on disk, with what the server chose it by.
type ReplaySetEntry = ci.ReplaySetEntry

// LeftOutEntry is a stored run the server could not read, so it is not in
// the set.
type LeftOutEntry = ci.LeftOutEntry

// ReplaySet is the manifest of a download.
type ReplaySet = ci.ReplaySet

// DefaultDownloadTimeout is how long a replay set download may take when its
// options carry no Timeout.
const DefaultDownloadTimeout = ci.DefaultDownloadTimeout

// DefaultRetryUnavailableFor is how long a call of a replay set download that
// Flowrensic answers Service Unavailable is tried again when its options carry
// no RetryUnavailableFor.
const DefaultRetryUnavailableFor = ci.DefaultRetryUnavailableFor

// FetchOptions is a replay set download.
type FetchOptions = ci.FetchOptions

// FetchReplaySet fetches the set the server picks for a workflow type in each
// env named, and writes each history, as the engine wrote it, and the
// manifest to the folder. Which histories are worth replaying is the server's
// call. Nothing here runs a workflow: the CI job replays each file with
// Temporal's worker.WorkflowReplayer, and PublishVerdicts takes the outcome
// back. An empty set is not an error.
func FetchReplaySet(ctx context.Context, options FetchOptions) (ReplaySet, error) {
	return ci.FetchReplaySet(ctx, options, engine.Tag())
}

// ReadReplaySet reads a manifest a download left behind, the CLI's or this
// package's.
func ReadReplaySet(dir string) (ReplaySet, error) { return ci.ReadReplaySet(dir) }

// ListHistoryFiles is the histories in a folder, by name, without the
// manifest.
func ListHistoryFiles(dir string) ([]string, error) { return ci.ListHistoryFiles(dir) }

// ReplayVerdict is what one history did when replayed against the code under
// test.
type ReplayVerdict = ci.ReplayVerdict

// The defaults of PublishOptions.
const (
	DefaultPublishTimeout = ci.DefaultPublishTimeout
	DefaultSendAgainFor   = ci.DefaultSendAgainFor
)

// PublishOptions is the results of a replay to send.
type PublishOptions = ci.PublishOptions

// PublishVerdicts reports what a replay found, against the history and the
// version replayed, so a break is put down to a code change rather than to a
// failing test.
func PublishVerdicts(ctx context.Context, options PublishOptions) error {
	return ci.PublishVerdicts(ctx, options, engine.Tag())
}
