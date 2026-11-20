package ci

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/transport"
	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/worker"
)

// IndexName is the manifest written beside the histories, the same one the
// CLI and the other SDKs write.
const IndexName = "replay-set.json"

// ReplaySetEntry is one history on disk, with what the server chose it by.
type ReplaySetEntry struct {
	RunID  string `json:"run_id"`
	File   string `json:"file"`
	Status string `json:"status"`
	// Env is the env the history comes from.
	Env string `json:"env,omitempty"`
	// CodeVersions is the build ids the history carries.
	CodeVersions []string `json:"code_versions"`
	Events       int32    `json:"events"`
	StartedAt    string   `json:"started_at"`
	ClosedAt     string   `json:"closed_at,omitempty"`
}

// LeftOutEntry is a stored run the server could not read, so it is not in
// the set.
type LeftOutEntry struct {
	RunID  string `json:"run_id"`
	Reason string `json:"reason"`
}

// ReplaySet is the manifest of a download.
type ReplaySet struct {
	WorkflowType string `json:"workflow_type"`
	// Envs is the envs the histories come from, in the order they were asked
	// for.
	Envs      []string         `json:"envs"`
	FetchedAt string           `json:"fetched_at"`
	Server    string           `json:"server"`
	Histories []ReplaySetEntry `json:"histories"`
	// LeftOut is empty when every stored history of the type and envs could
	// be read.
	LeftOut []LeftOutEntry `json:"left_out"`
	// Failed is why the download failed, when FailOnError was false and it
	// did. The set is then empty.
	Failed string `json:"failed,omitempty"`
}

// DefaultDownloadTimeout is how long a replay set download may take when its
// options carry no Timeout.
const DefaultDownloadTimeout = 60 * time.Second

// DefaultRetryUnavailableFor is how long a call of a replay set download that
// Flowrensic answers Service Unavailable is tried again when its options carry
// no RetryUnavailableFor.
const DefaultRetryUnavailableFor = 2 * time.Minute

// The first wait before a call answered Service Unavailable is tried again,
// and the longest.
const (
	firstRetryWait   = time.Second
	longestRetryWait = 10 * time.Second
)

// retryUnavailable runs call, and while Flowrensic answers it Service
// Unavailable, runs it again, after a wait that starts at a second and doubles
// up to ten, until retryFor has passed since the first such answer. Each wait
// writes one line to log. Any other error, or the last one, is returned.
func retryUnavailable[T any](ctx context.Context, log io.Writer, serverBaseURL string, retryFor time.Duration, call func(context.Context) (T, error)) (T, error) {
	var giveUpAt time.Time
	wait := firstRetryWait
	for {
		got, err := call(ctx)
		if err == nil || connect.CodeOf(err) != connect.CodeUnavailable {
			return got, err
		}
		if giveUpAt.IsZero() {
			giveUpAt = time.Now().Add(retryFor)
		}
		left := time.Until(giveUpAt)
		if left <= 0 {
			return got, err
		}
		now := min(wait, left)
		_, _ = fmt.Fprintf(log, "replay set download: Flowrensic at %s is unavailable, trying again in %d ms\n", serverBaseURL, now.Round(time.Millisecond).Milliseconds())
		select {
		case <-ctx.Done():
			return got, err
		case <-time.After(now):
		}
		wait = min(wait*2, longestRetryWait)
	}
}

// FetchOptions is a replay set download.
type FetchOptions struct {
	// ServerBaseURL is the Flowrensic server, such as http://flowrensic:8080.
	ServerBaseURL string
	// ReplayHistoryWorkflow is the workflow type to replay.
	ReplayHistoryWorkflow string
	// ReplayHistoryOutDir is the folder to write the histories and the
	// manifest into.
	ReplayHistoryOutDir string
	// ReplayHistoryFromEnvs is the envs to replay from, such as prod and
	// staging. Each one must be the current env or an env it may read.
	ReplayHistoryFromEnvs []string
	// CurrentEnv is the env of the caller.
	CurrentEnv string
	// EnvAccessKey is the access key of the env of the caller.
	EnvAccessKey string
	// CurrentEphemeralRun is the ephemeral run of the caller, such as the CI
	// build number. Empty, none.
	CurrentEphemeralRun string
	// Timeout is how long each call of the download may take. Zero,
	// DefaultDownloadTimeout.
	Timeout time.Duration
	// RetryUnavailableFor is how long a call that Flowrensic answers Service
	// Unavailable is tried again, counted from the first such answer. Zero,
	// DefaultRetryUnavailableFor. Negative, no call is tried again.
	RetryUnavailableFor time.Duration
	// KeepGoingOnError, when true, returns an empty set whose Failed says why
	// in place of an error, and writes no manifest. False, the default, a
	// download that failed returns the error, so the CI job fails: the
	// failOnError true of the other SDKs.
	KeepGoingOnError bool
	// Log is where the lines of the download go. Nil, os.Stderr.
	Log io.Writer
	// HTTPClient sends the calls. Nil, http.DefaultClient.
	HTTPClient *http.Client
}

// statusLabels are the labels a manifest writes for each run status, the same
// as the CLI.
var statusLabels = map[flowrensicv1.RunStatus]string{
	flowrensicv1.RunStatus_RUN_STATUS_UNSPECIFIED:      "unspecified",
	flowrensicv1.RunStatus_RUN_STATUS_RUNNING:          "running",
	flowrensicv1.RunStatus_RUN_STATUS_COMPLETED:        "completed",
	flowrensicv1.RunStatus_RUN_STATUS_FAILED:           "failed",
	flowrensicv1.RunStatus_RUN_STATUS_CANCELED:         "canceled",
	flowrensicv1.RunStatus_RUN_STATUS_TERMINATED:       "terminated",
	flowrensicv1.RunStatus_RUN_STATUS_TIMED_OUT:        "timed_out",
	flowrensicv1.RunStatus_RUN_STATUS_CONTINUED_AS_NEW: "continued_as_new",
	flowrensicv1.RunStatus_RUN_STATUS_PAUSED:           "paused",
}

// isoTime is a time as JavaScript writes it: UTC with milliseconds.
func isoTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// fetchedLog is what a download writes, the same lines in every SDK and the
// CLI: the workflow type and the envs, how many histories the answer held and
// how many were saved, the run id of each, and each run left out with its
// reason.
func fetchedLog(set ReplaySet, got int, dir string) string {
	lines := []string{
		fmt.Sprintf("replay set: workflow %s, envs %s", set.WorkflowType, strings.Join(set.Envs, ", ")),
		fmt.Sprintf("got %d histories, saved %d in %s", got, len(set.Histories), dir),
	}
	for _, one := range set.Histories {
		lines = append(lines, "  "+one.RunID)
	}
	for _, one := range set.LeftOut {
		lines = append(lines, fmt.Sprintf("left out %s: %s", one.RunID, one.Reason))
	}
	return strings.Join(lines, "\n") + "\n"
}

// FetchReplaySet fetches the set the server picks for a workflow type in each
// env named, and writes each history, as the engine wrote it, and the
// manifest to the folder. Which histories are worth replaying is the server's
// call. Nothing here runs a workflow: the CI job replays each file with the
// replayer of its engine, and PublishVerdicts takes the outcome back. An empty set is not an error.
func FetchReplaySet(ctx context.Context, options FetchOptions, tag transport.Tag) (ReplaySet, error) {
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = DefaultDownloadTimeout
	}
	retryFor := options.RetryUnavailableFor
	if retryFor == 0 {
		retryFor = DefaultRetryUnavailableFor
	}
	retryFor = max(retryFor, 0)
	log := options.Log
	if log == nil {
		log = os.Stderr
	}
	envs := append([]string{}, options.ReplayHistoryFromEnvs...)
	clients := ciClients(options.ServerBaseURL, options.CurrentEnv, options.EnvAccessKey, options.CurrentEphemeralRun, options.HTTPClient, tag)
	var histories []*flowrensicv1.ReplayHistory
	var leftOut []*flowrensicv1.LeftOutHistory
	err := func() error {
		token := ""
		for {
			req := &flowrensicv1.ListReplayHistoriesRequest{
				WorkflowType: options.ReplayHistoryWorkflow, Envs: envs, Page: &flowrensicv1.PageRequest{PageToken: token},
			}
			res, err := retryUnavailable(ctx, log, options.ServerBaseURL, retryFor, func(ctx context.Context) (*connect.Response[flowrensicv1.ListReplayHistoriesResponse], error) {
				callCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				return clients.Query.ListReplayHistories(callCtx, connect.NewRequest(req))
			})
			if err != nil {
				return err
			}
			histories = append(histories, res.Msg.GetHistories()...)
			leftOut = append(leftOut, res.Msg.GetLeftOut()...)
			token = res.Msg.GetPage().GetNextPageToken()
			if token == "" {
				return nil
			}
		}
	}()
	set := ReplaySet{
		WorkflowType: options.ReplayHistoryWorkflow,
		Envs:         envs,
		Server:       options.ServerBaseURL,
		Histories:    []ReplaySetEntry{},
		LeftOut:      []LeftOutEntry{},
	}
	if err != nil {
		set.FetchedAt = isoTime(time.Now())
		if !options.KeepGoingOnError {
			return set, ciError("replay set download", options.ServerBaseURL, timeout, err)
		}
		set.Failed = ciFailure("replay set download", options.ServerBaseURL, timeout, err)
		return set, nil
	}
	dir := options.ReplayHistoryOutDir
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return set, err
	}
	for _, h := range histories {
		file := h.GetRunId() + ".json"
		// Written and renamed, so a failed write never leaves half a history
		// for a replayer to read as a non-determinism report.
		temp := filepath.Join(dir, ".fetch-"+h.GetRunId())
		if err := os.WriteFile(temp, h.GetHistory(), 0o600); err != nil {
			return set, err
		}
		if err := os.Rename(temp, filepath.Join(dir, file)); err != nil {
			return set, err
		}
		entry := ReplaySetEntry{
			RunID:        h.GetRunId(),
			File:         file,
			Status:       statusLabels[h.GetStatus()],
			Env:          h.GetEnv(),
			CodeVersions: append([]string{}, h.GetCodeVersions()...),
			Events:       h.GetEvents(),
			StartedAt:    isoTime(time.Unix(0, 0)),
		}
		if h.GetStartedAt() != nil {
			entry.StartedAt = isoTime(h.GetStartedAt().AsTime())
		}
		if h.GetClosedAt() != nil {
			entry.ClosedAt = isoTime(h.GetClosedAt().AsTime())
		}
		set.Histories = append(set.Histories, entry)
	}
	for _, one := range leftOut {
		set.LeftOut = append(set.LeftOut, LeftOutEntry{RunID: one.GetRunId(), Reason: one.GetReason()})
	}
	set.FetchedAt = isoTime(time.Now())
	var text bytes.Buffer
	enc := json.NewEncoder(&text)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(set); err != nil {
		return set, err
	}
	if err := os.WriteFile(filepath.Join(dir, IndexName), text.Bytes(), 0o600); err != nil {
		return set, err
	}
	_, _ = io.WriteString(log, fetchedLog(set, len(histories), dir))
	return set, nil
}

// ReadReplaySet reads a manifest a download left behind, the CLI's or this
// package's.
func ReadReplaySet(dir string) (ReplaySet, error) {
	path := filepath.Join(dir, IndexName)
	text, err := os.ReadFile(path)
	if err != nil {
		return ReplaySet{}, err
	}
	var set ReplaySet
	if err := json.Unmarshal(text, &set); err != nil || set.WorkflowType == "" || set.Histories == nil {
		return ReplaySet{}, fmt.Errorf("%s is not a replay set manifest", path)
	}
	return set, nil
}

// ListHistoryFiles is the histories in a folder, by name, without the
// manifest.
func ListHistoryFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, one := range entries {
		name := one.Name()
		if !one.IsDir() && strings.HasSuffix(name, ".json") && name != IndexName && !strings.HasPrefix(name, ".") {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// ReplayVerdict is what one history did when replayed against the code under
// test.
type ReplayVerdict struct {
	RunID        string
	WorkflowType string
	Passed       bool
	// Error is the replayer's own message, kept as it is: it names what
	// diverged.
	Error string
}

// The defaults of PublishOptions.
const (
	DefaultPublishTimeout = 30 * time.Second
	DefaultSendAgainFor   = 10 * time.Minute
)

// PublishOptions is the results of a replay to send.
type PublishOptions struct {
	ServerBaseURL string
	// CodeVersion is the commit, tag or build replayed.
	CodeVersion string
	Verdicts    []ReplayVerdict
	// CurrentEnv is the env the verdicts are sent for.
	CurrentEnv string
	// EnvAccessKey is the access key of that env.
	EnvAccessKey string
	// CurrentEphemeralRun is the ephemeral run of that env. Empty, none.
	CurrentEphemeralRun string
	// WorkflowInterceptors is the names of the workflow interceptors the
	// replay ran with. Every result is sent with them, to compare with the
	// list the workers of the env send. Nil sends no list, which is not an
	// empty list.
	WorkflowInterceptors []string
	// SendAgainFor is how long results Flowrensic does not answer, or answers
	// try again later, are sent again before PublishVerdicts gives up with
	// the last error. Zero, DefaultSendAgainFor.
	SendAgainFor time.Duration
	// Timeout is how long one call may take. Zero, DefaultPublishTimeout.
	Timeout time.Duration
	// HTTPClient sends the calls. Nil, http.DefaultClient.
	HTTPClient *http.Client
}

// waitBeforeSendingAgain is the time to wait before sending again a call that
// Flowrensic did not answer or answered try again later: its Retry-After, or
// a second. False for any other error, which is not sent again.
func waitBeforeSendingAgain(err error) (time.Duration, bool) {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return 0, false
	}
	switch ce.Code() {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeResourceExhausted:
	default:
		return 0, false
	}
	if seconds, err := strconv.ParseFloat(ce.Meta().Get("Retry-After"), 64); err == nil && seconds > 0 {
		return time.Duration(seconds * float64(time.Second)), true
	}
	return time.Second, true
}

// PublishVerdicts reports what a replay found, against the history and the
// version replayed, so a break is put down to a code change rather than to a
// failing test.
func PublishVerdicts(ctx context.Context, options PublishOptions, tag transport.Tag) error {
	if len(options.Verdicts) == 0 {
		return nil
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = DefaultPublishTimeout
	}
	sendAgainFor := options.SendAgainFor
	if sendAgainFor <= 0 {
		sendAgainFor = DefaultSendAgainFor
	}
	clients := ciClients(options.ServerBaseURL, options.CurrentEnv, options.EnvAccessKey, options.CurrentEphemeralRun, options.HTTPClient, tag)
	replayedAt := timestamppb.Now()
	interceptors := worker.InterceptorList(options.WorkflowInterceptors)
	results := make([]*flowrensicv1.ReplayResult, 0, len(options.Verdicts))
	for _, v := range options.Verdicts {
		results = append(results, &flowrensicv1.ReplayResult{
			RunId:                v.RunID,
			CodeVersion:          options.CodeVersion,
			WorkflowType:         v.WorkflowType,
			Passed:               v.Passed,
			Error:                v.Error,
			ReplayedAt:           replayedAt,
			WorkflowInterceptors: interceptors,
		})
	}
	giveUpAt := time.Now().Add(sendAgainFor)
	for {
		callCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err := clients.Ingest.IngestReplayResults(callCtx, connect.NewRequest(&flowrensicv1.IngestReplayResultsRequest{Results: results}))
		cancel()
		if err == nil {
			return nil
		}
		wait, again := waitBeforeSendingAgain(err)
		if !again || time.Now().Add(wait).After(giveUpAt) {
			return ciError("replay results", options.ServerBaseURL, timeout, err)
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ciError("replay results", options.ServerBaseURL, timeout, err)
		}
	}
}
