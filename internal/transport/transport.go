// Package transport makes the Connect clients of the Flowrensic API with the
// headers every call of the SDK carries.
package transport

import (
	"context"
	"net/http"
	"runtime/debug"
	"strings"

	"connectrpc.com/connect"

	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1/flowrensicv1connect"
)

// The headers of the calls.
const (
	CurrentEnvHeader          = "Flowrensic-Current-Env"
	EnvAccessKeyHeader        = "Flowrensic-Env-Access-Key"
	CurrentEphemeralRunHeader = "Flowrensic-Current-Ephemeral-Run"
	SdkHeader                 = "Flowrensic-Sdk"
	WorkerIDHeader            = "Flowrensic-Worker-Id"
)

// Tag is what every call says about the SDK that makes it: the SDK header
// value, such as go/1.2.3, and the engine's own header, such as the version of
// the engine's SDK. An empty EngineValue sends no engine header.
type Tag struct {
	Sdk          string
	EngineHeader string
	EngineValue  string
}

// SdkVersion is the version of the module path in the build info of the
// program, dev when the module has no version there or is replaced by a
// folder.
func SdkVersion(path string) string {
	v := ModuleVersion(path)
	if v == "" || v == "(devel)" || strings.HasPrefix(v, "0.0.0") {
		return "dev"
	}
	return v
}

// ModuleVersion is the version of the module path in the build info of the
// program, with no leading v: the version of its replacement when it is
// replaced, empty when the program has no build info, does not hold the
// module, or replaces it by a folder.
func ModuleVersion(path string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	mods := append([]*debug.Module{&info.Main}, info.Deps...)
	for _, m := range mods {
		if m == nil || m.Path != path {
			continue
		}
		if m.Replace != nil {
			m = m.Replace
		}
		return strings.TrimPrefix(m.Version, "v")
	}
	return ""
}

// Context is who the caller is. An empty field sends no header.
type Context struct {
	ServerBaseURL       string
	CurrentEnv          string
	EnvAccessKey        string
	CurrentEphemeralRun string
	// WorkerID is sent on every call of a worker.
	WorkerID   string
	HTTPClient *http.Client
	// Tag names the SDK and its engine on every call.
	Tag Tag
}

// Clients are the clients of the Flowrensic services the SDK calls.
type Clients struct {
	Ingest flowrensicv1connect.IngestServiceClient
	Query  flowrensicv1connect.QueryServiceClient
}

// New makes the clients, each call with the headers of c.
func New(c Context) Clients {
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	base := strings.TrimRight(c.ServerBaseURL, "/")
	httpClient = withRetryAfter(httpClient)
	opts := connect.WithInterceptors(headers(c), retryAfterOfAnswer())
	return Clients{
		Ingest: flowrensicv1connect.NewIngestServiceClient(httpClient, base, opts),
		Query:  flowrensicv1connect.NewQueryServiceClient(httpClient, base, opts),
	}
}

// headers puts the SDK, the engine tag when it has a value, and the env, its access
// key, the ephemeral run and the worker id of c in the headers of every call.
func headers(c Context) connect.UnaryInterceptorFunc {
	set := [][2]string{
		{CurrentEnvHeader, c.CurrentEnv},
		{EnvAccessKeyHeader, c.EnvAccessKey},
		{CurrentEphemeralRunHeader, c.CurrentEphemeralRun},
		{WorkerIDHeader, c.WorkerID},
	}
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set(SdkHeader, c.Tag.Sdk)
			if c.Tag.EngineHeader != "" && c.Tag.EngineValue != "" {
				req.Header().Set(c.Tag.EngineHeader, c.Tag.EngineValue)
			}
			for _, h := range set {
				if h[1] != "" {
					req.Header().Set(h[0], h[1])
				}
			}
			return next(ctx, req)
		}
	}
}
