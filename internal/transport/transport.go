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
	TemporalSdkHeader         = "Flowrensic-Temporal-Sdk"
	WorkerIDHeader            = "Flowrensic-Worker-Id"
)

const modulePath = "go.flowrensic.com/temporal-flowrensic-go-sdk"

const temporalModulePath = "go.temporal.io/sdk"

// SdkHeaderValue is go, a slash and the version of this SDK: the tag of the
// module in the build info of the program, dev when the module has no version
// there or is replaced by a folder.
var SdkHeaderValue = "go/" + sdkVersion()

func sdkVersion() string {
	v := moduleVersion(modulePath)
	if v == "" || v == "(devel)" || strings.HasPrefix(v, "0.0.0") {
		return "dev"
	}
	return v
}

// TemporalSdkHeaderValue is the version of go.temporal.io/sdk in the build
// info of the program, with no leading v, empty when it is not known.
var TemporalSdkHeaderValue = temporalSdkVersion()

func temporalSdkVersion() string {
	v := moduleVersion(temporalModulePath)
	if v == "(devel)" {
		return ""
	}
	return v
}

// moduleVersion is the version of the module path in the build info of the
// program, with no leading v: the version of its replacement when it is
// replaced, empty when the program has no build info, does not hold the
// module, or replaces it by a folder.
func moduleVersion(path string) string {
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
	opts := connect.WithInterceptors(headers(c))
	return Clients{
		Ingest: flowrensicv1connect.NewIngestServiceClient(httpClient, base, opts),
		Query:  flowrensicv1connect.NewQueryServiceClient(httpClient, base, opts),
	}
}

// headers puts the SDK, the Temporal SDK when known, and the env, its access
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
			req.Header().Set(SdkHeader, SdkHeaderValue)
			if TemporalSdkHeaderValue != "" {
				req.Header().Set(TemporalSdkHeader, TemporalSdkHeaderValue)
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
