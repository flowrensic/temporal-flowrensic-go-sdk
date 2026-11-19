package client

import (
	"errors"
	"reflect"
	"testing"

	"connectrpc.com/connect"
)

type MetricsInterceptor struct{}

// FPP-140-C56: the name of a workflow interceptor is the last part of its Go
// type name or file path, and no list given sends none, which is not an empty
// list.
func TestFPP140C56InterceptorNames(t *testing.T) {
	for in, want := range map[string]string{
		"*github.com/acme/app/interceptors.MetricsInterceptor": "MetricsInterceptor",
		"interceptors.TraceInterceptor":                        "TraceInterceptor",
		"./interceptors/Metrics.go":                            "Metrics",
		`C:\app\interceptors\Metrics.go`:                       "Metrics",
		"Plain":                                                "Plain",
	} {
		if got := InterceptorName(in); got != want {
			t.Errorf("InterceptorName(%q) = %q, want %q", in, got, want)
		}
	}
	if got := InterceptorNames(&MetricsInterceptor{}, MetricsInterceptor{}); !reflect.DeepEqual(got, []string{"MetricsInterceptor", "MetricsInterceptor"}) {
		t.Errorf("InterceptorNames = %v", got)
	}
	if interceptorList(nil) != nil {
		t.Error("no list sent a list")
	}
	if got := interceptorList([]string{}); got == nil || len(got.GetNames()) != 0 {
		t.Errorf("an empty list = %v", got)
	}
}

// FPP-140-C57: the engine's page keeps only the series named and their HELP
// and TYPE lines, a histogram's by its suffixed names too; no names keeps the
// whole page.
func TestFPP140C57OnlyNamed(t *testing.T) {
	page := "# HELP a_seconds A.\n# TYPE a_seconds histogram\na_seconds_bucket{le=\"1\"} 1\na_seconds_count 1\n# HELP b B.\n# TYPE b counter\nb 2\nc{x=\"y\"} 3\n"
	got := onlyNamed(page, map[string]bool{"a_seconds_count": true, "c": true})
	want := "# HELP a_seconds A.\n# TYPE a_seconds histogram\na_seconds_count 1\nc{x=\"y\"} 3\n"
	if got != want {
		t.Errorf("onlyNamed = %q, want %q", got, want)
	}
	if onlyNamed(page, nil) != page {
		t.Error("no names did not keep the page")
	}
	if onlyNamed(page, map[string]bool{"zzz": true}) != "" {
		t.Error("no match did not give an empty page")
	}
}

// FPP-140-C57: only a refusal the server sent is an answer; an error
// connect-go makes itself, a code that is not a refusal and any other error
// are no answer. An interval out of 1 to 60 seconds is the default.
func TestFPP140C57NoAnswer(t *testing.T) {
	refused := connect.NewWireError(connect.CodePermissionDenied, errors.New("wrong key"))
	if noAnswer(refused) {
		t.Error("a refusal of the server was no answer")
	}
	if !noAnswer(connect.NewError(connect.CodeInvalidArgument, errors.New("unmarshal"))) {
		t.Error("an error of the client was an answer")
	}
	if !noAnswer(connect.NewWireError(connect.CodeInternal, nil)) || !noAnswer(errors.New("other")) {
		t.Error("a failure was an answer")
	}
	for in, want := range map[int32]int{0: 5, 1: 1, 60: 60, 61: 5} {
		if got := checkedInterval(in); got != want {
			t.Errorf("checkedInterval(%d) = %d, want %d", in, got, want)
		}
	}
}
