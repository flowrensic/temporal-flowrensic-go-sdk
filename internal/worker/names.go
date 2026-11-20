package worker

import (
	"reflect"
	"strings"

	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
)

// InterceptorName is the name of a workflow interceptor as Flowrensic is sent
// it: the last part of a Go type name or of a file path, without a leading
// star, the package or folders, and a .go extension. So
// *github.com/acme/app/interceptors.Metrics, interceptors.Metrics and
// ./interceptors/Metrics.go are all Metrics.
func InterceptorName(name string) string {
	name = strings.TrimSpace(name)
	if i := strings.LastIndexAny(name, `/\`); i >= 0 {
		name = name[i+1:]
	}
	name = strings.TrimPrefix(strings.TrimSuffix(name, ".go"), "*")
	if i := strings.LastIndex(name, "."); i >= 0 {
		name = name[i+1:]
	}
	return name
}

// InterceptorNames is the type name of each workflow interceptor given, in
// order, to put in Options.WorkflowInterceptors and PublishOptions.
func InterceptorNames(interceptors ...any) []string {
	out := make([]string, 0, len(interceptors))
	for _, one := range interceptors {
		t := reflect.TypeOf(one)
		for t != nil && t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t == nil {
			out = append(out, "")
			continue
		}
		out = append(out, t.Name())
	}
	return out
}

// InterceptorList is the names as Flowrensic is sent them, in their order.
// Nil sends no list, which is not an empty list.
func InterceptorList(names []string) *flowrensicv1.WorkflowInterceptors {
	if names == nil {
		return nil
	}
	out := &flowrensicv1.WorkflowInterceptors{Names: make([]string, 0, len(names))}
	for _, one := range names {
		out.Names = append(out.Names, InterceptorName(one))
	}
	return out
}
