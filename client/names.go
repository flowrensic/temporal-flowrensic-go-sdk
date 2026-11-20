package client

import "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/worker"

// InterceptorName is the name of a workflow interceptor as Flowrensic is sent
// it: the last part of a Go type name or of a file path, without a leading
// star, the package or folders, and a .go extension. So
// *github.com/acme/app/interceptors.Metrics, interceptors.Metrics and
// ./interceptors/Metrics.go are all Metrics.
func InterceptorName(name string) string { return worker.InterceptorName(name) }

// InterceptorNames is the type name of each workflow interceptor given, in
// order, to put in Options.WorkflowInterceptors and PublishOptions.
func InterceptorNames(interceptors ...any) []string { return worker.InterceptorNames(interceptors...) }
