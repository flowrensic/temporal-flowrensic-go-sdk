package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
)

// FPP-140-C65: the Retry-After header of an answer 503 from what stands in
// front of Flowrensic reaches the error of the call, which reads as
// unavailable.
func TestFPP140C65RetryAfterReachesTheError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	clients := New(Context{ServerBaseURL: server.URL})
	_, err := clients.Ingest.IngestActivityEvents(context.Background(), connect.NewRequest(&flowrensicv1.IngestActivityEventsRequest{}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("the code is %v: %v", connect.CodeOf(err), err)
	}
	var ce *connect.Error
	if !errors.As(err, &ce) || ce.Meta().Get("Retry-After") != "3" {
		t.Fatalf("the error carries no Retry-After: %v", err)
	}
}
