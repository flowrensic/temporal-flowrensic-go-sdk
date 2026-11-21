package transport

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"connectrpc.com/connect"
)

// RetryAfterHeader is the header of an answer that says how long to wait
// before the next call.
const RetryAfterHeader = "Retry-After"

type retryAfterKey struct{}

// retryAfterNote holds the Retry-After header of the answer to one call.
type retryAfterNote struct {
	mu    sync.Mutex
	value string
}

// retryAfterTransport notes the Retry-After header of an answer that is not
// 200 in the note of the call, since connect-go leaves the headers out of the
// error of an answer that carries no Connect error, such as a 503 from what
// stands in front of Flowrensic.
type retryAfterTransport struct {
	base http.RoundTripper
}

func (t retryAfterTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.base.RoundTrip(req)
	if err == nil && res.StatusCode != http.StatusOK {
		if note, ok := req.Context().Value(retryAfterKey{}).(*retryAfterNote); ok {
			if value := res.Header.Get(RetryAfterHeader); value != "" {
				note.mu.Lock()
				note.value = value
				note.mu.Unlock()
			}
		}
	}
	return res, err
}

// withRetryAfter is a copy of client whose transport notes Retry-After.
func withRetryAfter(client *http.Client) *http.Client {
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	copied := *client
	copied.Transport = retryAfterTransport{base: base}
	return &copied
}

// retryAfterOfAnswer puts the Retry-After header of an answer that failed in
// the metadata of the error, where the buffer reads it.
func retryAfterOfAnswer() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			note := &retryAfterNote{}
			res, err := next(context.WithValue(ctx, retryAfterKey{}, note), req)
			var ce *connect.Error
			if err != nil && errors.As(err, &ce) && ce.Meta().Get(RetryAfterHeader) == "" {
				note.mu.Lock()
				value := note.value
				note.mu.Unlock()
				if value != "" {
					ce.Meta().Set(RetryAfterHeader, value)
				}
			}
			return res, err
		}
	}
}
