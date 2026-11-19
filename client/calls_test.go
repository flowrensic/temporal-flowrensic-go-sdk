package client

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/api/serviceerror"
)

// FPP-140-C59: a failed call carries the canonical gRPC code name when its
// error has a gRPC status, and the Go type name otherwise; it ended on its
// deadline for DEADLINE_EXCEEDED, and for CANCELLED only while the caller's
// context is not done.
func TestFPP140C59CallErrors(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{serviceerror.NewNotFound("gone"), "NOT_FOUND"},
		{serviceerror.NewQueryFailed("boom"), "INVALID_ARGUMENT"},
		{serviceerror.NewDeadlineExceeded("late"), "DEADLINE_EXCEEDED"},
		{serviceerror.NewCanceled("stop"), "CANCELLED"},
		{context.DeadlineExceeded, "DEADLINE_EXCEEDED"},
		{errors.New("plain"), "errorString"},
	} {
		if got := callErrorType(c.err); got != c.want {
			t.Errorf("callErrorType(%T) = %q, want %q", c.err, got, c.want)
		}
	}
	live := context.Background()
	done, cancel := context.WithCancel(live)
	cancel()
	if !endedOnDeadline(live, serviceerror.NewDeadlineExceeded("late")) || !endedOnDeadline(live, serviceerror.NewCanceled("by the server")) {
		t.Error("a deadline was not seen")
	}
	if endedOnDeadline(done, serviceerror.NewCanceled("by the caller")) || endedOnDeadline(live, serviceerror.NewNotFound("gone")) {
		t.Error("a call that did not end on its deadline was seen as one")
	}
}
