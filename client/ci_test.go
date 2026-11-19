package client

import (
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"

	flowrensicv1 "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/gen/flowrensic/v1"
)

// FPP-140-C60: every run status has its label in a manifest, and a CI call
// that failed says that Flowrensic could not be reached within its time, or
// what it answered, the code in brackets and the message.
func TestFPP140C60CIMessages(t *testing.T) {
	for value := range flowrensicv1.RunStatus_name {
		if statusLabels[flowrensicv1.RunStatus(value)] == "" {
			t.Errorf("run status %d has no label", value)
		}
	}
	url := "http://flowrensic:8080"
	if got, want := ciFailure("deploy report", url, 2*time.Second, connect.NewError(connect.CodeDeadlineExceeded, errors.New("late"))),
		"deploy report: Flowrensic at http://flowrensic:8080 could not be reached within 2 seconds: late"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := ciFailure("deploy report", url, 1500*time.Millisecond, connect.NewError(connect.CodeUnavailable, errors.New("refused"))),
		"deploy report: Flowrensic at http://flowrensic:8080 could not be reached within 1.5 seconds: refused"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	answered := connect.NewWireError(connect.CodeInternal, errors.New("broken store"))
	if got, want := ciFailure("replay results", url, time.Second, answered),
		"replay results: Flowrensic at http://flowrensic:8080 answered [internal] broken store"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if connect.CodeOf(ciError("replay results", url, time.Second, answered)) != connect.CodeInternal {
		t.Error("the code of the answer is lost")
	}
	if wait, again := waitBeforeSendingAgain(connect.NewWireError(connect.CodeUnavailable, nil)); !again || wait != time.Second {
		t.Errorf("unavailable waits %v, %v", wait, again)
	}
	if _, again := waitBeforeSendingAgain(answered); again {
		t.Error("internal is sent again")
	}
}
