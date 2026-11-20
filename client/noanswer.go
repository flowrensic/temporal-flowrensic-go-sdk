package client

import (
	"time"

	"go.flowrensic.com/temporal-flowrensic-go-sdk/internal/worker"
)

// DefaultKeepTryingDelay is the pause between two asks of KeepTrying when it
// carries none.
const DefaultKeepTryingDelay = worker.DefaultKeepTryingDelay

// NoAnswerStrategy is what the worker does when the hello gets no answer. The
// zero value is KeepTrying with the default delay.
type NoAnswerStrategy = worker.NoAnswerStrategy

// KeepTrying starts the worker at once and asks again in the background,
// with delay between two asks, DefaultKeepTryingDelay when it is zero, and
// once answered does what the answer says. It is the default.
func KeepTrying(delay time.Duration) NoAnswerStrategy { return worker.KeepTrying(delay) }

// Fail returns the error of the hello at once.
func Fail() NoAnswerStrategy { return worker.Fail() }

// Retry asks again, attempts times in all with delay between two asks, and
// then returns the error the way Fail does.
func Retry(attempts int, delay time.Duration) NoAnswerStrategy {
	return worker.Retry(attempts, delay)
}

// GiveUpQuietly starts the worker with nothing sent on a timer.
func GiveUpQuietly() NoAnswerStrategy { return worker.GiveUpQuietly() }

// ForwardAnyway starts the worker as if it was told to forward when it has a
// metrics address and a current ephemeral run, at the default interval.
func ForwardAnyway() NoAnswerStrategy { return worker.ForwardAnyway() }
