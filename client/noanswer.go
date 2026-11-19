package client

import (
	"errors"
	"time"

	"connectrpc.com/connect"
)

type strategyKind int

const (
	keepTrying strategyKind = iota
	fail
	retry
	giveUpQuietly
	forwardAnyway
)

// DefaultKeepTryingDelay is the pause between two asks of KeepTrying when it
// carries none.
const DefaultKeepTryingDelay = 5 * time.Second

// NoAnswerStrategy is what the worker does when the hello gets no answer. The
// zero value is KeepTrying with the default delay.
type NoAnswerStrategy struct {
	kind     strategyKind
	attempts int
	delay    time.Duration
}

// KeepTrying starts the worker at once and asks again in the background,
// with delay between two asks, DefaultKeepTryingDelay when it is zero, and
// once answered does what the answer says. It is the default.
func KeepTrying(delay time.Duration) NoAnswerStrategy {
	return NoAnswerStrategy{kind: keepTrying, delay: delay}
}

// Fail returns the error of the hello at once.
func Fail() NoAnswerStrategy { return NoAnswerStrategy{kind: fail} }

// Retry asks again, attempts times in all with delay between two asks, and
// then returns the error the way Fail does.
func Retry(attempts int, delay time.Duration) NoAnswerStrategy {
	return NoAnswerStrategy{kind: retry, attempts: attempts, delay: delay}
}

// GiveUpQuietly starts the worker with nothing sent on a timer.
func GiveUpQuietly() NoAnswerStrategy { return NoAnswerStrategy{kind: giveUpQuietly} }

// ForwardAnyway starts the worker as if it was told to forward when it has a
// metrics address and a current ephemeral run, at the default interval.
func ForwardAnyway() NoAnswerStrategy { return NoAnswerStrategy{kind: forwardAnyway} }

func (s NoAnswerStrategy) keepTryingDelay() time.Duration {
	if s.delay <= 0 {
		return DefaultKeepTryingDelay
	}
	return s.delay
}

// refusals are the codes Flowrensic refuses a hello with on purpose, such as
// an env it does not know or a wrong key.
var refusals = map[connect.Code]bool{
	connect.CodeInvalidArgument:    true,
	connect.CodeNotFound:           true,
	connect.CodeAlreadyExists:      true,
	connect.CodePermissionDenied:   true,
	connect.CodeFailedPrecondition: true,
	connect.CodeOutOfRange:         true,
	connect.CodeUnauthenticated:    true,
}

// noAnswer is whether the call got no answer a worker can act on: it never
// reached a Flowrensic that could answer it, Flowrensic failed, or the answer
// was broken. Only a refusal on purpose, an error the server sent, is an
// answer; connect-go makes an error of the same codes itself for an answer it
// cannot read.
func noAnswer(err error) bool {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return true
	}
	return !connect.IsWireError(ce) || !refusals[ce.Code()]
}
