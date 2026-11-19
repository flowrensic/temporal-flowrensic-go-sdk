package owntime

import (
	"reflect"
	"testing"
	"time"
)

// FPP-140-C14: a measure is counted in the bucket of the first edge it is not
// above, one above the last edge in the last bucket, and a take empties the
// counts of its env only.
func TestFPP140C14OwnTimeBuckets(t *testing.T) {
	Spent("a", 5*time.Microsecond)
	Spent("a", 6*time.Microsecond)
	Spent("a", time.Millisecond)
	Spent("a", 2*time.Second)
	Spent("b", 0)

	want := make([]int64, len(EdgesUs)+1)
	want[0] = 1
	want[1] = 1
	want[7] = 1
	want[len(EdgesUs)] = 1
	if got := Take("a"); !reflect.DeepEqual(got, want) {
		t.Fatalf("counts of a = %v, want %v", got, want)
	}
	if got := Take("a"); got != nil {
		t.Fatalf("a second take = %v, want nil", got)
	}
	if got := Take("b"); len(got) != len(EdgesUs)+1 || got[0] != 1 {
		t.Fatalf("counts of b = %v", got)
	}
}
