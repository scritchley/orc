package orc

import (
	"math"
	"testing"
)

func TestSubtractionOverflow(t *testing.T) {
	testCases := []struct {
		a, b bool
	}{
		{
			false,
			isSafeSubtract(int64(22222222222), math.MinInt64),
		},
		{
			false,
			isSafeSubtract(int64(-22222222222), math.MaxInt64),
		},
		{
			false,
			isSafeSubtract(math.MinInt64, math.MaxInt64),
		},
		{
			true,
			isSafeSubtract(int64(-1553103058346370095), int64(6553103058346370095)),
		},
		{
			true,
			isSafeSubtract(int64(0), math.MaxInt64),
		},
		{
			true,
			isSafeSubtract(math.MinInt64, 0),
		},
	}

	for i, tc := range testCases {
		if tc.a != tc.b {
			t.Errorf("Test failed, case %v", i)
		}
	}
}
