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

func TestZigZagEncoder(t *testing.T) {
	ints := []int64{0, -1, 1, -2, 2, -3, 3, -4, 4, -5}
	for i, v := range ints {
		if int(zigzagEncode(v)) != i {
			t.Errorf("Test failed, expected %v to equal %v", v, i)
		}
	}
}

func TestZigZagDecode(t *testing.T) {
	ints := []int64{0, -1, 1, -2, 2, -3, 3, -4, 4, -5}
	for i, v := range ints {
		if zigzagDecode(uint64(i)) != v {
			t.Errorf("Test failed, expected %v to equal %v", i, v)
		}
	}
}

func BenchmarkzigzagEncodeeger(b *testing.B) {
	for i := 0; i < b.N; i++ {
		zigzagEncode(int64(i))
	}
}

func BenchmarkzigzagDecodeeger(b *testing.B) {
	for i := 0; i < b.N; i++ {
		zigzagDecode(uint64(i))
	}
}
