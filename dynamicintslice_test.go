package orc

import (
	"testing"
)

func TestDynamicIntSlice(t *testing.T) {

	dis := NewDynamicIntSlice(10)
	for i := 0; i < 10000; i++ {
		dis.add(2 * i)
	}

	if dis.size() != 10000 {
		t.Errorf("Test failed, got size %v", dis.size())
	}

	for i := 0; i < 10000; i++ {
		if v := dis.get(i); v != 2*i {
			t.Errorf("Test failed, got size %v", v)
		}
	}

	dis.clear()
	dis.add(3)
	dis.add(12)
	dis.add(65)

	if s, err := dis.String(); s != "{3,12,65}" {
		if err != nil {
			t.Fatal(err)
		}
		t.Errorf("Test failed, got string %s", s)
	}

	for i := 0; i < 5; i++ {
		dis.increment(i, 3)
	}

	if s, err := dis.String(); s != "{6,15,68,3,3}" {
		if err != nil {
			t.Fatal(err)
		}
		t.Errorf("Test failed, got string %s", s)
	}

}
