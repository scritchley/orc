package orc

import (
	"fmt"
	"testing"
)

func TestDictionary(t *testing.T) {
	tree := NewDictionary(5)
	if v := tree.add("owen"); v != 0 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("ashutosh"); v != 1 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("owen"); v != 0 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("alan"); v != 2 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("alan"); v != 2 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("ashutosh"); v != 1 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("greg"); v != 3 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("eric"); v != 4 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("arun"); v != 5 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.Size(); v != 6 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("eric14"); v != 6 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("o"); v != 7 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("ziggy"); v != 8 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("z"); v != 9 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("greg"); v != 3 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("zak"); v != 10 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("eric1"); v != 11 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("ash"); v != 12 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("harry"); v != 13 {
		t.Errorf("Test failed, got  %v", v)
	}
	if v := tree.add("john"); v != 14 {
		t.Errorf("Test failed, got  %v", v)
	}
	tree.clear()
}

func TestDictionary2(t *testing.T) {
	tree := NewDictionary(InitialDictionarySize)
	for i := 0; i < 10000; i++ {
		tree.add(fmt.Sprint(i))
	}
	if tree.Size() != 10000 {
		t.Errorf("Test failed, got %v", tree.Size())
	}
}
