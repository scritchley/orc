package orc

import (
	"bytes"
	"encoding/json"
	"math/big"
	"testing"
)

func TestDecimal(t *testing.T) {

	d := Decimal{big.NewInt(-8361232), 4}

	if v := d.Float64(); v != -836.1232 {
		t.Errorf("Test failed, expected -836.1232 got %v", v)
	}

	if v := d.Float32(); v != -836.1232 {
		t.Errorf("Test failed, expected -836.1232 got %v", v)
	}

	byt, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	expected := []byte(`-836.1232`)
	if !bytes.Equal(byt, expected) {
		t.Errorf("Test failed, expected %s got %s", expected, byt)
	}

}
