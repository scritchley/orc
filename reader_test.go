package orc

import (
	"fmt"
	"io"
	"testing"
)

func TestReader(t *testing.T) {

	r, err := Open("./examples/orc-file-11-format.orc")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	c := r.Select("decimal1")

	for c.Stripes() {

		for c.Next() {

			fmt.Println(c.Row())

		}

	}

	if err := c.Err(); err != nil && err != io.EOF {
		t.Fatal(err)
	}

}
