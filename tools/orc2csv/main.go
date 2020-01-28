package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"orc"
)

var (
	filepath = flag.String("f", "", "the json file to convert")
	cols     = flag.String("c", "", "the columns to read")
)

func main() {

	flag.Parse()

	w := csv.NewWriter(os.Stdout)

	r, err := orc.Open(*filepath)
	if err != nil {
		log.Fatal(err)
	}

	selected := r.Schema().Columns()
	if *cols != "" {
		selected = strings.Split(*cols, ",")
	}

	c := r.Select(selected...)
	defer c.Close()

	vals := make([]interface{}, len(selected))
	ptrVals := make([]interface{}, len(selected))
	strVals := make([]string, len(selected))
	for i := range vals {
		ptrVals[i] = &vals[i]
	}

	for c.Stripes() {
		for c.Next() {
			err := c.Scan(ptrVals...)
			if err != nil {
				log.Fatal(err)
			}
			for i := range ptrVals {
				strVals[i] = fmt.Sprint(ptrVals[i])
			}
			err = w.Write(strVals)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	w.Flush()

	if err := c.Err(); err != nil {
		log.Fatal(err)
	}

}
