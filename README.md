# orc

[![Build Status](https://drone.io/github.com/scritchley/orc/status.png)](https://drone.io/github.com/scritchley/orc/latest)
[![code-coverage](http://gocover.io/_badge/code.simon-critchley.co.uk/orc)](http://gocover.io/code.simon-critchley.co.uk/orc)
[![go-doc](https://godoc.org/code.simon-critchley.co.uk/orc?status.svg)](https://godoc.org/code.simon-critchley.co.uk/orc)


### Example

    r, err := Open("./examples/demo-12-zlib.orc")
    if err != nil {
        log.Fatal(err)
    }
    defer r.Close()

    c := r.Select("_col0", "_col1", "_col2")

    for c.Stripes()
            
        for c.Next() {
            
            log.Println(c.Row())
            
        }
       
    }

    if err := c.Err(); err != nil {
        log.Fatal(err)
    }
