package orc

// import (
// 	"bytes"
// 	"fmt"
// 	"github.com/golang/protobuf/proto"
// 	"io"
// )

// func writeProto(m proto.Message, w io.Writer) error {
// 	byt, err := proto.Marshal(m)
// 	if err != nil {
// 		return err
// 	}
// 	buf := bytes.NewBuffer(byt)
// 	n, err := buf.WriteTo(w)
// 	if err != nil {
// 		return err
// 	}
// 	if n != len(byt) {
// 		return fmt.Errorf("error writing, wrote: %v expected: %v", n, len(byt))
// 	}
// 	return nil
// }
