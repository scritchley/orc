package orc

// import (
// 	// "fmt"
// 	"io"
// )

// type DictionaryDataStreamReader struct {
// 	*IntStreamReader
// }

// func NewDictionaryDataStreamReader(r io.ByteReader) *DictionaryDataStreamReader {
// 	return &DictionaryDataStreamReader{
// 		IntStreamReader: NewIntStreamReader(r, false),
// 	}
// }

// func (d *DictionaryDataStreamReader) Prepare(i IntStreamReader) error {
// 	for i.HasNext() {
// 		l, ok := i.NextInt()
// 		if !ok {
// 			return fmt.Errorf("error whilst reading integer stream")
// 		}
// 		var b []byte
// 		for j := 0; j < int(l); j++ {

// 			nb, err := d.r.ReadByte()
// 			if err != nil {
// 				return err
// 			}
// 			b = append(b, nb)
// 		}
// 		d.data = append(d.data, b)
// 	}
// 	return nil
// }
