package orc

type DictionaryIndex [][]byte

func NewDictionaryIndex(data []byte, i *IntStreamReader) DictionaryIndex {
	var dict [][]byte
	var offset int
	for i.Next() {
		length, ok := i.Int()
		if ok {
			dict = append(dict, data[offset:offset+int(length)])
			offset += int(length)
		}
	}
	return dict
}

func (d DictionaryIndex) Bytes(i int) []byte {
	if i >= len(d) || i < 0 {
		return nil
	}
	return d[i]
}
