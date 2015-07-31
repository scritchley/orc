package orc

import "os"

type FileORCReader struct {
	*os.File
}

func (f FileORCReader) Size() int64 {
	stat, err := f.Stat()
	if err != nil {
		return int64(0)
	}
	return stat.Size()
}
