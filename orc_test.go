package orc

import (
	"os"
	"testing"
)

type testCase struct {
	filepath string
	expect   func(f *os.File)
}

func runTestCases(testCases []testCase, t *testing.T) {
	for _, tc := range testCases {
		fd, err := os.Open(tc.filepath)
		if err != nil {
			t.Fatal(err)
		}
		tc.expect(fd)
	}
}

func TestReadPostScript(t *testing.T) {

	testCases := []testCase{
		// {
		// 	filepath: "./testdata/000000_0",
		// 	expect: func(f *os.File) {

		// 		decoder := NewDecoder(FileORCReader{f})

		// 		err := decoder.getTail()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		err = decoder.readPostScript()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		err = decoder.readTail()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		err = decoder.Cursor()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 	},
		// },
		// {
		// 	filepath: "./testdata/TestOrcFile.metaData.orc",
		// 	expect: func(f *os.File) {

		// 		decoder := NewDecoder(FileORCReader{f})

		// 		err := decoder.getTail()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		err = decoder.readPostScript()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		// t.Log(decoder.PostScript)

		// 		err = decoder.readCompleteFooter()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		// t.Log(decoder.Metadata)

		// 		t.Log(decoder.Footer)

		// 	},
		// },

		// {
		// 	filepath: "./testdata/TestOrcFile.testDate1900.orc",
		// 	expect: func(f *os.File) {

		// 		decoder := NewDecoder(FileORCReader{f})

		// 		err := decoder.getTail()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		err = decoder.readPostScript()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		err = decoder.readTail()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 		err = decoder.Cursor()
		// 		if err != nil {
		// 			t.Fatal(err)
		// 		}

		// 	},
		// },

		{
			filepath: "./testdata/TestOrcFile.testDate2038.orc",
			expect: func(f *os.File) {

				decoder := NewDecoder(FileORCReader{f})

				err := decoder.getTail()
				if err != nil {
					t.Fatal(err)
				}

				err = decoder.readPostScript()
				if err != nil {
					t.Fatal(err)
				}

				err = decoder.readTail()
				if err != nil {
					t.Fatal(err)
				}

				err = decoder.Cursor()
				if err != nil {
					t.Fatal(err)
				}

			},
		},
	}

	runTestCases(testCases, t)

}
