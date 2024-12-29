package sorted2iter

import (
	"bufio"
	"io"
	"iter"
	"os"

	gu "github.com/google/uuid"
)

func SortedReaderToUuids(
	rdr io.Reader,
) iter.Seq2[gu.UUID, error] {
	return func(yield func(gu.UUID, error) bool) {
		var id gu.UUID

		var br io.Reader = bufio.NewReader(rdr)

		for {
			_, e := io.ReadFull(br, id[:])
			if io.EOF == e {
				return
			}

			if !yield(id, e) {
				return
			}
		}
	}
}

func FileLikeToUuids(
	f io.ReadCloser,
) iter.Seq2[gu.UUID, error] {
	return func(yield func(gu.UUID, error) bool) {
		defer f.Close()

		var i iter.Seq2[gu.UUID, error] = SortedReaderToUuids(f)

		for id, e := range i {
			if !yield(id, e) {
				return
			}
		}
	}
}

func FilenameToUuids(
	filename string,
) iter.Seq2[gu.UUID, error] {
	f, e := os.Open(filename)
	if nil != e {
		return func(yield func(gu.UUID, error) bool) {
			var buf gu.UUID
			yield(buf, e)
		}
	}
	return FileLikeToUuids(f)
}
