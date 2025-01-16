package sorted2map

import (
	"context"
	"iter"

	gu "github.com/google/uuid"
	ui "github.com/takanoriyanagitani/go-avro-uuid2int"
	. "github.com/takanoriyanagitani/go-avro-uuid2int/util"
)

func SortedUuidsToMap(i iter.Seq2[gu.UUID, error]) IO[ui.UuidMapInt] {
	return func(_ context.Context) (ui.UuidMapInt, error) {
		ret := map[gu.UUID]int32{}
		var ix int32
		for id, e := range i {
			if nil != e {
				return ret, e
			}

			ret[id] = ix
			ix += 1
		}
		return ret, nil
	}
}
