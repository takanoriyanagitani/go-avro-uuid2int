package uuid2int

import (
	"errors"
	"iter"

	gu "github.com/google/uuid"
)

var (
	ErrUuidMissing error = errors.New("uuid missing")
	ErrInvalidUuid error = errors.New("invalid uuid")

	ErrUnexpectedUuid error = errors.New("unexpected uuid")
)

type UuidMapInt map[gu.UUID]int32

func AnyToUuid(
	i any,
) (gu.UUID, error) {
	var buf gu.UUID
	switch t := i.(type) {
	case [16]byte:
		return t, nil
	case []byte:
		if 16 == len(t) {
			copy(buf[:], t)
			return buf, nil
		}
	default:
	}

	return buf, ErrInvalidUuid
}

func MapToUuid(
	m map[string]any,
	uuidColumnName string,
) (gu.UUID, error) {
	var ret gu.UUID
	raw, found := m[uuidColumnName]
	if !found {
		return ret, ErrUuidMissing
	}
	return AnyToUuid(raw)
}

func (m UuidMapInt) MapsToMaps(
	original iter.Seq2[map[string]any, error],
	uuidColumnName string,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		buf := map[string]any{}

		for row, e := range original {
			clear(buf)

			if nil != e {
				yield(buf, e)
				return
			}

			for key, val := range row {
				buf[key] = val
			}

			id, e := MapToUuid(row, uuidColumnName)
			if nil != e {
				yield(buf, e)
				return
			}

			mapd, found := m[id]
			if !found {
				yield(buf, ErrUnexpectedUuid)
				return
			}

			buf[uuidColumnName] = mapd

			if !yield(buf, nil) {
				return
			}
		}
	}
}

func (m UuidMapInt) ColumnNameToMapd(
	colname string,
) func(iter.Seq2[map[string]any, error]) iter.Seq2[map[string]any, error] {
	return func(
		original iter.Seq2[map[string]any, error],
	) iter.Seq2[map[string]any, error] {
		return m.MapsToMaps(original, colname)
	}
}

const BlobSizeMaxDefault int = 1048576

type DecodeConfig struct {
	BlobSizeMax int
}

var DecodeConfigDefault DecodeConfig = DecodeConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

const BlockLengthDefault int = 100

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
