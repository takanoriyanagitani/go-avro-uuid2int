package uuid2int

import (
	"errors"
	"fmt"
	"iter"

	gu "github.com/google/uuid"
)

var (
	ErrUuidMissing error = errors.New("uuid missing")
	ErrInvalidUuid error = errors.New("invalid uuid")

	ErrUnexpectedUuid error = errors.New("unexpected uuid")

	ErrAnyNil error = errors.New("nil any got")
)

type UuidMapInt map[gu.UUID]int32

const UuidSize int = 16

func AnyToUuid(
	i any,
) (gu.UUID, error) {
	var buf gu.UUID
	switch t := i.(type) {
	case [UuidSize]byte:
		return t, nil
	case []byte:
		if UuidSize == len(t) {
			copy(buf[:], t)
			return buf, nil
		}
		return gu.Nil, fmt.Errorf("%w: bytes=%v", ErrInvalidUuid, t)
	case map[string]any:
		for _, val := range t {
			return AnyToUuid(val)
		}
		return gu.Nil, fmt.Errorf("%w: map=%v", ErrInvalidUuid, t)
	case string:
		return gu.Parse(t)
	case nil:
		return gu.Nil, ErrAnyNil
	default:
	}

	return gu.Nil, fmt.Errorf("%w: any=%v", ErrInvalidUuid, i)
}

func MapToUuidAllowMissing(
	m map[string]any,
	uuidColumnName string,
	allowMissing bool,
) (gu.UUID, error) {
	var ret gu.UUID
	raw, found := m[uuidColumnName]
	if !found {
		if allowMissing {
			return gu.Nil, nil
		}
		return ret, ErrUuidMissing
	}

	u, e := AnyToUuid(raw)
	if nil == e {
		return u, nil
	}

	if allowMissing && ErrAnyNil == e {
		return gu.Nil, nil
	}

	return gu.Nil, e
}

func MapToUuid(
	m map[string]any,
	uuidColumnName string,
) (gu.UUID, error) {
	return MapToUuidAllowMissing(m, uuidColumnName, false)
}

func (m UuidMapInt) MapsToMapsAllowMissing(
	original iter.Seq2[map[string]any, error],
	uuidColumnName string,
	allowMissingUuid bool,
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

			id, e := MapToUuidAllowMissing(
				row,
				uuidColumnName,
				allowMissingUuid,
			)
			if nil != e {
				yield(buf, e)
				return
			}

			mapd, found := m[id]
			if !found {
				var accept bool = allowMissingUuid && id == gu.Nil
				if !accept {
					yield(buf, ErrUnexpectedUuid)
					return
				}
			}

			buf[uuidColumnName] = mapd

			if !yield(buf, nil) {
				return
			}
		}
	}
}

func (m UuidMapInt) MapsToMaps(
	original iter.Seq2[map[string]any, error],
	uuidColumnName string,
) iter.Seq2[map[string]any, error] {
	return m.MapsToMapsAllowMissing(original, uuidColumnName, false)
}

func (m UuidMapInt) ColumnNameToMapdAllowMissing(
	colname string,
	allowMissingUuid bool,
) func(iter.Seq2[map[string]any, error]) iter.Seq2[map[string]any, error] {
	return func(
		original iter.Seq2[map[string]any, error],
	) iter.Seq2[map[string]any, error] {
		return m.MapsToMapsAllowMissing(original, colname, allowMissingUuid)
	}
}

func (m UuidMapInt) ColumnNameToMapd(
	colname string,
) func(iter.Seq2[map[string]any, error]) iter.Seq2[map[string]any, error] {
	return m.ColumnNameToMapdAllowMissing(colname, false)
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
