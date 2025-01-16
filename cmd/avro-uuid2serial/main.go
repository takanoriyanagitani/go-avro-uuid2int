package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strconv"
	"strings"

	gu "github.com/google/uuid"

	ui "github.com/takanoriyanagitani/go-avro-uuid2int"
	. "github.com/takanoriyanagitani/go-avro-uuid2int/util"

	sm "github.com/takanoriyanagitani/go-avro-uuid2int/sorted2map"

	us "github.com/takanoriyanagitani/go-avro-uuid2int/uuid2int/sorted2iter"

	dh "github.com/takanoriyanagitani/go-avro-uuid2int/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-uuid2int/avro/enc/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var uuidColName IO[string] = EnvValByKey("ENV_UUID_PKEY_COLNAME")

var sortedUuidFilename IO[string] = EnvValByKey("ENV_SORTED_UUID_FILENAME")

var sortedUuid IO[iter.Seq2[gu.UUID, error]] = Bind(
	sortedUuidFilename,
	Lift(func(name string) (iter.Seq2[gu.UUID, error], error) {
		return us.FilenameToUuids(name), nil
	}),
)

var uuid2int IO[ui.UuidMapInt] = Bind(
	sortedUuid,
	sm.SortedUuidsToMap,
)

type Config struct {
	UuidColumnName   string
	AllowMissingUuid bool
}

var allowMissingUuid IO[bool] = Bind(
	EnvValByKey("ENV_ALLOW_MISSING_UUID"),
	Lift(strconv.ParseBool),
).Or(Of(false))

var config IO[Config] = Bind(
	uuidColName,
	func(colname string) IO[Config] {
		return Bind(
			allowMissingUuid,
			Lift(func(allowMissing bool) (Config, error) {
				return Config{
					UuidColumnName:   colname,
					AllowMissingUuid: allowMissing,
				}, nil
			}),
		)
	},
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	config,
	func(cfg Config) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			uuid2int,
			func(umi ui.UuidMapInt) IO[iter.Seq2[map[string]any, error]] {
				return Bind(
					stdin2maps,
					Lift(func(
						original iter.Seq2[map[string]any, error],
					) (iter.Seq2[map[string]any, error], error) {
						return umi.ColumnNameToMapdAllowMissing(
							cfg.UuidColumnName,
							cfg.AllowMissingUuid,
						)(original), nil
					}),
				)
			},
		)
	},
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(schema string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(schema),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
