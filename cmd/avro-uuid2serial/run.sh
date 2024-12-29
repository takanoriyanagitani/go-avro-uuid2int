#!/bin/sh

genpgcopy(){
	echo "

		COPY (
			SELECT
				'fuji'::TEXT AS name,
				'{}'::BYTEA AS data,
				404::INTEGER AS status,
				42::BIGINT AS id,
				100.0::REAL AS ratio,
				3.776::DOUBLE PRECISION AS height,
				FALSE::BOOLEAN AS active,
				'cafef00d-dead-beaf-face-864299792458'::UUID AS related,
				CLOCK_TIMESTAMP()::TIMESTAMP WITH TIME ZONE AS created,
				NULL::BIGINT AS updated,
				CLOCK_TIMESTAMP()::TIMESTAMP WITH TIME ZONE AS processed
	
			UNION ALL
	
			SELECT
				'takao'::TEXT AS name,
				'{}'::BYTEA AS data,
				403::INTEGER AS status,
				43::BIGINT AS id,
				99.1::REAL AS ratio,
				0.599::DOUBLE PRECISION AS height,
				TRUE::BOOLEAN AS active,
				'dafef00d-dead-beaf-face-864299792458'::UUID AS related,
				CLOCK_TIMESTAMP()::TIMESTAMP WITH TIME ZONE AS created,
				(EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())*1e6)::BIGINT AS updated,
				NULL::TIMESTAMP WITH TIME ZONE AS processed
	
		)
		TO STDOUT
		WITH (
			FORMAT BINARY
		)

	" |
	env PGUSER=postgres LC_ALL=C psql |
	cat > ./sample.d/sample.pgcopy
}

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
	cat ./sample.d/sample.pgcopy |
		pgcopy2avro |
		cat > ./sample.d/sample.avro
}

#genpgcopy
#genavro

export ENV_UUID_PKEY_COLNAME=related
export ENV_SORTED_UUID_FILENAME=sample.d/sorted-uuids.dat
export ENV_SCHEMA_FILENAME=./sample.d/output.avsc

cat sample.d/sample.avro |
	./avro-uuid2serial |
	rq \
		--input-avro \
		--output-json |
	jq --compact-output
