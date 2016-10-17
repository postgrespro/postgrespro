/* contrib/lz/lz-1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION lz" to load this file. \quit

-- Compression handlers

CREATE FUNCTION zstd_handler(internal)
RETURNS compression_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION lz4_handler(internal)
RETURNS compression_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION lz4hc_handler(internal)
RETURNS compression_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION lz4d_handler(internal)
RETURNS compression_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION snappy_handler(internal)
RETURNS compression_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Compression methods

CREATE COMPRESSION METHOD zstd FOR "any" HANDLER zstd_handler;
COMMENT ON COMPRESSION METHOD zstd IS 'zstandard compression method';

CREATE COMPRESSION METHOD lz4 FOR "any" HANDLER lz4_handler;
COMMENT ON COMPRESSION METHOD lz4 IS 'LZ4 Fast compression method';

CREATE COMPRESSION METHOD lz4hc FOR "any" HANDLER lz4hc_handler;
COMMENT ON COMPRESSION METHOD lz4hc IS 'LZ4 High Compression method';

CREATE COMPRESSION METHOD lz4d FOR "any" HANDLER lz4d_handler;
COMMENT ON COMPRESSION METHOD lz4d IS 'LZ4 Dictionary Compression method';

CREATE COMPRESSION METHOD snappy FOR "any" HANDLER snappy_handler;
COMMENT ON COMPRESSION METHOD snappy IS 'snappy compression method';

-- LZ4 Dictionary

CREATE TABLE lz4_dictionary
(
	dict_name name PRIMARY KEY,
	dict_data bytea
) WITH oids;

CREATE INDEX lz4_dictionary_oid_idx  ON lz4_dictionary(oid);
CREATE INDEX lz4_dictionary_name_idx ON lz4_dictionary(dict_name);

CREATE FUNCTION lz4_dictionary_create(dict_name name)
RETURNS oid
AS
$$
	INSERT INTO lz4_dictionary (dict_name, dict_data)
	VALUES (dict_name, '')
	RETURNING oid
$$
LANGUAGE SQL STRICT;

CREATE FUNCTION lz4_dictionary_add_sample(dict_id oid, sample "any")
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION lz4_dictionary_add_sample(dict_name name, sample "any")
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

