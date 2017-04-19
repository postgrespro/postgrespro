/* contrib/bson/bson--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bson" to load this file. \quit

CREATE FUNCTION bson_handler(internal)
RETURNS compression_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE COMPRESSION METHOD bson HANDLER bson_handler;
COMMENT ON COMPRESSION METHOD bson IS 'BSON compression method';
