/* contrib/hunspell_fr/hunspell_fr--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hunspell_fr" to load this file. \quit

CREATE TEXT SEARCH DICTIONARY public.french_hunspell (
    TEMPLATE = ispell,
    DictFile = fr,
    AffFile = fr,
    StopWords = french
);

CREATE TEXT SEARCH CONFIGURATION public.french (
    COPY = pg_catalog.simple
);

ALTER TEXT SEARCH CONFIGURATION public.french
    ALTER MAPPING FOR asciiword, asciihword, hword_asciipart,
        word, hword, hword_part
    WITH public.french_hunspell, pg_catalog.french_stem;
