/* contrib/hunspell_tr_tr/hunspell_tr_tr--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hunspell_tr_tr" to load this file. \quit

CREATE TEXT SEARCH DICTIONARY public.turkish_hunspell (
    TEMPLATE = ispell,
    DictFile = tr_tr,
    AffFile = tr_tr,
    StopWords = turkish
);

CREATE TEXT SEARCH CONFIGURATION public.turkish (
    COPY = pg_catalog.simple
);

ALTER TEXT SEARCH CONFIGURATION public.turkish
    ALTER MAPPING FOR asciiword, asciihword, hword_asciipart,
        word, hword, hword_part
    WITH public.turkish_hunspell, pg_catalog.turkish_stem;
