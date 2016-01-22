CREATE EXTENSION hunspell_nl_nl;

CREATE TABLE table1(name varchar);
INSERT INTO table1 VALUES ('klimmen'), ('zitten'), ('dragen'),
						('mooie'), ('boekje'), ('ouders'), ('deuren'),
						('uitbetalen'), ('achtentwintig');

SELECT ts_lexize('public.dutch_hunspell', name) FROM table1;

CREATE INDEX name_idx ON table1 USING GIN (to_tsvector('public.dutch', "name"));
SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.dutch', t.name) AS d;

DROP INDEX name_idx;
CREATE INDEX name_idx ON table1 USING GIST (to_tsvector('public.dutch', "name"));
SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.dutch', t.name) AS d;