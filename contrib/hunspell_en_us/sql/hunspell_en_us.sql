CREATE EXTENSION hunspell_en_us;

CREATE TABLE table1(name varchar);
INSERT INTO table1 VALUES ('stories'), ('traveled'), ('eaten'),
						('Saturdays'), ('healthcare'), ('generally'),
						('integrating'), ('lankiness'), ('rewritten');

SELECT ts_lexize('public.english_hunspell', name) FROM table1;

CREATE INDEX name_idx ON table1 USING GIN (to_tsvector('public.english', "name"));
SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.english', t.name) AS d;

DROP INDEX name_idx;
CREATE INDEX name_idx ON table1 USING GIST (to_tsvector('public.english', "name"));
SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.english', t.name) AS d;