CREATE EXTENSION hunspell_en_us;

CREATE TABLE table1(name varchar);
INSERT INTO table1 VALUES ('leaves'), ('leaved'), ('leaving'),
						('inability'), ('abilities'), ('disability'), ('ability');

SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.english', t.name) AS d;

CREATE INDEX name_idx ON table1 USING GIN (to_tsvector('public.english', "name"));
SELECT * FROM table1 WHERE to_tsvector('public.english', name)
	@@ to_tsquery('public.english', 'leaving');
SELECT * FROM table1 WHERE to_tsvector('public.english', name)
	@@ to_tsquery('public.english', 'abilities');
SELECT * FROM table1 WHERE to_tsvector('public.english', name)
	@@ to_tsquery('public.english', 'ability');

DROP INDEX name_idx;
CREATE INDEX name_idx ON table1 USING GIST (to_tsvector('public.english', "name"));
SELECT * FROM table1 WHERE to_tsvector('public.english', name)
	@@ to_tsquery('public.english', 'leaving');
SELECT * FROM table1 WHERE to_tsvector('public.english', name)
	@@ to_tsquery('public.english', 'abilities');
SELECT * FROM table1 WHERE to_tsvector('public.english', name)
	@@ to_tsquery('public.english', 'ability');