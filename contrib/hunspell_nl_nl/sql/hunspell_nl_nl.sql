CREATE EXTENSION hunspell_nl_nl;

CREATE TABLE table1(name varchar);
INSERT INTO table1 VALUES ('deuren'), ('deurtje'), ('deur'),
						('twee'), ('tweehonderd'), ('tweeduizend');

SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.dutch', t.name) AS d;

CREATE INDEX name_idx ON table1 USING GIN (to_tsvector('public.dutch', "name"));
SELECT * FROM table1 WHERE to_tsvector('public.dutch', name)
	@@ to_tsquery('public.dutch', 'deurtje');
SELECT * FROM table1 WHERE to_tsvector('public.dutch', name)
	@@ to_tsquery('public.dutch', 'twee');

DROP INDEX name_idx;
CREATE INDEX name_idx ON table1 USING GIST (to_tsvector('public.dutch', "name"));
SELECT * FROM table1 WHERE to_tsvector('public.dutch', name)
	@@ to_tsquery('public.dutch', 'deurtje');
SELECT * FROM table1 WHERE to_tsvector('public.dutch', name)
	@@ to_tsquery('public.dutch', 'twee');