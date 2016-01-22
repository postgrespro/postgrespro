CREATE EXTENSION hunspell_ru_ru;

CREATE TABLE table1(name varchar);
INSERT INTO table1 VALUES ('земля'), ('землей'), ('землями'), ('земли'),
						('туши'), ('тушь'), ('туша'), ('тушат'),
						('тушью');

SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.russian', t.name) AS d;

CREATE INDEX name_idx ON table1 USING GIN (to_tsvector('public.russian', "name"));
SELECT * FROM table1 WHERE to_tsvector('public.russian', name)
	@@ to_tsquery('public.russian', 'землей');
SELECT * FROM table1 WHERE to_tsvector('public.russian', name)
	@@ to_tsquery('public.russian', 'тушь');
SELECT * FROM table1 WHERE to_tsvector('public.russian', name)
	@@ to_tsquery('public.russian', 'туша');

DROP INDEX name_idx;
CREATE INDEX name_idx ON table1 USING GIST (to_tsvector('public.russian', "name"));
SELECT * FROM table1 WHERE to_tsvector('public.russian', name)
	@@ to_tsquery('public.russian', 'землей');
SELECT * FROM table1 WHERE to_tsvector('public.russian', name)
	@@ to_tsquery('public.russian', 'тушь');
SELECT * FROM table1 WHERE to_tsvector('public.russian', name)
	@@ to_tsquery('public.russian', 'туша');