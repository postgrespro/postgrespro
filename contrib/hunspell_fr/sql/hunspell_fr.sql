CREATE EXTENSION hunspell_fr;

CREATE TABLE table1(name varchar);
INSERT INTO table1 VALUES ('batifoler'), ('batifolant'), ('batifole'), ('batifolait'),
						('consentant'), ('consentir'), ('consentiriez');

SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.french', t.name) AS d;

CREATE INDEX name_idx ON table1 USING GIN (to_tsvector('public.french', "name"));
SELECT * FROM table1 WHERE to_tsvector('public.french', name)
	@@ to_tsquery('public.french', 'batifolant');
SELECT * FROM table1 WHERE to_tsvector('public.french', name)
	@@ to_tsquery('public.french', 'consentiriez');

DROP INDEX name_idx;
CREATE INDEX name_idx ON table1 USING GIST (to_tsvector('public.french', "name"));
SELECT * FROM table1 WHERE to_tsvector('public.french', name)
	@@ to_tsquery('public.french', 'batifolant');
SELECT * FROM table1 WHERE to_tsvector('public.french', name)
	@@ to_tsquery('public.french', 'consentiriez');