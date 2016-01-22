CREATE EXTENSION hunspell_fr;

CREATE TABLE table1(name varchar);
INSERT INTO table1 VALUES ('beau'), ('antérieur'), ('fraternel'),
						('plaît'), ('comprends'), ('désolée'),
						('cents'), ('grammairiens'), ('résistèrent'),
						('derniers'), ('rapprochent');

SELECT ts_lexize('public.french_hunspell', name) FROM table1;

CREATE INDEX name_idx ON table1 USING GIN (to_tsvector('public.french', "name"));
SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.french', t.name) AS d;

DROP INDEX name_idx;
CREATE INDEX name_idx ON table1 USING GIST (to_tsvector('public.french', "name"));
SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.french', t.name) AS d;