CREATE EXTENSION hunspell_ru_ru;

CREATE TABLE table1(name varchar);
INSERT INTO table1 VALUES ('распространённости'), ('межнационального'),
						('включается'), ('значу'), ('зевнув'),
						('зевоты'), ('землей'), ('волчатами'), ('мойтесь');

SELECT ts_lexize('public.russian_hunspell', name) FROM table1;

CREATE INDEX name_idx ON table1 USING GIN (to_tsvector('public.russian', "name"));
SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.russian', t.name) AS d;

DROP INDEX name_idx;
CREATE INDEX name_idx ON table1 USING GIST (to_tsvector('public.russian', "name"));
SELECT d.* FROM table1 AS t, LATERAL ts_debug('public.russian', t.name) AS d;