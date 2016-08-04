CREATE COMPRESSION METHOD jsonb2 HANDLER no_such_handler;

CREATE COMPRESSION METHOD jsonb HANDLER jsonb_handler;

CREATE COMPRESSION METHOD jsonb2 HANDLER jsonb_handler;

CREATE TABLE jstest(
	js text COMPRESSED jsonb
);

CREATE TABLE jstest(
	js text COMPRESSED jsonb2
);

CREATE TABLE jstest(
	js1 json,
	js2 json COMPRESSED jsonb2,
	js3 json COMPRESSED jsonbc,
	js4 json COMPRESSED jsonbc,
	js5 jsonb
);

INSERT INTO jstest
SELECT js, js, js, js, js
FROM (VALUES
	('{"key1": "val1", "key2": ["val2", 3, 4, 5]}'::json),
	('["val1", 2, {"k1": "v1", "k2": 2}, "", 5, "6"]'),
	('"val"'),
	('12345'),
	(('[' || repeat('"test", ', 10000) || '"test"]')::json)
) AS jsvals(js);

SELECT
	substring(js1::text for 100),
	substring(js2::text for 100),
	substring(js3::text for 100),
	substring(js4::text for 100),
	substring(js5::text for 100)
FROM jstest;

-- check page items size
CREATE EXTENSION pageinspect;

SELECT
	lp, lp_off, lp_flags, lp_len,
	CASE lp WHEN 5 THEN NULL ELSE t_data END
FROM
	heap_page_items(get_raw_page('jstest', 0));

SELECT
	lp,
	length(unnest(t_attrs)),
	CASE lp WHEN 5 THEN NULL ELSE unnest(t_attrs) END
FROM
	heap_page_item_attrs(
		get_raw_page('jstest', 0),
		'jstest'::regclass,
		false
	);

DROP EXTENSION pageinspect;

-- copy json values with different compression

INSERT INTO jstest SELECT js1, js2, js3, js4, js5 FROM jstest LIMIT 5;
INSERT INTO jstest SELECT js2, js3, js4, js5, js1 FROM jstest LIMIT 5;
INSERT INTO jstest SELECT js3, js4, js5, js1, js2 FROM jstest LIMIT 5;
INSERT INTO jstest SELECT js4, js5, js1, js2, js3 FROM jstest LIMIT 5;
INSERT INTO jstest SELECT js5, js1, js2, js3, js4 FROM jstest LIMIT 5;

SELECT
	substring(js1::text for 100),
	substring(js2::text for 100),
	substring(js3::text for 100),
	substring(js4::text for 100),
	substring(js5::text for 100)
FROM jstest;



-- Try to drop compression method: fail because of dependent objects
DROP COMPRESSION METHOD jsonb2;

-- Drop compression method cascade
DROP COMPRESSION METHOD jsonb2 CASCADE;

SELECT * FROM jstest LIMIT 0;

DROP TABLE jstest;

-- Test ALTER TYPE SET COMPRESSED
ALTER TYPE json SET COMPRESSED jsonb;
CREATE TABLE jstest (js json);

SELECT attcompression FROM pg_attribute WHERE attrelid = 'jstest'::regclass AND attnum = 1;

INSERT INTO jstest VALUES ('[ 123,  "abc", { "k" : "v" }  ]');
SELECT * FROM jstest;
DROP TABLE jstest;

ALTER TYPE json SET NOT COMPRESSED;
CREATE TABLE jstest (js json);

SELECT attcompression FROM pg_attribute WHERE attrelid = 'jstest'::regclass AND attnum = 1;

INSERT INTO jstest VALUES ('[ 123,  "abc", { "k" : "v" }  ]');
SELECT * FROM jstest;
DROP TABLE jstest;

-- Test compressable type creation
CREATE TYPE json2;

CREATE TEMP TABLE json2_procs AS
SELECT * FROM pg_proc p WHERE proname IN ('json_in', 'json_out', 'json_null_cm_handler');

UPDATE json2_procs
SET proname = replace(proname, 'json_', 'json2_');

UPDATE json2_procs
SET prorettype = (SELECT oid FROM pg_type WHERE typname = 'json2')
WHERE proname = 'json2_in';

UPDATE json2_procs
SET proargtypes = (SELECT oid::text::oidvector FROM pg_type WHERE typname = 'json2')
WHERE proname = 'json2_out';

INSERT INTO pg_proc
SELECT * FROM json2_procs;

CREATE COMPRESSION METHOD json2_null HANDLER json2_null_cm_handler;

CREATE TYPE json2 (
	INPUT  = json2_in,
	OUTPUT = json2_out,
	NULLCM = json2_null
);

CREATE TEMP TABLE tjson2(js json2);
INSERT INTO tjson2 VALUES ('abc');
INSERT INTO tjson2 VALUES ('["abc", {"key": 123}, null]');
SELECT * FROM tjson2;

DROP FUNCTION json2_null_cm_handler(internal);
DROP FUNCTION json2_in(cstring);
DROP FUNCTION json2_out(json2);
DROP FUNCTION json2_out(json2) CASCADE;
DROP FUNCTION json2_in(cstring);
DROP FUNCTION json2_null_cm_handler(internal);
DROP FUNCTION json2_null_cm_handler(internal) CASCADE;

DROP TABLE tjson2;

-- Test compression methods on domains
CREATE DOMAIN json_not_null AS json NOT NULL;

CREATE TEMP TABLE json_domain_test1(js json_not_null);
SELECT attcompression FROM pg_attribute WHERE attrelid = 'json_domain_test1'::regclass AND attnum = 1;
DROP TABLE json_domain_test1;

CREATE TEMP TABLE json_domain_test2(js json_not_null compressed jsonb);
SELECT attcompression FROM pg_attribute WHERE attrelid = 'json_domain_test2'::regclass AND attnum = 1;
DROP TABLE json_domain_test2;

-- Test compression inheritance
create temp table json_parent1(js json);
create temp table json_parent2(js json compressed jsonb);
create temp table json_parent3(js json compressed jsonb);

create temp table json_child1(js json) inherits (json_parent1);
create temp table json_child2(js json) inherits (json_parent2);
create temp table json_child3(js json compressed jsonb) inherits (json_parent1);
create temp table json_child4(js json compressed jsonb) inherits (json_parent2);
create temp table json_child5(js json) inherits (json_parent1, json_parent2);
create temp table json_child6(js json) inherits (json_parent2, json_parent3);
create temp table json_child7(js json compressed jsonb) inherits (json_parent2, json_parent3);

SELECT a.attrelid::regclass, a.attnum, c.cmname
FROM pg_attribute a JOIN pg_compression c ON a.attcompression = c.oid
WHERE a.attrelid::regclass::text LIKE 'json_child%' AND a.attnum = 1;

drop table json_parent1 cascade;
drop table json_parent2 cascade;
drop table json_parent3 cascade;

