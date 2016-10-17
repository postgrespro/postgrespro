CREATE EXTENSION lz;

-- Create tables

CREATE TEMP TABLE invalid_datatype_for_lz_compression(i int compressed zstd);

---- zstd
CREATE TEMP TABLE zstd_invalid_clevel(js json compressed zstd with (clevel 'abc'));
CREATE TEMP TABLE zstd_invalid_option(js json compressed zstd with (invalid_option 'value'));

CREATE TEMP TABLE t_zstd(t text compressed zstd);
CREATE TEMP TABLE js_zstd(js json compressed zstd);
CREATE TEMP TABLE jb_zstd(jb json compressed zstd);
CREATE TEMP TABLE jb_zstd_10(js json compressed zstd with (clevel '10'));

---- lz4
CREATE TEMP TABLE lz4_invalid_clevel  (js json compressed lz4 with (clevel 'aaa'));
CREATE TEMP TABLE lz4_invalid_option  (js json compressed lz4 with (invalid_option 'value'));
CREATE TEMP TABLE lz4hc_invalid_clevel(js json compressed lz4hc with (clevel 'aaa'));
CREATE TEMP TABLE lz4hc_invalid_option(js json compressed lz4hc with (invalid_option 'value'));
CREATE TEMP TABLE lz4d_invalid_clevel (js json compressed lz4 with (clevel 'aaa'));
CREATE TEMP TABLE lz4d_invalid_dict   (js json compressed lz4 with (dict 'aaa'));
CREATE TEMP TABLE lz4d_invalid_option (js json compressed lz4 with (invalid_option 'value'));

CREATE TEMP TABLE jb_lz4     (jb json compressed lz4);
CREATE TEMP TABLE jb_lz4_5   (jb json compressed lz4 with (clevel '5'));
CREATE TEMP TABLE jb_lz4hc   (jb json compressed lz4hc);
CREATE TEMP TABLE jb_lz4hc_16(jb json compressed lz4hc with (clevel '16'));

CREATE TEMP TABLE jb_lz4d_no_dict(jb json compressed lz4d);

SELECT NULL FROM lz4_dictionary_create('test_json_dict');
SELECT NULL FROM lz4_dictionary_create('test_jsonb_dict');

CREATE TEMP TABLE jb_lz4d(jb json compressed lz4d with (dict 'test_jsonb_dict'));
CREATE TEMP TABLE jb_lz4d_5(jb json compressed lz4d with (dict 'test_jsonb_dict', clevel '5'));

---- snappy
CREATE TEMP TABLE snappy_invalid_clevel(js json compressed snappy with (clevel '1'));
CREATE TEMP TABLE snappy_invalid_option(js json compressed snappy with (invalid_option 'value'));

CREATE TEMP TABLE js_snappy(js json compressed snappy);
CREATE TEMP TABLE jb_snappy(jb jsonb compressed snappy);

-- Insert data
CREATE TEMP TABLE lz_data(js text);

INSERT INTO lz_data
SELECT
	'{
		"id": ' || i || ',
		"string": "value' || i || '",
		"array": [' || i ||
				repeat(', 12345, ' || i || ', "str 12345 abcdef aaaaaaaaaa '|| i || '"', 20) || 
				'],
		"object": { "key": "value' || i || '" }
	}'
FROM generate_series(1, 10000) i;

SELECT lz4_dictionary_add_sample('test_jsonb_dict', js) FROM lz_data LIMIT 1;

DO
$$
DECLARE
	tab text[];
	tabs text[][] := array[
		array['t_zstd',      'text'],
		array['js_zstd',     'json'],
		array['jb_zstd',     'jsonb'],
		array['jb_zstd_10',  'jsonb'],
		array['jb_lz4',      'jsonb'],
		array['jb_lz4_5',    'jsonb'],
		array['jb_lz4hc',    'jsonb'],
		array['jb_lz4hc_16', 'jsonb'],
		array['jb_lz4d',     'jsonb'],
		array['jb_lz4d_5',   'jsonb'],
		array['js_snappy',   'json'],
		array['jb_snappy',   'jsonb']
	];
	tab_size bigint;
BEGIN
	FOREACH tab SLICE 1 IN ARRAY tabs
	LOOP
		EXECUTE 'INSERT INTO ' || tab[1] ||
				' SELECT js::' || tab[2] ||
				' FROM lz_data';
		SELECT INTO tab_size pg_relation_size(tab[1]);
		RAISE NOTICE '%: %', tab[1], pg_size_pretty(tab_size);
	END LOOP;
END
$$;
