\set VERBOSITY terse

CREATE EXTENSION pathman;

CREATE TABLE hash_rel (
    id      SERIAL PRIMARY KEY,
    value   INTEGER);
SELECT create_hash_partitions('hash_rel', 'value', 3);

CREATE TABLE range_rel (
    id SERIAL PRIMARY KEY,
    dt TIMESTAMP,
    txt TEXT);
SELECT create_range_partitions('range_rel', 'dt', '2015-01-01'::DATE, '1 month'::INTERVAL, 3);

CREATE TABLE num_range_rel (
    id SERIAL PRIMARY KEY,
    txt TEXT);
SELECT create_range_partitions('num_range_rel', 'id', 0, 1000, 3);

INSERT INTO hash_rel VALUES (1, 1);
INSERT INTO hash_rel VALUES (2, 2);
INSERT INTO hash_rel VALUES (3, 3);
INSERT INTO hash_rel VALUES (4, 4);
INSERT INTO hash_rel VALUES (5, 5);
INSERT INTO hash_rel VALUES (6, 6);

INSERT INTO num_range_rel SELECT g, md5(g::TEXT) FROM generate_series(1, 3000) as g;
VACUUM;

SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_seqscan = ON;

EXPLAIN (COSTS OFF) SELECT * FROM hash_rel;
EXPLAIN (COSTS OFF) SELECT * FROM hash_rel WHERE value = 2;
EXPLAIN (COSTS OFF) SELECT * FROM hash_rel WHERE value = 2 OR value = 1;
EXPLAIN (COSTS OFF) SELECT * FROM num_range_rel WHERE id > 2500;
EXPLAIN (COSTS OFF) SELECT * FROM num_range_rel WHERE id >= 1000 AND id < 3000;
EXPLAIN (COSTS OFF) SELECT * FROM num_range_rel WHERE id >= 1500 AND id < 2500;
EXPLAIN (COSTS OFF) SELECT * FROM num_range_rel WHERE (id >= 500 AND id < 1500) OR (id > 2500);

SET enable_indexscan = ON;
SET enable_bitmapscan = OFF;
SET enable_seqscan = OFF;

EXPLAIN (COSTS OFF) SELECT * FROM hash_rel;
EXPLAIN (COSTS OFF) SELECT * FROM hash_rel WHERE value = 2;
EXPLAIN (COSTS OFF) SELECT * FROM hash_rel WHERE value = 2 OR value = 1;
EXPLAIN (COSTS OFF) SELECT * FROM num_range_rel WHERE id > 2500;
EXPLAIN (COSTS OFF) SELECT * FROM num_range_rel WHERE id >= 1000 AND id < 3000;
EXPLAIN (COSTS OFF) SELECT * FROM num_range_rel WHERE id >= 1500 AND id < 2500;
EXPLAIN (COSTS OFF) SELECT * FROM num_range_rel WHERE (id >= 500 AND id < 1500) OR (id > 2500);

/*
 * Test split and merge
 */

/* Split first partition in half */
SELECT split_range_partition('num_range_rel_1', 500);
EXPLAIN (COSTS OFF) SELECT * FROM num_range_rel WHERE id BETWEEN 100 AND 700;

SELECT split_range_partition('range_rel_1', '2015-01-15'::DATE);

/* Merge two partitions into one */
SELECT merge_range_partitions('num_range_rel_1', 'num_range_rel_' || currval('num_range_rel_seq'));
EXPLAIN (COSTS OFF) SELECT * FROM num_range_rel WHERE id BETWEEN 100 AND 700;

SELECT merge_range_partitions('range_rel_1', 'range_rel_' || currval('range_rel_seq'));

/* Append and prepend partitions */
SELECT append_partition('num_range_rel');
SELECT prepend_partition('num_range_rel');

SELECT append_partition('range_rel');
SELECT prepend_partition('range_rel');

/*
 * Clean up
 */
SELECT drop_hash_partitions('hash_rel');
DROP TABLE hash_rel CASCADE;

SELECT drop_range_partitions('num_range_rel');
DROP TABLE num_range_rel CASCADE;

DROP EXTENSION pathman;
