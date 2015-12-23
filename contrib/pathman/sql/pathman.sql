CREATE EXTENSION pathman;

CREATE TABLE hash_rel (
    id      SERIAL PRIMARY KEY,
    value   INTEGER);

SELECT create_hash_partitions('hash_rel', 'value', 3);

INSERT INTO hash_rel VALUES (1, 1);
INSERT INTO hash_rel VALUES (2, 2);
INSERT INTO hash_rel VALUES (3, 3);
INSERT INTO hash_rel VALUES (4, 4);
INSERT INTO hash_rel VALUES (5, 5);
INSERT INTO hash_rel VALUES (6, 6);

EXPLAIN SELECT * FROM hash_rel;
EXPLAIN SELECT * FROM hash_rel WHERE value = 2;

SELECT drop_hash_partitions('hash_rel');
DROP TABLE hash_rel CASCADE;

CREATE TABLE num_range_rel (
    id SERIAL PRIMARY KEY,
    txt TEXT);
SELECT create_range_partitions('num_range_rel', 'id', 'num', '0', '1000', 3);
INSERT INTO num_range_rel SELECT g, md5(g::TEXT) FROM generate_series(1, 3000) as g;
VACUUM;
EXPLAIN SELECT * FROM num_range_rel WHERE id > 2500;

SELECT drop_range_partitions('num_range_rel');
DROP TABLE num_range_rel CASCADE;

DROP EXTENSION pathman;
