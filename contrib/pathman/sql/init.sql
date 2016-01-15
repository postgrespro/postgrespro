/*
 * Relations using partitioning
 */
CREATE TABLE IF NOT EXISTS @extschema@.pg_pathman_rels (
    id         SERIAL PRIMARY KEY,
    relname    VARCHAR(127),
    attname    VARCHAR(127),
    parttype   INTEGER
);

/*
 * Relations using hash strategy
 */
-- CREATE TABLE IF NOT EXISTS @extschema@.pg_pathman_hash_rels (
--     id         SERIAL PRIMARY KEY,
--     parent     VARCHAR(127),
--     hash       INTEGER,
--     child      VARCHAR(127)
-- );

/*
 * Relations using range strategy
 */
-- CREATE TABLE IF NOT EXISTS @extschema@.pg_pathman_range_rels (
--     id         SERIAL PRIMARY KEY,
--     parent     VARCHAR(127),
--     min_num    DOUBLE PRECISION,
--     max_num    DOUBLE PRECISION,
--     min_dt     TIMESTAMP,
--     max_dt     TIMESTAMP,
--     child      VARCHAR(127) 
-- );

CREATE OR REPLACE FUNCTION pg_pathman_on_create_partitions(relid INTEGER)
RETURNS VOID AS 'pathman', 'on_partitions_created' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION pg_pathman_on_update_partitions(relid INTEGER)
RETURNS VOID AS 'pathman', 'on_partitions_updated' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION pg_pathman_on_remove_partitions(relid INTEGER)
RETURNS VOID AS 'pathman', 'on_partitions_removed' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION find_range_partition(relid OID, value ANYELEMENT)
RETURNS OID AS 'pathman', 'find_range_partition' LANGUAGE C STRICT;

/*
 * Returns min and max values for specified RANGE partition.
 */
CREATE OR REPLACE FUNCTION get_partition_range(
    parent_relid OID, partition_relid OID, dummy ANYELEMENT)
RETURNS ANYARRAY AS 'pathman', 'get_partition_range' LANGUAGE C STRICT;

/*
 * Returns N-th range (in form of array)
 */
CREATE OR REPLACE FUNCTION get_range_by_idx(
    parent_relid OID, idx INTEGER, dummy ANYELEMENT)
RETURNS ANYARRAY AS 'pathman', 'get_range_by_idx' LANGUAGE C STRICT;

/*
 * Copy rows to partitions
 */
CREATE OR REPLACE FUNCTION partition_data(p_parent text)
RETURNS bigint AS
$$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN  (SELECT child.relname, pg_constraint.consrc
                 FROM pg_pathman_rels
                 JOIN pg_class AS parent ON parent.relname = pg_pathman_rels.relname
                 JOIN pg_inherits ON inhparent = parent.relfilenode
                 JOIN pg_constraint ON conrelid = inhrelid AND contype='c'
                 JOIN pg_class AS child ON child.relfilenode = inhrelid
                 WHERE pg_pathman_rels.relname = p_parent)
    LOOP
        RAISE NOTICE 'Copying data to % (condition: %)', rec.relname, rec.consrc;
        EXECUTE format('WITH part_data AS (
                            DELETE FROM ONLY %s WHERE %s RETURNING *)
                        INSERT INTO %s SELECT * FROM part_data'
                        , p_parent
                        , rec.consrc
                        , rec.relname);
    END LOOP;
    RETURN 0;
END
$$ LANGUAGE plpgsql;


/*
 * Disable pathman partitioning for specified relation
 */
CREATE OR REPLACE FUNCTION disable_partitioning(IN relation TEXT)
RETURNS VOID AS
$$
BEGIN
    DELETE FROM pg_pathman_rels WHERE relname = relation;

    /* Notify backend about changes */
    PERFORM pg_pathman_on_remove_partitions(relation::regclass::integer);
END
$$ LANGUAGE plpgsql;


/*
 * Returns attribute type name for relation
 */
CREATE OR REPLACE FUNCTION get_attribute_type_name(
    p_relation TEXT
    , p_attname TEXT
    , OUT p_atttype TEXT)
RETURNS TEXT AS
$$
BEGIN
    SELECT typname::TEXT INTO p_atttype
    FROM pg_type JOIN pg_attribute on atttypid = "oid"
    WHERE attrelid = p_relation::regclass::oid and attname = lower(p_attname);
END
$$
LANGUAGE plpgsql;


-- CREATE OR REPLACE FUNCTION sample_rel_trigger_func()
-- RETURNS TRIGGER AS $$
-- DECLARE
--  hash integer := 0;
--  -- q TEXT = 'INSERT INTO sample_rel_% VALUES (NEW.*)';
-- BEGIN
--  hash := NEW.val % 1000;
--  EXECUTE format('INSERT INTO sample_rel_%s VALUES ($1, $2)', hash)
--      USING NEW.id, NEW.val;
--  RETURN NULL;
-- END
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER sample_rel_trigger
--  BEFORE INSERT ON sample_rel
--  FOR EACH ROW EXECUTE PROCEDURE sample_rel_trigger_func();



/* INHERITANCE TEST */
-- CREATE OR REPLACE FUNCTION public.create_children_tables(IN relation TEXT)
-- RETURNS INTEGER AS $$
-- DECLARE
--  q TEXT := 'CREATE TABLE %s_%s (CHECK (val IN (%s))) INHERITS (%s)';
-- BEGIN
--  FOR partnum IN 0..999
--  LOOP
--      EXECUTE format(q, relation, partnum, partnum, relation);
--  END LOOP;
--  RETURN 0;
-- END
-- $$ LANGUAGE plpgsql;

/* sample data */
-- insert into pg_pathman_rels (oid, attnum, parttype) values (49350, 2, 1);
-- insert into pg_pathman_hash_rels (parent_oid, hash, child_oid) values (49350, 1, 49355);
-- insert into pg_pathman_hash_rels (parent_oid, hash, child_oid) values (49350, 0, 49360);
