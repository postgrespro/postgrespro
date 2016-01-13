/*
 * Creates RANGE partitions for specified relation
 */
    -- , v_attribute_type TEXT
CREATE OR REPLACE FUNCTION create_range_partitions(
    v_relation TEXT
    , v_attribute TEXT
    , v_start_value ANYELEMENT
    , v_interval ANYELEMENT
    , v_premake INTEGER)
RETURNS VOID AS
$$
DECLARE
    v_relid     INTEGER;
    v_value     TEXT;
    i INTEGER;
BEGIN
    SELECT relfilenode INTO v_relid
    FROM pg_class WHERE relname = v_relation;

    IF EXISTS (SELECT * FROM pg_pathman_rels WHERE relname = v_relation) THEN
        RAISE EXCEPTION 'Reltion "%s" has already been partitioned', v_relation;
    END IF;

    EXECUTE format('DROP SEQUENCE IF EXISTS %s_seq', v_relation);
    EXECUTE format('CREATE SEQUENCE %s_seq START 1', v_relation);

    -- IF NOT v_start_value IS NULL THEN
    --     v_value := v_start_value;
    -- ELSE
    --     IF v_attribute_type = 'time' THEN
    --         SELECT current_date INTO v_value;
    --     ELSIF v_attribute_type = 'num' THEN
    --         v_value := 0;
    --     ELSE
    --         RAISE EXCEPTION 'Only ''time'' and ''num'' attribute types are supported';
    --     END IF;
    -- END IF;

    INSERT INTO pg_pathman_rels (
        relname
        , attname
        , parttype)
    VALUES (
        v_relation
        , v_attribute
        , 2);

    /* create first partition */
    FOR i IN 1..v_premake+1
    LOOP
        PERFORM create_single_range_partition(v_relation
                                              -- , v_attribute_type
                                              , v_start_value
                                              , v_interval);
        v_start_value := v_start_value + v_interval;
    END LOOP;

    /* premake further partitions */
    -- PERFORM append_range_partitions_internal(v_relation, v_interval, v_premake);

    /* Create triggers */
    PERFORM create_range_insert_trigger(v_relation, v_attribute);
    -- PERFORM create_hash_update_trigger(relation, attribute, partitions_count);
    /* Notify backend about changes */
    PERFORM pg_pathman_on_create_partitions(v_relid);
END
$$ LANGUAGE plpgsql;

/*
 * Create additional partitions for existing RANGE partitioning
 */
-- CREATE OR REPLACE FUNCTION append_range_partitions(
--     v_relation TEXT
--     , v_interval TEXT
--     , v_premake INTEGER)
-- RETURNS VOID AS
-- $$
-- DECLARE
--     v_attribute TEXT;
--     v_type TEXT;
--     v_dt TIMESTAMP;
--     v_num DOUBLE PRECISION;
--     v_relid INTEGER;
-- BEGIN
--     /* get an attribute name config */
--     v_attribute := attname FROM pg_pathman_rels WHERE relname = v_relation;
--     RAISE NOTICE 'v_attribute = %', v_attribute;

--     /* get relation oid */
--     v_relid := relfilenode FROM pg_class WHERE relname = v_relation;

--     /* get range type: time or numeral */
--     SELECT max(max_dt), max(max_num) INTO v_dt, v_num
--     FROM pg_pathman_range_rels WHERE parent = v_relation;
--     IF NOT v_dt IS NULL THEN
--         v_type := 'time';
--     ELSIF NOT v_num IS NULL THEN
--         v_type := 'num';
--     END IF;

--     /* create partitions */
--     PERFORM append_range_partitions_internal(v_relation, v_interval, v_premake);

--     /* recreate triggers */
--     PERFORM drop_range_triggers(v_relation);
--     PERFORM create_range_insert_trigger(v_relation, v_attribute, v_type);

--     PERFORM pg_pathman_on_update_partitions(v_relid);
-- END
-- $$ LANGUAGE plpgsql;

/*
 * Create additional partitions for existing RANGE partitioning
 * (function for internal use)
 */
-- CREATE OR REPLACE FUNCTION append_range_partitions_internal(
--     v_relation TEXT
--     , v_interval TEXT
--     , v_premake INTEGER)
-- RETURNS VOID AS
-- $$
-- DECLARE
--     v_part_timestamp TIMESTAMP;
--     v_part_num DOUBLE PRECISION;
--     v_type TEXT;
--     i INTEGER;
-- BEGIN
--     SELECT max(max_dt), max(max_num)
--     INTO v_part_timestamp, v_part_num
--     FROM pg_pathman_range_rels
--     WHERE parent = v_relation;

--     /* Create partitions and update pg_pathman configuration */
--     if NOT v_part_timestamp IS NULL THEN
--         FOR i IN 0..v_premake-1
--         LOOP
--             PERFORM create_single_range_partition(v_relation
--                                                   , 'time'
--                                                   , v_part_timestamp::TEXT
--                                                   , v_interval);
--             v_part_timestamp := v_part_timestamp + v_interval::INTERVAL;
--         END LOOP;
--     ELSIF NOT v_part_num IS NULL THEN
--         /* Numerical range partitioning */
--         FOR i IN 0..v_premake-1
--         LOOP
--             PERFORM create_single_range_partition(v_relation
--                                                   , 'num'
--                                                   , v_part_num::TEXT
--                                                   , v_interval);
--             v_part_num := v_part_num + v_interval::DOUBLE PRECISION;
--         END LOOP;
--     END IF;
-- END
-- $$ LANGUAGE plpgsql;

/*
 * Creates range condition. Utility function.
 */
CREATE OR REPLACE FUNCTION get_range_condition(
    p_attname TEXT
    , p_start_value ANYELEMENT
    , p_interval ANYELEMENT)
RETURNS TEXT AS
$$
DECLARE
    v_type TEXT;
    v_sql  TEXT;
BEGIN
    /* determine the type of values */
    v_type := lower(pg_typeof(p_start_value)::TEXT);

    /* we cannot use placeholders in DDL queries, so we are using format(...) */
    IF v_type IN ('date', 'timestamp', 'timestamptz') THEN
        v_sql := '%s >= ''%s'' AND %s < ''%s''';
    ELSE
        v_sql := '%s >= %s AND %s < %s';
    END IF;

    v_sql := format(v_sql
                    , p_attname
                    , p_start_value
                    , p_attname
                    , p_start_value + p_interval);
    RETURN v_sql;
END
$$
LANGUAGE plpgsql;

/*
 * Creates new RANGE partition. Returns partition name
 */
CREATE OR REPLACE FUNCTION create_single_range_partition(
    p_parent_relname TEXT
    , p_start_value ANYELEMENT
    , p_interval ANYELEMENT)
RETURNS TEXT AS
$$
DECLARE
    v_child_relname TEXT;
    v_attname TEXT;

    v_part_num INT;
    v_sql TEXT;
    -- v_type TEXT;
    v_cond TEXT;
BEGIN
    v_attname := attname FROM pg_pathman_rels
                 WHERE relname = p_parent_relname;

    /* get next value from sequence */
    v_part_num := nextval(format('%s_seq', p_parent_relname));
    v_child_relname := format('%s_%s', p_parent_relname, v_part_num);

    /* Skip existing partitions */
    IF EXISTS (SELECT * FROM pg_tables WHERE tablename = v_child_relname) THEN
        RAISE WARNING 'Relation % already exists, skipping...', v_child_relname;
        RETURN NULL;
    END IF;

    EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING ALL)'
                   , v_child_relname
                   , p_parent_relname);

    EXECUTE format('ALTER TABLE %s INHERIT %s'
                   , v_child_relname
                   , p_parent_relname);

    v_cond := get_range_condition(v_attname, p_start_value, p_interval);
    v_sql := format('ALTER TABLE %s ADD CHECK (%s)'
                  , v_child_relname
                  , v_cond);

    EXECUTE v_sql;
    RETURN v_child_relname;
END
$$ LANGUAGE plpgsql;


/*
 * Split RANGE partition
 */
CREATE OR REPLACE FUNCTION split_range_partition(
    p_partition TEXT
    , p_value ANYELEMENT
    , OUT p_range ANYARRAY)
RETURNS ANYARRAY AS
$$
DECLARE
    v_parent_relid OID;
    v_child_relid OID := p_partition::regclass::oid;
    v_atttype INT;
    v_attname TEXT;
    -- v_range ANYARRAY;
    -- v_min ANYELEMENT;
    -- v_max ANYELEMENT;
    v_cond TEXT;
    v_new_partition TEXT;
BEGIN
    v_parent_relid := inhparent
                      FROM pg_inherits
                      WHERE inhrelid = v_child_relid;

    SELECT attname INTO v_attname
    FROM pg_pathman_rels
    WHERE relname = v_parent_relid::regclass::text;

    /* Get partition values range */
    p_range := get_partition_range(v_parent_relid, v_child_relid);
    IF p_range IS NULL THEN
        RAISE EXCEPTION 'Could not find specified partition';
    END IF;
    RAISE NOTICE 'range: % - %', p_range[1], p_range[2];

    /* Check if value fit into the range */
    IF p_range[1] > p_value OR p_range[2] <= p_value
    THEN
        RAISE EXCEPTION 'Specified value does not fit into the range [%, %)',
            p_range[1], p_range[2];
    END IF;

    /* Create new partition */
    RAISE NOTICE 'Creating new partition...';
    v_new_partition := create_single_range_partition(v_parent_relid::regclass::text,
                                                     p_value,
                                                     p_range[2] - p_value);

    /* Copy data */
    RAISE NOTICE 'Copying data to new partition...';
    v_cond := get_range_condition(v_attname, p_value, p_range[2] - p_value);
    EXECUTE format('
                WITH part_data AS (
                    DELETE FROM %s WHERE %s RETURNING *)
                INSERT INTO %s SELECT * FROM part_data'
                , p_partition
                , v_cond
                , v_new_partition);

    /* Alter original partition */
    v_cond := get_range_condition(v_attname, p_range[1], p_value - p_range[1]);
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %s_%s_check'
                   , p_partition
                   , p_partition
                   , v_attname);
    EXECUTE format('ALTER TABLE %s ADD CHECK (%s)'
                   , p_partition
                   , v_cond);

    /* Tell backend to reload configuration */
    PERFORM pg_pathman_on_update_partitions(v_parent_relid::INTEGER);
END
$$
LANGUAGE plpgsql;


/*
 * Creates range partitioning insert trigger
 */
CREATE OR REPLACE FUNCTION create_range_insert_trigger(
    v_relation    TEXT
    , v_attname   TEXT)
RETURNS VOID AS
$$
DECLARE
    v_func TEXT := '
        CREATE OR REPLACE FUNCTION %s_range_insert_trigger_func()
        RETURNS TRIGGER
        AS $body$
        DECLARE
            v_part_relid OID;
        BEGIN
            IF TG_OP = ''INSERT'' THEN
                v_part_relid := find_range_partition(TG_RELID, NEW.%s);
                IF NOT v_part_relid IS NULL THEN
                    EXECUTE format(''INSERT INTO %%s SELECT $1.*'', v_part_relid::regclass)
                    USING NEW;
                ELSE
                    RAISE EXCEPTION ''ERROR: Cannot determine approprite partition'';
                END IF;
            END IF;
            RETURN NULL;
        END
        $body$ LANGUAGE plpgsql;';
    v_trigger TEXT := '
        CREATE TRIGGER %s_insert_trigger
        BEFORE INSERT ON %1$s
        FOR EACH ROW EXECUTE PROCEDURE %1$s_range_insert_trigger_func();';
BEGIN
    v_func := format(v_func, v_relation, v_attname);
    v_trigger := format(v_trigger, v_relation);

    EXECUTE v_func;
    EXECUTE v_trigger;
    RETURN;
END
$$ LANGUAGE plpgsql;


/*
 * Drop partitions
 */
CREATE OR REPLACE FUNCTION drop_range_partitions(IN relation TEXT)
RETURNS VOID AS
$$
DECLARE
    v_relid INTEGER;
    v_rec   RECORD;
BEGIN
    /* Drop trigger first */
    PERFORM drop_range_triggers(relation);

    v_relid := relfilenode FROM pg_class WHERE relname = relation;

    FOR v_rec IN (SELECT inhrelid::regclass AS tbl FROM pg_inherits WHERE inhparent = v_relid)
    LOOP
        EXECUTE format('DROP TABLE %s', v_rec.tbl);
    END LOOP;

    DELETE FROM pg_pathman_rels WHERE relname = relation;
    -- DELETE FROM pg_pathman_range_rels WHERE parent = relation;

    /* Notify backend about changes */
    PERFORM pg_pathman_on_remove_partitions(v_relid);
END
$$ LANGUAGE plpgsql;


/*
 * Drop trigger
 */
CREATE OR REPLACE FUNCTION drop_range_triggers(IN relation TEXT)
RETURNS VOID AS
$$
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS %s_insert_trigger ON %1$s CASCADE', relation);
END
$$ LANGUAGE plpgsql;
