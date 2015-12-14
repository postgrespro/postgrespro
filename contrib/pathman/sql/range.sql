/*
 * Creates RANGE partitions for specified relation
 */
CREATE OR REPLACE FUNCTION create_range_partitions(
    v_relation TEXT
    , v_attribute TEXT
    , v_attribute_type TEXT
    , v_start_value TEXT
    , v_interval TEXT
    , v_premake INTEGER)
RETURNS VOID AS
$$
DECLARE
    v_relid     INTEGER;
    v_value     TEXT;
BEGIN
    SELECT relfilenode INTO v_relid
    FROM pg_class WHERE relname = v_relation;

    IF EXISTS (SELECT * FROM pg_pathman_rels WHERE relname = v_relation) THEN
        RAISE EXCEPTION 'Reltion "%s" has already been partitioned', v_relation;
    END IF;

    IF NOT v_start_value IS NULL THEN
        v_value := v_start_value;
    ELSE
        IF v_attribute_type = 'time' THEN
            SELECT current_date::TEXT INTO v_value;
        ELSIF v_attribute_type = 'num' THEN
            v_value := '0';
        ELSE
            RAISE EXCEPTION 'Only ''time'' and ''num'' attribute types are supported';
        END IF;
    END IF;

    INSERT INTO pg_pathman_rels (
        relname
        , attname
        , parttype)
    VALUES (
        v_relation
        , v_attribute
        , 2);

    /* create first partition */
    PERFORM create_single_range_partition(v_relation
                                          , v_attribute_type
                                          , v_value
                                          , v_interval);

    /* premake further partitions */
    PERFORM append_range_partitions_internal(v_relation, v_interval, v_premake);

    /* Create triggers */
    PERFORM create_range_insert_trigger(v_relation, v_attribute, v_attribute_type);
    -- PERFORM create_hash_update_trigger(relation, attribute, partitions_count);
    /* Notify backend about changes */
    PERFORM pg_pathman_on_create_partitions(v_relid);
END
$$ LANGUAGE plpgsql;

/*
 * Create additional partitions for existing RANGE partitioning
 */
CREATE OR REPLACE FUNCTION append_range_partitions(
    v_relation TEXT
    , v_interval TEXT
    , v_premake INTEGER)
RETURNS VOID AS
$$
DECLARE
    v_attribute TEXT;
    v_type TEXT;
    v_dt TIMESTAMP;
    v_num DOUBLE PRECISION;
    v_relid INTEGER;
BEGIN
    /* get an attribute name config */
    v_attribute := attname FROM pg_pathman_rels WHERE relname = v_relation;
    RAISE NOTICE 'v_attribute = %', v_attribute;

    /* get relation oid */
    v_relid := relfilenode FROM pg_class WHERE relname = v_relation;

    /* get range type: time or numeral */
    SELECT max(max_dt), max(max_num) INTO v_dt, v_num
    FROM pg_pathman_range_rels WHERE parent = v_relation;
    IF NOT v_dt IS NULL THEN
        v_type := 'time';
    ELSIF NOT v_num IS NULL THEN
        v_type := 'num';
    END IF;

    /* create partitions */
    PERFORM append_range_partitions_internal(v_relation, v_interval, v_premake);

    /* recreate triggers */
    PERFORM drop_range_triggers(v_relation);
    PERFORM create_range_insert_trigger(v_relation, v_attribute, v_type);

    PERFORM pg_pathman_on_update_partitions(v_relid);
END
$$ LANGUAGE plpgsql;

/*
 * Create additional partitions for existing RANGE partitioning
 * (function for internal use)
 */
CREATE OR REPLACE FUNCTION append_range_partitions_internal(
    v_relation TEXT
    , v_interval TEXT
    , v_premake INTEGER)
RETURNS VOID AS
$$
DECLARE
    v_part_timestamp TIMESTAMP;
    v_part_num DOUBLE PRECISION;
    v_type TEXT;
    i INTEGER;
BEGIN
    SELECT max(max_dt), max(max_num)
    INTO v_part_timestamp, v_part_num
    FROM pg_pathman_range_rels
    WHERE parent = v_relation;

    /* Create partitions and update pg_pathman configuration */
    if NOT v_part_timestamp IS NULL THEN
        FOR i IN 0..v_premake-1
        LOOP
            PERFORM create_single_range_partition(v_relation
                                                  , 'time'
                                                  , v_part_timestamp::TEXT
                                                  , v_interval);
            v_part_timestamp := v_part_timestamp + v_interval::INTERVAL;
        END LOOP;
    ELSIF NOT v_part_num IS NULL THEN
        /* Numerical range partitioning */
        FOR i IN 0..v_premake-1
        LOOP
            PERFORM create_single_range_partition(v_relation
                                                  , 'num'
                                                  , v_part_num::TEXT
                                                  , v_interval);
            v_part_num := v_part_num + v_interval::DOUBLE PRECISION;
        END LOOP;
    END IF;
END
$$ LANGUAGE plpgsql;

/*
 *
 */
CREATE OR REPLACE FUNCTION create_single_range_partition(
    v_parent_relname TEXT
    , v_type TEXT
    , v_start_value TEXT
    , v_interval TEXT)
RETURNS VOID AS
$$
DECLARE
    v_child_relname TEXT;
    v_check TEXT;
    v_min_value TEXT;
    v_max_value TEXT;
    v_attname TEXT;
BEGIN
    v_attname := attname FROM pg_pathman_rels
                 WHERE relname = v_parent_relname;

    IF v_type = 'time' THEN
        v_child_relname := format('%s_%s'
                                 , v_parent_relname
                                 , to_char(v_start_value::TIMESTAMP, 'YYYY_MM_DD_HH24'));
    ELSIF v_type = 'num' THEN
        v_child_relname := format('%s_%s'
                                 , v_parent_relname
                                 , v_start_value);
    END IF;

    /* Skip existing partitions */
    IF EXISTS (SELECT * FROM pg_tables WHERE tablename = v_child_relname) THEN
        RAISE WARNING 'Relation % already exists, skipping...', v_child_relname;
        RETURN;
    END IF;

    EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING ALL)'
                   , v_child_relname
                   , v_parent_relname);

    EXECUTE format('ALTER TABLE %s INHERIT %s'
                   , v_child_relname
                   , v_parent_relname);

    IF v_type = 'time' THEN
        INSERT INTO pg_pathman_range_rels (parent, min_dt, max_dt, child)
        VALUES (v_parent_relname
                , v_start_value::TIMESTAMP
                , v_start_value::TIMESTAMP + v_interval::INTERVAL
                , v_child_relname);
        v_min_value := to_char(v_start_value::TIMESTAMP, 'YYYY-MM-DD HH:MI:SS');
        v_max_value := to_char(v_start_value::TIMESTAMP + v_interval::INTERVAL, 'YYYY-MM-DD HH:MI:SS');
    ELSIF v_type = 'num' THEN
        INSERT INTO pg_pathman_range_rels (parent, min_num, max_num, child)
        VALUES (v_parent_relname
                , v_start_value::DOUBLE PRECISION
                , v_start_value::DOUBLE PRECISION + v_interval::DOUBLE PRECISION
                , v_child_relname);
        v_min_value := v_start_value::DOUBLE PRECISION;
        v_max_value := v_start_value::DOUBLE PRECISION + v_interval::DOUBLE PRECISION;
    END IF;

    EXECUTE format('ALTER TABLE %s ADD CHECK (%s >= ''%s'' AND %s < ''%s'')'
                   , v_child_relname
                   , v_attname
                   , v_min_value
                   , v_attname
                   , v_max_value);
END
$$ LANGUAGE plpgsql;


/*
 * Creates range partitioning insert trigger
 */
CREATE OR REPLACE FUNCTION create_range_insert_trigger(
    v_relation    TEXT
    , v_attname   TEXT
    , v_type      TEXT)
RETURNS VOID AS
$$
DECLARE
    v_func TEXT :=
           'CREATE OR REPLACE FUNCTION %s_range_insert_trigger_func()
            RETURNS TRIGGER
            AS $body$
            DECLARE
                v_partition_timestamp   timestamptz;
            BEGIN
            IF TG_OP = ''INSERT'' THEN
            ';
    v_trigger TEXT :=
           'CREATE TRIGGER %s_insert_trigger
            BEFORE INSERT ON %1$s
            FOR EACH ROW EXECUTE PROCEDURE %1$s_range_insert_trigger_func();';
    v_rec     RECORD;
    v_cnt     INTEGER := 0;
    v_min_value TEXT;
    v_max_value TEXT;
BEGIN
    v_func = format(v_func, v_relation);
    FOR v_rec IN SELECT * 
                 FROM pg_pathman_range_rels
                 WHERE parent = v_relation
                 ORDER BY min_dt, min_num DESC
    LOOP
        IF v_type = 'time' THEN
            v_min_value = to_char(v_rec.min_dt, 'YYYY-MM-DD HH:MI:SS');
            v_max_value = to_char(v_rec.max_dt, 'YYYY-MM-DD HH:MI:SS');
        ELSIF v_type = 'num' THEN
            v_min_value = v_rec.min_num;
            v_max_value = v_rec.max_num;
        END IF;
        v_func = v_func || format('
                %s NEW.%s >= ''%s'' AND NEW.%s < ''%s'' THEN 
                    INSERT INTO %s VALUES (NEW.*);'
                , CASE WHEN v_cnt = 0 THEN 'IF' ELSE 'ELSIF' END
                , v_attname
                , v_min_value
                , v_attname
                , v_max_value
                , v_rec.child);
        v_cnt := v_cnt + 1;
    END LOOP;
    v_func := v_func || '
            ELSE
                RAISE EXCEPTION ''ERROR: Cannot determine approprite partition'';
            END IF;
        END IF;
        RETURN NULL;
        END;
        $body$ LANGUAGE plpgsql;';

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

    FOR v_rec IN (SELECT * FROM pg_pathman_range_rels WHERE parent = relation)
    LOOP
        EXECUTE format('DROP TABLE %s', v_rec.child);
    END LOOP;

    DELETE FROM pg_pathman_rels WHERE relname = relation;
    DELETE FROM pg_pathman_range_rels WHERE parent = relation;

    /* Notify backend about changes */
    PERFORM pg_pathman_on_remove_partitions(v_relid);
END
$$ LANGUAGE plpgsql;

/*
 *
 */
CREATE OR REPLACE FUNCTION disable_range_partitions(IN relation TEXT)
RETURNS VOID AS
$$
DECLARE
    v_relid INTEGER;
    v_rec   RECORD;
BEGIN
    DELETE FROM pg_pathman_rels WHERE relname = relation;
    DELETE FROM pg_pathman_range_rels WHERE parent = relation;

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
