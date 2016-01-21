/*
 * Creates hash partitions for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.create_hash_partitions(
    relation TEXT
    , attribute TEXT
    , partitions_count INTEGER
) RETURNS VOID AS
$$
BEGIN
    relation := @extschema@.validate_relname(relation);

    IF EXISTS (SELECT * FROM @extschema@.pathman_config WHERE relname = relation) THEN
        RAISE EXCEPTION 'Reltion "%s" has already been partitioned', relation;
    END IF;

    /* Create partitions and update pg_pathman configuration */
    FOR partnum IN 0..partitions_count-1
    LOOP
        EXECUTE format('CREATE TABLE %s_%s (LIKE %1$s INCLUDING ALL)'
                        , relation
                        , partnum);

        EXECUTE format('ALTER TABLE %s_%s INHERIT %1$s'
                        , relation
                        , partnum);

        EXECUTE format('ALTER TABLE %s_%s ADD CHECK (%s %% %s = %s)'
                       , relation
                       , partnum
                       , attribute
                       , partitions_count
                       , partnum);
    END LOOP;
    INSERT INTO @extschema@.pathman_config (relname, attname, parttype)
    VALUES (relation, attribute, 1);

    /* Create triggers */
    PERFORM @extschema@.create_hash_insert_trigger(relation, attribute, partitions_count);
    /* TODO: вернуть */
    -- PERFORM create_hash_update_trigger(relation, attribute, partitions_count);

    /* Notify backend about changes */
    PERFORM @extschema@.on_create_partitions(relation::regclass::oid);
END
$$ LANGUAGE plpgsql;

/*
 * Creates hash trigger for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.create_hash_insert_trigger(
    IN relation TEXT
    , IN attr TEXT
    , IN partitions_count INTEGER)
RETURNS VOID AS
$$
DECLARE
    func TEXT := '
        CREATE OR REPLACE FUNCTION %s_hash_insert_trigger_func()
        RETURNS TRIGGER AS $body$
        DECLARE
            hash INTEGER;
        BEGIN
            hash := NEW.%s %% %s;
            %s
            RETURN NULL;
        END $body$ LANGUAGE plpgsql;';
    trigger TEXT := '
        CREATE TRIGGER %s_insert_trigger
        BEFORE INSERT ON %s
        FOR EACH ROW EXECUTE PROCEDURE %2$s_hash_insert_trigger_func();';
    fields TEXT;
    fields_format TEXT;
    insert_stmt TEXT;
BEGIN
    /* drop trigger and corresponding function */
    PERFORM @extschema@.drop_hash_triggers(relation);

    /* determine fields for INSERT */
    SELECT string_agg('NEW.' || attname, ', '), string_agg('$' || attnum, ', ')
    FROM pg_attribute
    WHERE attrelid=relation::regclass::oid AND attnum>0
    INTO fields, fields_format;

    /* generate INSERT statement for trigger */
    insert_stmt = format('EXECUTE format(''INSERT INTO %s_%%s VALUES (%s)'', hash) USING %s;'
                         , relation
                         , fields_format
                         , fields);

    /* format and create new trigger for relation */
    func := format(func, relation, attr, partitions_count, insert_stmt);
    trigger := format(trigger, @extschema@.get_schema_qualified_name(relation::regclass), relation);
    EXECUTE func;
    EXECUTE trigger;
END
$$ LANGUAGE plpgsql;

/*
 * Drops all partitions for specified relation
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_hash_partitions(IN relation TEXT)
RETURNS VOID AS
$$
DECLARE
    relid INTEGER;
    partitions_count INTEGER;
    q TEXT := 'DROP TABLE %s_%s';
BEGIN
    /* Drop trigger first */
    PERFORM @extschema@.drop_hash_triggers(relation);
    DELETE FROM @extschema@.pathman_config WHERE relname = relation;
    -- EXECUTE format('DROP TABLE %s CASCADE', relation);

    /* Notify backend about changes */
    PERFORM @extschema@.on_remove_partitions(relation::regclass::oid);
END
$$ LANGUAGE plpgsql;

/*
 * Drops hash trigger
 */
CREATE OR REPLACE FUNCTION @extschema@.drop_hash_triggers(IN relation TEXT)
RETURNS VOID AS
$$
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS %s_insert_trigger ON %s'
                   , @extschema@.get_schema_qualified_name(relation::regclass)
                   , relation);
    EXECUTE format('DROP FUNCTION IF EXISTS %s_hash_insert_trigger_func()', relation::regclass::text);
    -- EXECUTE format('DROP TRIGGER IF EXISTS %s_update_trigger ON %1$s', relation);
    -- EXECUTE format('DROP FUNCTION IF EXISTS %s_hash_update_trigger_func()', relation);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION @extschema@.create_hash_update_trigger(
    IN relation TEXT
    , IN attr TEXT
    , IN partitions_count INTEGER)
RETURNS VOID AS
$$
DECLARE
    func TEXT := '
        CREATE OR REPLACE FUNCTION %s_update_trigger_func()
        RETURNS TRIGGER AS
        $body$
        DECLARE old_hash INTEGER; new_hash INTEGER; q TEXT;
        BEGIN
            old_hash := OLD.%2$s %% %3$s;
            new_hash := NEW.%2$s %% %3$s;
            IF old_hash = new_hash THEN RETURN NEW; END IF;
            q := format(''DELETE FROM %1$s_%%s WHERE %4$s'', old_hash);
            EXECUTE q USING %5$s;
            q := format(''INSERT INTO %1$s_%%s VALUES (%6$s)'', new_hash);
            EXECUTE q USING %7$s;
            RETURN NULL;
        END $body$ LANGUAGE plpgsql';
    trigger TEXT := 'CREATE TRIGGER %s_update_trigger ' ||  
        'BEFORE UPDATE ON %1$s_%s ' ||
        'FOR EACH ROW EXECUTE PROCEDURE %1$s_update_trigger_func()';
    att_names   TEXT;
    old_fields  TEXT;
    new_fields  TEXT;
    att_val_fmt TEXT;
    att_fmt     TEXT;
    relid       INTEGER;
BEGIN
    relid := relfilenode FROM pg_class WHERE relname = relation;
    SELECT string_agg(attname, ', '),
           string_agg('OLD.' || attname, ', '),
           string_agg('NEW.' || attname, ', '),
           string_agg(attname || '=$' || attnum, ' AND '),
           string_agg('$' || attnum, ', ')
    FROM pg_attribute
    WHERE attrelid=relid AND attnum>0
    INTO   att_names,
           old_fields,
           new_fields,
           att_val_fmt,
           att_fmt;

    EXECUTE format(func, relation, attr, partitions_count, att_val_fmt,
                   old_fields, att_fmt, new_fields);
    FOR num IN 0..partitions_count-1
    LOOP
        EXECUTE format(trigger, relation, num);
    END LOOP;
END
$$ LANGUAGE plpgsql;
