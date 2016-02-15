setup
{
	--SELECT '>>> SETUP <<<';
	CREATE EXTENSION pg_pathman;
	CREATE TABLE range_rel(id serial primary key);
	SELECT create_range_partitions('range_rel', 'id', 1, 100, 1);
}

teardown
{
	--SELECT '>>> TEARDOWN <<<';
	--SELECT drop_range_partitions('range_rel');
	DROP TABLE range_rel CASCADE;
	DROP EXTENSION pg_pathman;
}

session "s1"
step "s1i" { INSERT INTO range_rel SELECT generate_series(1, 150); }
step "s1d" { SELECT * FROM pg_inherits WHERE inhparent = 'range_rel'::regclass::oid; }

session "s2"
step "s2i" { INSERT INTO range_rel SELECT generate_series(151, 300); }
