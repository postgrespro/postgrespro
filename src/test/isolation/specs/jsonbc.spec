setup
{
	CREATE TABLE foo (
		bar jsonb compressed jsonbc
	);
}

teardown
{
	DROP TABLE foo;
}

session "s1"
setup		{ BEGIN; SET deadlock_timeout = '100ms'; }
step "s1rc"	{ SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s1sr"	{ SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step "s1ia"	{ INSERT INTO foo VALUES (jsonb_build_object('a', 1)); }
step "s1ib"	{ INSERT INTO foo VALUES (jsonb_build_object('b', 1)); }
step "s1ic"	{ INSERT INTO foo VALUES (jsonb_build_object('a', 1, 'b', 1, 'c', jsonb_build_object('a', 1))); }
step "s1sf"	{ SELECT * FROM foo; }
step "s1sd"	{ SELECT id, name FROM pg_jsonbc_dict; }
step "s1ci"	{ COMMIT; }
step "s1rb"	{ ROLLBACK; }

session "s2"
setup		{ BEGIN; SET deadlock_timeout = '100ms'; }
step "s2rc"	{ SET TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step "s2sr"	{ SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; }
step "s2ia"	{ INSERT INTO foo VALUES (jsonb_build_object('a', 2)); }
step "s2ib"	{ INSERT INTO foo VALUES (jsonb_build_object('b', 2)); }
step "s2ic"	{ INSERT INTO foo VALUES (jsonb_build_object('a', 2, 'b', 2, 'c', jsonb_build_object('a', 2))); }
step "s2sf"	{ SELECT * FROM foo; }
step "s2sd"	{ SELECT id, name FROM pg_jsonbc_dict; }
step "s2ci"	{ COMMIT; }
step "s2rb"	{ ROLLBACK; }

permutation "s1rc" "s2rc" "s1ia" "s2ia" "s1sd" "s1sf" "s2sd" "s2sf" "s1ci" "s2ci" "s1sd" "s1sf"   
permutation "s1rc" "s2rc" "s1ia" "s2ia" "s1ci" "s2rb" "s1sd" "s1sf"
permutation "s1rc" "s2rc" "s1ia" "s2ia" "s1rb" "s2ci" "s1sd" "s1sf"
permutation "s1rc" "s2rc" "s1ia" "s2ia" "s1rb" "s2rb" "s1sd" "s1sf"
permutation "s1rc" "s2rc" "s1ia" "s2ia" "s2ci" "s1ci" "s1sd" "s1sf"   
permutation "s1rc" "s2rc" "s1ia" "s2ia" "s2ci" "s1rb" "s1sd" "s1sf"
permutation "s1rc" "s2rc" "s1ia" "s2ia" "s2rb" "s1ci" "s1sd" "s1sf"
permutation "s1rc" "s2rc" "s1ia" "s2ia" "s2rb" "s1rb" "s1sd" "s1sf"

permutation "s1sr" "s2sr" "s1ia" "s2ia" "s1sd" "s1sf" "s1ci" "s2ci" "s1sd" "s1sf"   
permutation "s1sr" "s2sr" "s1ia" "s2ia" "s1ci" "s2rb" "s1sd" "s1sf"
permutation "s1sr" "s2sr" "s1ia" "s2ia" "s1rb" "s2ci" "s1sd" "s1sf"
permutation "s1sr" "s2sr" "s1ia" "s2ia" "s1rb" "s2rb" "s1sd" "s1sf"
permutation "s1sr" "s2sr" "s1ia" "s2ia" "s2ci" "s1ci" "s1sd" "s1sf"   
permutation "s1sr" "s2sr" "s1ia" "s2ia" "s2ci" "s1rb" "s1sd" "s1sf"
permutation "s1sr" "s2sr" "s1ia" "s2ia" "s2rb" "s1ci" "s1sd" "s1sf"
permutation "s1sr" "s2sr" "s1ia" "s2ia" "s2rb" "s1rb" "s1sd" "s1sf"

permutation "s1sr" "s2sr" "s1ia" "s1ia" "s1ib" "s1ic" "s2ib" "s2ia" "s2ib" "s1ci" "s2ci" "s1sd" "s1sf"
permutation "s1sr" "s2sr" "s1ic" "s1ic" "s1ia" "s2ic" "s2ib" "s1ci" "s2ci" "s1sd" "s1sf"
