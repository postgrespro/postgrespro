#include "pathman.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"

#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "utils/typcache.h"


HTAB   *relations = NULL;
HTAB   *hash_restrictions = NULL;
HTAB   *range_restrictions = NULL;
bool	initialization_needed = true;


static bool validate_range_constraint(Expr *, PartRelationInfo *, Datum *, Datum *);
static int cmp_range_entries(const void *p1, const void *p2);


/*
 * Initialize hashtables
 */
void
init(void)
{
	initialization_needed = false;
	create_dsm_segment(32);

	LWLockAcquire(load_config_lock, LW_EXCLUSIVE);
	load_part_relations_hashtable();
	LWLockRelease(load_config_lock);
}

static bool
check_extension()
{
	SPI_exec("SELECT * FROM pg_extension WHERE extname = 'pathman'", 0);
	return SPI_processed > 0;
}

void
load_part_relations_hashtable()
{
	PartRelationInfo *prinfo;
	int ret;
	int i;
	int proc;
	bool isnull;
	List *part_oids = NIL;
	ListCell *lc;

	SPI_connect();

	/* if extension wasn't created then just quit */
	if (!check_extension())
	{
		SPI_finish();
		return;
	}

	ret = SPI_exec("SELECT pg_class.relfilenode, pg_attribute.attnum, pg_pathman_rels.parttype, pg_attribute.atttypid "
				   "FROM pg_pathman_rels "
				   "JOIN pg_class ON pg_class.relname = pg_pathman_rels.relname "
				   "JOIN pg_attribute ON pg_attribute.attname = pg_pathman_rels.attname "
				   "AND attrelid = pg_class.relfilenode", 0);
	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL)
	{
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable *tuptable = SPI_tuptable;

		for (i=0; i<proc; i++)
		{
			HeapTuple tuple = tuptable->vals[i];
			int oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &isnull));

			prinfo = (PartRelationInfo*)
				hash_search(relations, (const void *)&oid, HASH_ENTER, NULL);
			prinfo->oid = oid;
			prinfo->attnum = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));
			prinfo->parttype = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, &isnull));
			prinfo->atttype = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 4, &isnull));

			part_oids = lappend_int(part_oids, oid);

			/* children will be filled in later */
			// prinfo->children = NIL;
		}
	}

	/* load children information */
	foreach(lc, part_oids)
	{
		Oid oid = (int) lfirst_int(lc);

		prinfo = (PartRelationInfo*)
			hash_search(relations, (const void *)&oid, HASH_FIND, NULL);	

		// load_check_constraints(oid);

		switch(prinfo->parttype)
		{
			case PT_RANGE:
				// load_range_restrictions(oid);
				load_check_constraints(oid);
				break;
			case PT_HASH:
				load_hash_restrictions(oid);
				break;
		}
	}
	SPI_finish();
}

void
create_part_relations_hashtable()
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int);
	ctl.entrysize = sizeof(PartRelationInfo);

	/* already exists, recreate */
	if (relations != NULL)
		hash_destroy(relations);

	relations = ShmemInitHash("Partitioning relation info",
							  1024, 1024,
							  &ctl, HASH_ELEM);
							  // &ctl, HASH_ELEM | HASH_BLOBS);
}

void
load_hash_restrictions(Oid parent_oid)
{
	bool		found;
	PartRelationInfo *prel;
	HashRelation *hashrel;
	HashRelationKey key;
	int ret;
	int i;
	int proc;
	bool isnull;

	Datum vals[1];
	Oid oids[1] = {INT4OID};
	bool nulls[1] = {false};
	vals[0] = Int32GetDatum(parent_oid);

	prel = (PartRelationInfo*)
		hash_search(relations, (const void *) &parent_oid, HASH_FIND, &found);

	/* if already loaded then quit */
	if (prel->children_count > 0)
		return;

	ret = SPI_execute_with_args("SELECT p.relfilenode, hr.hash, c.relfilenode "
								"FROM pg_pathman_hash_rels hr "
								"JOIN pg_class p ON p.relname = hr.parent "
								"JOIN pg_class c ON c.relname = hr.child "
								"WHERE p.relfilenode = $1",
								1, oids, vals, nulls, true, 0);
	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL)
    {
    	TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        Oid *children;

        /* allocate an array of children Oids */
        alloc_dsm_array(&prel->children, sizeof(Oid), proc);
        children = (Oid *) dsm_array_get_pointer(&prel->children);

        for (i=0; i<proc; i++)
        {
            HeapTuple tuple = tuptable->vals[i];
			int child_oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 3, &isnull));

			key.parent_oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &isnull));
			key.hash = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));

			hashrel = (HashRelation *)
				hash_search(hash_restrictions, (void *) &key, HASH_ENTER, &found);
			hashrel->child_oid = child_oid;

			/* appending children to PartRelationInfo */
			// prel->children[prel->children_count++] = child_oid;
			children[prel->children_count++] = child_oid;
        }
    }

	// SPI_finish();
}

/*
 * Create hash restrictions table
 */
void
create_hash_restrictions_hashtable()
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(HashRelationKey);
	ctl.entrysize = sizeof(HashRelation);

	/* already exists, recreate */
	if (hash_restrictions != NULL)
		hash_destroy(hash_restrictions);

	hash_restrictions = ShmemInitHash("pg_pathman hash restrictions",
									  1024, 1024,
									  &ctl, HASH_ELEM | HASH_BLOBS);
}

/*
 * Load and validate constraints
 * TODO: make it work for HASH partitioning
 */
void
load_check_constraints(Oid parent_oid)
{
	bool		found;
	PartRelationInfo *prel;
	RangeRelation *rangerel;
	int ret;
	int i;
	int proc;
	bool arg1_isnull, arg2_isnull;

	Datum vals[1];
	Oid oids[1] = {INT4OID};
	bool nulls[1] = {false};
	vals[0] = Int32GetDatum(parent_oid);

	prel = (PartRelationInfo*)
		hash_search(relations, (const void *) &parent_oid, HASH_FIND, &found);

	// /* if already loaded then quit */
	// if (prel->children_count > 0)
	// 	return;

	// SPI_connect();
	ret = SPI_execute_with_args("select pg_constraint.* "
								"from pg_constraint "
								"join pg_inherits on inhrelid = conrelid "
								"where inhparent = $1 and contype='c';",
								1, oids, vals, nulls, true, 0);
	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL)
    {
    	TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        Oid *children;
        RangeEntry *ranges;
        Datum min;
        Datum max;

		rangerel = (RangeRelation *)
			hash_search(range_restrictions, (void *) &parent_oid, HASH_ENTER, &found);
		rangerel->nranges = 0;

        alloc_dsm_array(&prel->children, sizeof(Oid), proc);
        children = (Oid *) dsm_array_get_pointer(&prel->children);

        alloc_dsm_array(&rangerel->ranges, sizeof(RangeEntry), proc);
        ranges = (RangeEntry *) dsm_array_get_pointer(&rangerel->ranges);

        for (i=0; i<proc; i++)
        {
			RangeEntry	re;
            HeapTuple	tuple = tuptable->vals[i];
            bool		isnull;
            Datum		val;
            char	   *conbin;
            Expr	   *expr;

			// HeapTuple	reltuple;
			// Form_pg_class pg_class_tuple;
			Form_pg_constraint con;

			con = (Form_pg_constraint) GETSTRUCT(tuple);

			val = SysCacheGetAttr(CONSTROID, tuple, Anum_pg_constraint_conbin,
								  &isnull);
			if (isnull)
				elog(ERROR, "null conbin for constraint %u",
					 HeapTupleGetOid(tuple));
			conbin = TextDatumGetCString(val);
			expr = (Expr *) stringToNode(conbin);
			
			if (prel->parttype == PT_RANGE)
				validate_range_constraint(expr, prel, &min, &max);

			// re.child_oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 2, &arg1_isnull));
			re.child_oid = con->conrelid;
			re.min = min;
			re.max = max;

			ranges[rangerel->nranges++] = re;
			// children[prel->children_count++] = re.child_oid;
		}

		/* sort ascending */
		qsort(ranges, rangerel->nranges, sizeof(RangeEntry), cmp_range_entries);

		/* copy oids to prel */
		for(i=0; i < rangerel->nranges; i++, prel->children_count++)
			children[i] = ranges[i].child_oid;

		/* TODO: check if some ranges overlap! */
    }
}


/* qsort comparison function for oids */
static int
cmp_range_entries(const void *p1, const void *p2)
{
	RangeEntry		*v1 = (const RangeEntry *) p1;
	RangeEntry		*v2 = (const RangeEntry *) p2;

	if (v1->min < v2->min)
		return -1;
	if (v1->min > v2->min)
		return 1;
	return 0;
}


static bool
validate_range_constraint(Expr *expr, PartRelationInfo *prel, Datum *min, Datum *max)
{
	TypeCacheEntry *tce;
	int strategy;
	BoolExpr *boolexpr = (BoolExpr *) expr;
	OpExpr *opexpr;

	/* it should be an AND operator on top */
	if ( !(IsA(expr, BoolExpr) && boolexpr->boolop == AND_EXPR) )
		return false;

	/* and it should have exactly two operands */
	if (list_length(boolexpr->args) != 2)
		return false;

	tce = lookup_type_cache(prel->atttype, TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
	// strategy = get_op_opfamily_strategy(boolexpr->opno, tce->btree_opf);

	/* check that left operand is >= operator */
	opexpr = (OpExpr *) linitial(boolexpr->args);
	if (get_op_opfamily_strategy(opexpr->opno, tce->btree_opf) == BTGreaterEqualStrategyNumber)
	{
		Node *left = linitial(opexpr->args);
		Node *right = lsecond(opexpr->args);
		if ( !IsA(left, Var) || !IsA(right, Const) )
			return false;
		if ( ((Var*) left)->varattno != prel->attnum )
			return false;
		*min = ((Const*) right)->constvalue;
	}
	else
		return false;

	/* TODO: rewrite this */
	/* check that right operand is < operator */
	opexpr = (OpExpr *) lsecond(boolexpr->args);
	if (get_op_opfamily_strategy(opexpr->opno, tce->btree_opf) == BTLessStrategyNumber)
	{
		Node *left = linitial(opexpr->args);
		Node *right = lsecond(opexpr->args);
		if ( !IsA(left, Var) || !IsA(right, Const) )
			return false;
		if ( ((Var*) left)->varattno != prel->attnum )
			return false;
		*max = ((Const*) right)->constvalue;
	}
	else
		return false;
}

/*
 * Create range restrictions table
 */
void
create_range_restrictions_hashtable()
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int);
	ctl.entrysize = sizeof(RangeRelation);
	range_restrictions = ShmemInitHash("pg_pathman range restrictions",
									   1024, 1024,
									   &ctl, HASH_ELEM | HASH_BLOBS);
}

/*
 * Remove partitions
 */
void
remove_relation_info(Oid relid)
{
	PartRelationInfo   *prel;
	HashRelationKey		key;
	RangeRelation	   *rangerel;
	int i;

	prel = (PartRelationInfo *)
		hash_search(relations, (const void *) &relid, HASH_FIND, 0);

	/* if there is nothing to remove then just return */
	if (!prel)
		return;

	/* remove children relations */
	switch (prel->parttype)
	{
		case PT_HASH:
			for (i=0; i<prel->children_count; i++)
			{
				key.parent_oid = relid;
				key.hash = i;
				hash_search(hash_restrictions, (const void *) &key, HASH_REMOVE, 0);
			}
			free_dsm_array(&prel->children);
			break;
		case PT_RANGE:
			rangerel = (RangeRelation *)
				hash_search(range_restrictions, (const void *) &relid, HASH_FIND, 0);
			free_dsm_array(&rangerel->ranges);
			free_dsm_array(&prel->children);
			hash_search(range_restrictions, (const void *) &relid, HASH_REMOVE, 0);
			break;
	}
	prel->children_count = 0;
	hash_search(relations, (const void *) &relid, HASH_REMOVE, 0);
}