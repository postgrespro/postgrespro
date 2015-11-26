#include "postgres.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "nodes/primnodes.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "utils/hsearch.h"
#include "utils/tqual.h"
#include "utils/rel.h"
#include "utils/elog.h"
#include "utils/array.h"
#include "access/heapam.h"
#include "storage/ipc.h"
#include "catalog/pg_operator.h"
#include "executor/spi.h"

PG_MODULE_MAGIC;

#define ALL NIL
#define MAX_PARTITIONS 2048

/*
 * Partitioning type
 */
typedef enum PartType
{
	PT_HASH = 1
} PartType;

/*
 * PartRelationInfo
 *		Per-relation partitioning information
 *
 *		oid - parent table oid
 *		children - list of children oids
 *		parttype - partitioning type (HASH, LIST or RANGE)
 *		attnum - attribute number of parent relation
 */
typedef struct PartRelationInfo
{
	Oid			oid;
	// List	   *children;
	/* TODO: is there any better solution to store children in shared memory? */
	Oid			children[MAX_PARTITIONS];
	int			children_count;
	PartType	parttype;
	Index		attnum;
} PartRelationInfo;

static HTAB *relations = NULL;
static HTAB *hash_restrictions = NULL;
static bool initialization_needed = true;

typedef struct HashRelationKey
{
	int		hash;
	Oid		parent_oid;
} HashRelationKey;

typedef struct HashRelation
{
	HashRelationKey key;
	Oid		child_oid;
} HashRelation;

/* Original hooks */
static set_rel_pathlist_hook_type set_rel_pathlist_hook_original = NULL;
static shmem_startup_hook_type shmem_startup_hook_original = NULL;

void _PG_init(void);
void _PG_fini(void);
static void my_shmem_startup(void);
static void my_hook(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
static PlannedStmt * my_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams);

static void append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte, int childOID);
static void init(void);
static void create_part_relations_hashtable(void);
static void create_hash_restrictions_hashtable(void);
static void load_part_relations_hashtable(void);
static void load_hash_restrictions_hashtable(void);
static List *walk_expr_tree(Expr *expr, const PartRelationInfo *prel);
static int make_hash(const PartRelationInfo *prel, int value);
static List *handle_binary_opexpr(const PartRelationInfo *partrel, const Var *v, const Const *c);
static List *handle_opexpr(const OpExpr *expr, const PartRelationInfo *prel);
static List *handle_boolexpr(const BoolExpr *expr, const PartRelationInfo *prel);
static List *handle_arrexpr(const ScalarArrayOpExpr *expr, const PartRelationInfo *prel);

static void set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
static void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
static List *accumulate_append_subpath(List *subpaths, Path *path);

PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_removed );


/*
 * Entry point
 */
void
_PG_init(void)
{
	set_rel_pathlist_hook_original = set_rel_pathlist_hook;
	set_rel_pathlist_hook = my_hook;
	shmem_startup_hook_original = shmem_startup_hook;
	shmem_startup_hook = my_shmem_startup;

	planner_hook = my_planner_hook;
	/* TEMP */
	// get_relation_info_hook = my_get_relation_info;
}

void
_PG_fini(void)
{
	set_rel_pathlist_hook = set_rel_pathlist_hook_original;
	shmem_startup_hook = shmem_startup_hook_original;
	hash_destroy(relations);
	hash_destroy(hash_restrictions);
}

PlannedStmt *
my_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt	  *result;
	RangeTblEntry *rte;
	ListCell	  *lc;
	PartRelationInfo *prel;

	if (initialization_needed)
		init();

	/* Disable inheritance for relations covered by pathman (only for SELECT for now) */
	if (parse->commandType == CMD_SELECT)
	{
		foreach(lc, parse->rtable)
		{
			rte = (RangeTblEntry*) lfirst(lc);
			if (rte->inh)
			{
				/* look up this relation in pathman relations */
				prel = (PartRelationInfo *)
					hash_search(relations, (const void *) &rte->relid, HASH_FIND, 0);
				if (prel != NULL)
					rte->inh = false;
			}
		}
	}

	result = standard_planner(parse, cursorOptions, boundParams);
	return result;
}

/*
 * Initialize hashtables
 */
static void
init(void)
{
	initialization_needed = false;
	load_part_relations_hashtable();
	load_hash_restrictions_hashtable();
}

static void
my_shmem_startup(void)
{
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	create_part_relations_hashtable();
	create_hash_restrictions_hashtable();

	LWLockRelease(AddinShmemInitLock);
}

/*
 * The hook function. All the magic goes here
 */
void
my_hook(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	PartRelationInfo *prel = NULL;

	/* This works on for SELECT queries */
	if (root->parse->commandType != CMD_SELECT)
		return;

	/* Lookup partitioning information for parent relation */
	prel = (PartRelationInfo *)
		hash_search(relations, (const void *) &rte->relid, HASH_FIND, 0);

	if (prel != NULL)
	{
		List *children = NIL;
		ListCell	   *lc;
		int	childOID = -1;
		int	i;

		rte->inh = true;

		/* Run over restrictions and collect children partitions */
		ereport(LOG, (errmsg("Checking restrictions")));
		foreach(lc, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo*) lfirst(lc);
			List *ret = walk_expr_tree(rinfo->clause, prel);
			children = list_concat_unique_int(children, ret);
			list_free(ret);
		}

		if (children == NIL)
		{
			ereport(LOG, (errmsg("Restrictions empty. Copy children from partrel")));
			// children = get_children_oids(partrel);
			// children = list_copy(partrel->children);
			for (i=0; i<prel->children_count; i++)
				children = lappend_int(children, prel->children[i]);
		}

		if (length(children) > 0)
		{
			RelOptInfo **new_rel_array;
			RangeTblEntry **new_rte_array;
			int len = length(children);

			/* Expand simple_rel_array and simple_rte_array */
			ereport(LOG, (errmsg("Expanding simple_rel_array")));

			new_rel_array = (RelOptInfo **)
				palloc0((root->simple_rel_array_size + len) * sizeof(RelOptInfo *));

			/* simple_rte_array is an array equivalent of the rtable list */
			new_rte_array = (RangeTblEntry **)
				palloc0((root->simple_rel_array_size + len) * sizeof(RangeTblEntry *));

			/* TODO: use memcpy */
			for (i=0; i<root->simple_rel_array_size; i++)
			{
				new_rel_array[i] = root->simple_rel_array[i];
				new_rte_array[i] = root->simple_rte_array[i];
			}

			root->simple_rel_array_size += len;
			root->simple_rel_array = new_rel_array;
			root->simple_rte_array = new_rte_array;
			/* TODO: free old arrays */
		}
		else
		{
			ereport(LOG, (errmsg("Children count is 0")));
		}

		ereport(LOG, (errmsg("Appending children")));
		// Добавляем самого себя
		// append_child_relation(root, rel, rti, rte, partrel->oid);
		// {
		// 	AppendRelInfo *appinfo;
		// 	appinfo = makeNode(AppendRelInfo);
		// 	appinfo->parent_relid = rti;
		// 	appinfo->child_relid = rti;
		// 	appinfo->parent_reloid = rte->relid;
		// 	root->append_rel_list = lappend(root->append_rel_list, appinfo);
		// }
		// root->hasInheritedTarget = true;

		foreach(lc, children)
		{
			childOID = (Oid) lfirst_int(lc);
			append_child_relation(root, rel, rti, rte, childOID);
			// root->simple_rel_array_size += 1;
		}

		/* TODO: clear old path list */
		rel->pathlist = NIL;
		// if (root->parse->commandType == CMD_SELECT)
		set_append_rel_pathlist(root, rel, rti, rte);
		// else
		// {
		// 	set_plain_rel_pathlist(root, rel, rte);
		// 	/* Set plin pathlist for each child relation */
		// 	int			parentRTindex = rti;
		// 	ListCell   *l;
		// 	foreach(l, root->append_rel_list)
		// 	{
		// 		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		// 		int			childRTindex;
		// 		RangeTblEntry *childRTE;
		// 		RelOptInfo *childrel;

		// 		/* append_rel_list contains all append rels; ignore others */
		// 		if (appinfo->parent_relid != parentRTindex || appinfo->parent_relid == rti)
		// 			continue;

		// 		/* Re-locate the child RTE and RelOptInfo */
		// 		childRTindex = appinfo->child_relid;
		// 		// childRTE = root->simple_rte_array[childRTindex];
		// 		// childrel = root->simple_rel_array[childRTindex];
		// 		root->simple_rel_array[childRTindex] = NULL;

		// 		/*
		// 		 * Compute the child's access paths.
		// 		 */
		// 		// set_plain_rel_pathlist(root, childrel, childRTE);
		// 		// set_cheapest(childrel);
		// 	}
		// }
	}

	/* Invoke original hook if needed */
	if (set_rel_pathlist_hook_original != NULL)
	{
		set_rel_pathlist_hook_original(root, rel, rti, rte);
	}
}

void
append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte, int childOID)
{
	RangeTblEntry *childrte;
	RelOptInfo    *childrel;
	Index		childRTindex;
	AppendRelInfo *appinfo;

	/* Create RangeTblEntry for child relation */
	childrte = copyObject(rte);
	childrte->relid = childOID;
	childrte->inh = false;
	childrte->requiredPerms = 0;
	root->parse->rtable = lappend(root->parse->rtable, childrte);
	childRTindex = list_length(root->parse->rtable);
	root->simple_rte_array[childRTindex] = childrte;

	/* Create RelOptInfo */
	childrel = build_simple_rel(root, childRTindex, RELOPT_BASEREL);
	childrel->reltargetlist = rel->reltargetlist;
	childrel->baserestrictinfo = list_copy(rel->baserestrictinfo);

	/* Build an AppendRelInfo for this parent and child */
	appinfo = makeNode(AppendRelInfo);
	appinfo->parent_relid = rti;
	appinfo->child_relid = childRTindex;
	appinfo->parent_reloid = rte->relid;
	root->append_rel_list = lappend(root->append_rel_list, appinfo);

	ereport(LOG,
			(errmsg("Relation %u appended", childOID)));
}


/*
 * Recursive function to walk through conditions tree
 */
static List *
walk_expr_tree(Expr *expr, const PartRelationInfo *prel)
{
	BoolExpr		  *boolexpr;
	OpExpr			  *opexpr;
	ScalarArrayOpExpr *arrexpr;

	switch (expr->type)
	{
		/* AND, OR, NOT expressions */
		case T_BoolExpr:
			boolexpr = (BoolExpr *) expr;
			return handle_boolexpr(boolexpr, prel);
		/* =, !=, <, > etc. */
		case T_OpExpr:
			opexpr = (OpExpr *) expr;
			return handle_opexpr(opexpr, prel);
		/* IN expression */
		case T_ScalarArrayOpExpr:
			arrexpr = (ScalarArrayOpExpr *) expr;
			return handle_arrexpr(arrexpr, prel);
		default:
			return ALL;
	}
}

/*
 *	This function determines which partitions should appear in query plan
 */
static List *
handle_binary_opexpr(const PartRelationInfo *partrel, const Var *v, const Const *c)
{
	HashRelationKey		key;
	HashRelation	   *hashrel;
	int					value;

	value = DatumGetInt32(c->constvalue);
	key.hash = make_hash(partrel, value);
	key.parent_oid = partrel->oid;
	hashrel = (HashRelation *)
		hash_search(hash_restrictions, (const void *)&key, HASH_FIND, NULL);

	if (hashrel != NULL) {
		return list_make1_int(hashrel->child_oid);
	}
	else
		return ALL;
}

/*
 * Calculates hash value
 */
static int
make_hash(const PartRelationInfo *prel, int value) {
	return value % prel->children_count;
}

/*
 *
 */
static List *
handle_opexpr(const OpExpr *expr, const PartRelationInfo *prel)
{
	Node *firstarg = NULL;
	Node *secondarg = NULL;
	// Bitmapset *b = NULL;

	/* TODO: expr->opno == 96 - describe. Probably some function needed */
	if (length(expr->args) == 2 && expr->opno == Int4EqualOperator)
	{
		firstarg = (Node*) linitial(expr->args);
		secondarg = (Node*) lsecond(expr->args);
		if (firstarg->type == T_Var && secondarg->type == T_Const) {
			if (((Var*)firstarg)->varattno == prel->attnum)
			{
				/* filter children */
				return handle_binary_opexpr(prel, (Var*)firstarg, (Const*)secondarg);
			}
		}
	}

	return ALL;
}

/*
 *
 */
static List *
handle_boolexpr(const BoolExpr *expr, const PartRelationInfo *prel)
{
	ListCell *lc;
	List *ret = ALL;
	List *b = ALL;

	foreach (lc, expr->args)
	{
		b = walk_expr_tree((Expr*)lfirst(lc), prel);
		switch(expr->boolop)
		{
			case OR_EXPR:
				if (b == ALL)
				{
					list_free(ret);
					ret = ALL;
				}
				else
				{
					ret = list_concat_unique_int(ret, b);
					list_free(b);
					b = ALL;
				}
				break;
			case AND_EXPR:
				ret = list_intersection_int(ret, b);
				list_free(b);
				break;
			default:
				break;
		}
	}

	return ret;
}

/*
 *
 */
static List *
handle_arrexpr(const ScalarArrayOpExpr *expr, const PartRelationInfo *prel)
{
	Node	   *varnode = (Node *) linitial(expr->args);
	Node	   *arraynode = (Node *) lsecond(expr->args);
	HashRelationKey		key;
	HashRelation	   *hashrel;
	
	if (varnode == NULL || !IsA(varnode, Var))
		return ALL;

	if (arraynode && IsA(arraynode, Const) &&
		!((Const *) arraynode)->constisnull)
	{
		ArrayType  *arrayval;
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		int			num_elems;
		Datum	   *elem_values;
		bool	   *elem_nulls;
		int i;
		List *oids = NIL;

		/* extract values from array */
		arrayval = DatumGetArrayTypeP(((Const *) arraynode)->constvalue);
		get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
							 &elmlen, &elmbyval, &elmalign);
		deconstruct_array(arrayval,
						  ARR_ELEMTYPE(arrayval),
						  elmlen, elmbyval, elmalign,
						  &elem_values, &elem_nulls, &num_elems);

		/* construct OIDs list */
		for (i=0; i<num_elems; i++)
		{
			key.hash = make_hash(prel, elem_values[i]);
			key.parent_oid = prel->oid;
			hashrel = (HashRelation *)
				hash_search(hash_restrictions, (const void *)&key, HASH_FIND, NULL);

			if (hashrel != NULL)
				oids = list_append_unique_int(oids, hashrel->child_oid);

		}

		/* free resources */
		pfree(elem_values);
		pfree(elem_nulls);

		return oids;
	}
	return ALL;
}

/* copy-past from allpaths.c with modifications */

/*
 * set_plain_rel_pathlist
 *	  Build access paths for a plain relation (no subquery, no inheritance)
 */
static void
set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;

	/*
	 * We don't support pushing join clauses into the quals of a seqscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sequential scan */
	add_path(rel, create_seqscan_path(root, rel, required_outer, 0));

	/* Consider index scans */
	create_index_paths(root, rel);

	/* Consider TID scans */
	create_tidscan_paths(root, rel);
}

/*
 * set_append_rel_pathlist
 *	  Build access paths for an "append relation"
 */
static void
set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
						Index rti, RangeTblEntry *rte)
{
	int			parentRTindex = rti;
	List	   *live_childrels = NIL;
	List	   *subpaths = NIL;
	bool		subpaths_valid = true;
	ListCell   *l;

	/*
	 * Generate access paths for each member relation, and remember the
	 * cheapest path for each one.  Also, identify all pathkeys (orderings)
	 * and parameterizations (required_outer sets) available for the member
	 * relations.
	 */
	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int			childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		/* Re-locate the child RTE and RelOptInfo */
		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];
		childrel = root->simple_rel_array[childRTindex];

		/*
		 * Compute the child's access paths.
		 */
		// set_rel_pathlist(root, childrel, childRTindex, childRTE);
		set_plain_rel_pathlist(root, childrel, childRTE);
		set_cheapest(childrel);

		/*
		 * If child is dummy, ignore it.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		/*
		 * Child is live, so add it to the live_childrels list for use below.
		 */
		live_childrels = lappend(live_childrels, childrel);

		/*
		 * If child has an unparameterized cheapest-total path, add that to
		 * the unparameterized Append path we are constructing for the parent.
		 * If not, there's no workable unparameterized path.
		 */
		if (childrel->cheapest_total_path->param_info == NULL)
			subpaths = accumulate_append_subpath(subpaths,
											  childrel->cheapest_total_path);
		else
			subpaths_valid = false;
	}

	/*
	 * If we found unparameterized paths for all children, build an unordered,
	 * unparameterized Append path for the rel.  (Note: this is correct even
	 * if we have zero or one live subpath due to constraint exclusion.)
	 */
	if (subpaths_valid)
		add_path(rel, (Path *) create_append_path(rel, subpaths, NULL));

}

static List *
accumulate_append_subpath(List *subpaths, Path *path)
{
	return lappend(subpaths, path);
}

static void
load_part_relations_hashtable()
{
	PartRelationInfo *prinfo;
	int ret;
	int i;
	int proc;
	bool isnull;

	SPI_connect();
	ret = SPI_exec("SELECT pg_class.relfilenode, pg_attribute.attnum, pg_pathman_rels.parttype "
				   "FROM pg_pathman_rels "
				   "JOIN pg_class ON pg_class.relname = pg_pathman_rels.relname "
				   "JOIN pg_attribute ON pg_attribute.attname = pg_pathman_rels.attr "
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
			prinfo = (PartRelationInfo*)hash_search(relations, (const void *)&oid, HASH_ENTER, NULL);
			prinfo->oid = oid;
			prinfo->attnum = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));
			prinfo->parttype = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, &isnull));
			/* children will be filled in later */
			// prinfo->children = NIL;
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
							  128, 128,
							  &ctl, HASH_ELEM | HASH_BLOBS);
}

static void
load_hash_restrictions_hashtable()
{
	bool		found;
	PartRelationInfo *prinfo;
	HashRelation *hashrel;
	HashRelationKey key;
	int ret;
	int i;
	int proc;
	bool isnull;

	SPI_connect();
	ret = SPI_exec("SELECT p.relfilenode, hr.hash, c.relfilenode "
				   "FROM pg_pathman_hash_rels hr "
				   "JOIN pg_class p ON p.relname = hr.parent "
				   "JOIN pg_class c ON c.relname = hr.child", 0);
	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL)
    {
    	TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;

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
			prinfo = (PartRelationInfo*)
				hash_search(relations, (const void *)&key.parent_oid, HASH_ENTER, &found);
			// prinfo->children = lappend_int(prinfo->children, child_oid);
			// prinfo->children_count++;
			prinfo->children[prinfo->children_count++] = child_oid;
        }
    }

	SPI_finish();
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
 * Callbacks
 */
Datum
on_partitions_created(PG_FUNCTION_ARGS) {
	/* Reload config */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	load_part_relations_hashtable();
	load_hash_restrictions_hashtable();

	LWLockRelease(AddinShmemInitLock);

	PG_RETURN_NULL();
}

Datum
on_partitions_removed(PG_FUNCTION_ARGS) {
	HashRelationKey		key;
	Oid					relid;
	PartRelationInfo   *prel;
	int i;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* parent relation oid */
	relid = DatumGetInt32(PG_GETARG_DATUM(0));

	prel = (PartRelationInfo *)
		hash_search(relations, (const void *) &relid, HASH_FIND, 0);

	/* remove children relations from hash_restrictions */
	// for (i=0; i<length(prel->children); i++)
	for (i=0; i<prel->children_count; i++)
	{
		key.parent_oid = relid;
		key.hash = i;
		hash_search(hash_restrictions, (const void *)&key, HASH_REMOVE, 0);
	}
	prel->children_count = 0;
	hash_search(relations, (const void *) &relid, HASH_REMOVE, 0);

	LWLockRelease(AddinShmemInitLock);

	PG_RETURN_NULL();
}
