#include "pathman.h"
#include "postgres.h"
#include "fmgr.h"
#include "nodes/nodeFuncs.h"
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
#include "utils/date.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "access/heapam.h"
#include "storage/ipc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"

PG_MODULE_MAGIC;

/* Original hooks */
static set_rel_pathlist_hook_type set_rel_pathlist_hook_original = NULL;
static shmem_startup_hook_type shmem_startup_hook_original = NULL;

void _PG_init(void);
void _PG_fini(void);
static void my_shmem_startup(void);
static void my_hook(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
static PlannedStmt * my_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams);

static void append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte, int childOID);

static List *walk_expr_tree(Expr *expr, const PartRelationInfo *prel);
static int make_hash(const PartRelationInfo *prel, int value);
static List *handle_binary_opexpr(const PartRelationInfo *partrel, const OpExpr *expr, const Var *v, const Const *c);
static List *handle_opexpr(const OpExpr *expr, const PartRelationInfo *prel);
static List *handle_boolexpr(const BoolExpr *expr, const PartRelationInfo *prel);
static List *handle_arrexpr(const ScalarArrayOpExpr *expr, const PartRelationInfo *prel);

static void set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
static void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
static List *accumulate_append_subpath(List *subpaths, Path *path);

PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_updated );
PG_FUNCTION_INFO_V1( on_partitions_removed );

typedef struct
{
	Oid old_varno;
	Oid new_varno;
} change_varno_context;

static void change_varnos_in_restrinct_info(RestrictInfo *rinfo, Oid old_varno, Oid new_varno);
static void change_varnos(Node *node, Oid old_varno, Oid new_varno);
static bool change_varno_walker(Node *node, change_varno_context *context);

/* callbacks */
PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_updated );
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


static void
my_shmem_startup(void)
{
	/* initialize locks */
	RequestAddinLWLocks(2);
	load_config_lock = LWLockAssign();
	dsm_init_lock    = LWLockAssign();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* allocate shared memory objects */
	alloc_dsm_table();
	create_part_relations_hashtable();
	create_hash_restrictions_hashtable();
	create_range_restrictions_hashtable();

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
		List *ranges;
		ListCell	   *lc;
		int	childOID = -1;
		int	i;
		Oid *dsm_arr;

		rte->inh = true;

		dsm_arr = (Oid *) dsm_array_get_pointer(&prel->children);
		// for (i=0; i<prel->children_count; i++)
		// 	// children = lappend_int(children, prel->children[i]);
		// 	children = lappend_int(children, dsm_arr[i]);

		ranges = list_make1_int(make_irange(0, prel->children_count - 1, false));

		/* Run over restrictions and collect children partitions */
		ereport(LOG, (errmsg("Checking restrictions")));
		foreach(lc, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo*) lfirst(lc);
			List *ret = walk_expr_tree(rinfo->clause, prel, &all);
			ranges = irange_list_intersect(ranges, ret);

			// if (!all)
			// {
			// 	children = list_intersection_int(children, ret);
			// 	list_free(ret);
			// }
		}

		foreach(lc, ranges)
		{
			int i;
			IndexRange range = (IndexRange) lfirst(lc);
			for (i = irange_lower(range); i <= irange_upper(range); i++)
				children = lappend_int(children, dsm_arr[i]);
		}

		// if (children == NIL)
		// {
		// 	ereport(LOG, (errmsg("Restrictions empty. Copy children from partrel")));
		// 	// children = get_children_oids(partrel);
		// 	// children = list_copy(partrel->children);

		// }

		if (list_length(children) > 0)
		{
			RelOptInfo **new_rel_array;
			RangeTblEntry **new_rte_array;
			int len = list_length(children);

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

	Node *node;
	ListCell *lc;

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

	/* copy targetlist */
	childrel->reltargetlist = NIL;
	foreach(lc, rel->reltargetlist)
	{
		Node *new_target;

		node = (Node *) lfirst(lc);
		new_target = copyObject(node);
		change_varnos(new_target, rel->relid, childrel->relid);
		childrel->reltargetlist = lappend(childrel->reltargetlist, new_target);
	}

	/* copy restrictions */
	childrel->baserestrictinfo = NIL;
	foreach(lc, rel->baserestrictinfo)
	{
		Node *new_rinfo;

		node = (Node *) lfirst(lc);
		new_rinfo = copyObject(node);

		/* replace old relids with new ones */
		change_varnos_in_restrinct_info(new_rinfo, rel->relid, childrel->relid);

		childrel->baserestrictinfo = lappend(childrel->baserestrictinfo,
											 new_rinfo);
	}

	/* Build an AppendRelInfo for this parent and child */
	appinfo = makeNode(AppendRelInfo);
	appinfo->parent_relid = rti;
	appinfo->child_relid = childRTindex;
	appinfo->parent_reloid = rte->relid;
	root->append_rel_list = lappend(root->append_rel_list, appinfo);
	root->total_table_pages += (double) childrel->pages;

	ereport(LOG,
			(errmsg("Relation %u appended", childOID)));
}


static void
change_varnos(Node *node, Oid old_varno, Oid new_varno)
{
	change_varno_context context;
	context.old_varno = old_varno;
	context.new_varno = new_varno;

	change_varno_walker(node, &context);
}

static bool
change_varno_walker(Node *node, change_varno_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varno == context->old_varno)
			var->varno = context->new_varno;
		return false;
	}

	/* Should not find an unplanned subquery */
	Assert(!IsA(node, Query));

	return expression_tree_walker(node, change_varno_walker, (void *) context);
}


void
change_varnos_in_restrinct_info(RestrictInfo *rinfo, Oid old_varno, Oid new_varno)
{
	ListCell *lc;

	change_varnos((Node *) rinfo->clause, old_varno, new_varno);
	if (rinfo->left_em)
		change_varnos((Node *) rinfo->left_em->em_expr, old_varno, new_varno);
	if (rinfo->right_em)
		change_varnos((Node *) rinfo->right_em->em_expr, old_varno, new_varno);
	if (rinfo->orclause)
		foreach(lc, ((BoolExpr *) rinfo->orclause)->args)
		{
			RestrictInfo *rinfo2 = (RestrictInfo *) lfirst(lc);
			change_varnos_in_restrinct_info(rinfo2, old_varno, new_varno);
		}

	/* TODO: find some elegant way to do this */
	if (bms_is_member(old_varno, rinfo->clause_relids))
	{
		bms_del_member(rinfo->clause_relids, old_varno);
		bms_add_member(rinfo->clause_relids, new_varno);
	}
	if (bms_is_member(old_varno, rinfo->left_relids))
	{
		bms_del_member(rinfo->left_relids, old_varno);
		bms_add_member(rinfo->left_relids, new_varno);
	}
	if (bms_is_member(old_varno, rinfo->right_relids))
	{
		bms_del_member(rinfo->right_relids, old_varno);
		bms_add_member(rinfo->right_relids, new_varno);
	}
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
handle_binary_opexpr(const PartRelationInfo *prel, const OpExpr *expr,
					 const Var *v, const Const *c)
{
	HashRelationKey		key;
	RangeRelation	   *rangerel;
	int					int_value;
	Datum				value;
	int					i,
						startidx,
						endidx;
	FmgrInfo		   *cmp_func;

	*all = false;

	/* determine operator type */
	TypeCacheEntry *tce = lookup_type_cache(v->vartype,
		TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
	int strategy = get_op_opfamily_strategy(expr->opno, tce->btree_opf);

	switch (prel->parttype)
	{
		case PT_HASH:
			if (expr->opno == Int4EqualOperator)
			{
				int_value = DatumGetInt32(c->constvalue);
				key.hash = make_hash(prel, int_value);

				return list_make1_irange(make_irange(key.hash, key.hash, true));

				// key.parent_oid = prel->oid;
				// hashrel = (HashRelation *)
				// 	hash_search(hash_restrictions, (const void *)&key, HASH_FIND, NULL);

				// if (hashrel != NULL)
				// 	return list_make1_int(hashrel->child_oid);
				// else
				// {
				// 	*all = true;
				// 	return NIL;
				// }
			}
		case PT_RANGE:
			value = c->constvalue;
			rangerel = (RangeRelation *)
				hash_search(range_restrictions, (const void *)&prel->oid, HASH_FIND, NULL);
			if (rangerel != NULL)
			{
				RangeEntry *re;
				// List	   *children = NIL;
				bool		found = false;
				startidx = 0;
				int counter = 0;
				RangeEntry *ranges = dsm_array_get_pointer(&rangerel->ranges);

				endidx = rangerel->nranges-1;

				/* check boundaries */
				if (rangerel->nranges == 0)
				{
					*all = true;
					return NIL;
				}
				else if ((check_gt(cmp_func, ranges[0].min, value) && (strategy == BTGreaterStrategyNumber || strategy == BTGreaterEqualStrategyNumber)) ||
						 (check_lt(cmp_func, ranges[rangerel->nranges-1].max, value) && (strategy == BTLessStrategyNumber || strategy == BTLessEqualStrategyNumber)))
				{
					*all = true;
					return NIL;
				}
				else if (check_gt(cmp_func, ranges[0].min, value) ||
						 check_lt(cmp_func, ranges[rangerel->nranges-1].max, value))
				{
					*all = false;
					return NIL;
				}

				/* binary search */
				while (true)
				{
					i = startidx + (endidx - startidx) / 2;
					if (i >= 0 && i < rangerel->nranges)
					{
						re = &ranges[i];
						if (check_le(cmp_func, re->min, value) && check_le(cmp_func, value, re->max))
						{
							found = true;
							break;
						}
						else if (value < re->min)
							endidx = i - 1;
						// else if (value >= re->max)
						else if (value > re->max)
							startidx = i + 1;
					}
					else
						break;
					/* for debug's sake */
					Assert(++counter < 100);
				}

				/* filter partitions */
				if (found)
				{
					switch(strategy)
					{
						case OP_STRATEGY_LT:
							startidx = 0;
							endidx = check_eq(cmp_func, re->min, value) ? i - 1 : i;
							break;
						case OP_STRATEGY_LE:
							startidx = 0;
							endidx = i;
							break;
						case BTEqualStrategyNumber:
							// return list_make1_int(re->child_oid);
							// return list_make1_int(make_range(prel->oid, prel->oid));
							return list_make1_irange(make_irange(i, i, true));
						case BTGreaterEqualStrategyNumber:
							startidx = i;
							endidx = rangerel->nranges-1;
							break;
						case BTGreaterStrategyNumber:
							startidx = check_eq(cmp_func, re->max, value) ? i + 1 : i;
							endidx = rangerel->nranges-1;
					}
					// for (j=startidx; j<=endidx; j++)
					// 	children = lappend_int(children, ranges[j].child_oid);
					*all = false;
					return list_make1_irange(make_irange(startidx, endidx, true));

					// return children;
				}
			}
	}

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

	if (list_length(expr->args) == 2)
	{
		firstarg = (Node*) linitial(expr->args);
		secondarg = (Node*) lsecond(expr->args);
		if (firstarg->type == T_Var && secondarg->type == T_Const &&
			((Var*)firstarg)->varattno == prel->attnum)
		{
			return handle_binary_opexpr(prel, expr, (Var*)firstarg, (Const*)secondarg);
		}
		else if (secondarg->type == T_Var && firstarg->type == T_Const &&
			((Var*)secondarg)->varattno == prel->attnum)
		{
			return handle_binary_opexpr(prel, expr, (Var*)secondarg, (Const*)firstarg);
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

	if (expr->boolop == AND_EXPR)
		ret = list_make1_irange(make_irange(0, RANGE_INFINITY, false));

	foreach (lc, expr->args)
	{
		b = walk_expr_tree((Expr*)lfirst(lc), prel);
		switch(expr->boolop)
		{
			// case OR_EXPR:
			// 	if (sub_all)
			// 	{
			// 		list_free(ret);
			// 		ret = NIL;

			// 		/*
			// 		 * if at least one subexpr returns all partitions then
			// 		 * the whole OR-expression does
			// 		 */
			// 		*all = true;

			// 		/* so we just could return here */
			// 		return ret;
			// 	}
			// 	else
			// 	{
			// 		ret = list_concat_unique_int(ret, b);
			// 		list_free(b);
			// 	}
			// 	break;
			// case AND_EXPR:
			// 	ret = list_intersection_int(ret, b);
			// 	list_free(b);

			// 	/*
			// 	 * if even one subexpr doesn't return all partitions then
			// 	 * the whole AND-expression doesn't.
			// 	 */
			// 	if (!sub_all)
			// 		all = false;
			// 	break;

			case OR_EXPR:
				ret = irange_list_union(ret, b);
				// list_free(b);
				break;
			case AND_EXPR:
				ret = irange_list_intersect(ret, b);
				// list_free(b);
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
		int			i;
		List	   *ret = NIL;

		/* extract values from array */
		arrayval = DatumGetArrayTypeP(((Const *) arraynode)->constvalue);
		get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
							 &elmlen, &elmbyval, &elmalign);
		deconstruct_array(arrayval,
						  ARR_ELEMTYPE(arrayval),
						  elmlen, elmbyval, elmalign,
						  &elem_values, &elem_nulls, &num_elems);

		/* construct OIDs list */
		for (i = 0; i < num_elems; i++)
		{
			key.hash = make_hash(prel, elem_values[i]);
			// key.parent_oid = prel->oid;
			// hashrel = (HashRelation *)
			// 	hash_search(hash_restrictions, (const void *)&key, HASH_FIND, NULL);

			// if (hashrel != NULL)
			// 	oids = list_append_unique_int(oids, hashrel->child_oid);

			ret = list_append_unique_int(ret, make_irange(key.hash, key.hash, true));
		}

		/* free resources */
		pfree(elem_values);
		pfree(elem_nulls);

		*all = false;
		return ret;
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


/*
 * Callbacks
 */
Datum
on_partitions_created(PG_FUNCTION_ARGS) {
	// Oid relid;

	LWLockAcquire(load_config_lock, LW_EXCLUSIVE);

	/* Reload config */
	/* TODO: reload just the specified relation */
	// relid = DatumGetInt32(PG_GETARG_DATUM(0))
	load_part_relations_hashtable();

	LWLockRelease(load_config_lock);

	PG_RETURN_NULL();
}

Datum
on_partitions_updated(PG_FUNCTION_ARGS) {
	Oid					relid;
	PartRelationInfo   *prel;

	/* parent relation oid */
	relid = DatumGetInt32(PG_GETARG_DATUM(0));
	prel = (PartRelationInfo *)
		hash_search(relations, (const void *) &relid, HASH_FIND, 0);
	if (prel != NULL)
	{
		LWLockAcquire(load_config_lock, LW_EXCLUSIVE);
		remove_relation_info(relid);
		load_part_relations_hashtable();
		LWLockRelease(load_config_lock);
	}

	PG_RETURN_NULL();
}

Datum
on_partitions_removed(PG_FUNCTION_ARGS) {
	Oid					relid;

	LWLockAcquire(load_config_lock, LW_EXCLUSIVE);

	/* parent relation oid */
	relid = DatumGetInt32(PG_GETARG_DATUM(0));
	remove_relation_info(relid);

	LWLockRelease(load_config_lock);

	PG_RETURN_NULL();
}
