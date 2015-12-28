#include "pathman.h"
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
#include "utils/date.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "access/heapam.h"
#include "storage/ipc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"

PG_MODULE_MAGIC;

typedef struct
{
	Oid old_varno;
	Oid new_varno;
} change_varno_context;

/* Original hooks */
static set_rel_pathlist_hook_type set_rel_pathlist_hook_original = NULL;
static shmem_startup_hook_type shmem_startup_hook_original = NULL;

void _PG_init(void);
void _PG_fini(void);
static void my_shmem_startup(void);
static void my_hook(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
static PlannedStmt * my_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams);

static void append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte, int childOID);
static void set_pathkeys(PlannerInfo *root, RelOptInfo *childrel, Path *path);
static void disable_inheritance(Query *parse);

static List *walk_expr_tree(Expr *expr, const PartRelationInfo *prel, bool *all);
static int make_hash(const PartRelationInfo *prel, int value);
static int range_binary_search(const RangeRelation *rangerel, FmgrInfo *cmp_func, Datum value, bool *fountPtr);
static List *handle_binary_opexpr(const PartRelationInfo *partrel, const OpExpr *expr, const Var *v, const Const *c, bool *all);
static List *handle_opexpr(const OpExpr *expr, const PartRelationInfo *prel, bool *all);
static List *handle_boolexpr(const BoolExpr *expr, const PartRelationInfo *prel, bool *all);
static List *handle_arrexpr(const ScalarArrayOpExpr *expr, const PartRelationInfo *prel, bool *all);

static void set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
static void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
static List *accumulate_append_subpath(List *subpaths, Path *path);

static void change_varnos_in_restrinct_info(RestrictInfo *rinfo, change_varno_context *context);
static void change_varnos(Node *node, Oid old_varno, Oid new_varno);
static bool change_varno_walker(Node *node, change_varno_context *context);

static bool reconstruct_restrictinfo(Node *node, PartRelationInfo *prel, Oid relid);

/* callbacks */
PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_updated );
PG_FUNCTION_INFO_V1( on_partitions_removed );
PG_FUNCTION_INFO_V1( find_range_partition );


/*
 * Compare two Datums with the given comarison function
 *
 * flinfo is a pointer to an instance of FmgrInfo
 * arg1, arg2 are Datum instances
 */
#define check_lt(flinfo, arg1, arg2) \
	((int) FunctionCall2(cmp_func, arg1, arg2) < 0)
#define check_le(flinfo, arg1, arg2) \
	((int) FunctionCall2(cmp_func, arg1, arg2) <= 0)
#define check_eq(flinfo, arg1, arg2) \
	((int) FunctionCall2(cmp_func, arg1, arg2) == 0)
#define check_ge(flinfo, arg1, arg2) \
	((int) FunctionCall2(cmp_func, arg1, arg2) >= 0)
#define check_gt(flinfo, arg1, arg2) \
	((int) FunctionCall2(cmp_func, arg1, arg2) > 0)


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

	if (initialization_needed)
		init();

	disable_inheritance(parse);
	result = standard_planner(parse, cursorOptions, boundParams);
	return result;
}

/*
 *
 */
static void
disable_inheritance(Query *parse)
{
	RangeTblEntry *rte;
	ListCell	  *lc;
	PartRelationInfo *prel;

	if (parse->commandType != CMD_SELECT)
		return;

	foreach(lc, parse->rtable)
	{
		rte = (RangeTblEntry*) lfirst(lc);
		switch(rte->rtekind)
		{
			case RTE_RELATION:
				if (rte->inh)
				{
					/* look up this relation in pathman relations */
					prel = (PartRelationInfo *)
						hash_search(relations, (const void *) &rte->relid, HASH_FIND, 0);
					if (prel != NULL)
						rte->inh = false;
				}
				break;
			case RTE_SUBQUERY:
				/* recursively disable inheritance for subqueries */
				disable_inheritance(rte->subquery);
				break;
			default:
				break;
		}
	}
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

		ranges = list_make1_int(make_range(0, prel->children_count-1));

		/* Run over restrictions and collect children partitions */
		foreach(lc, rel->baserestrictinfo)
		{
			bool all;
			RestrictInfo *rinfo = (RestrictInfo*) lfirst(lc);
			List *ret = walk_expr_tree(rinfo->clause, prel, &all);
			ranges = intersect_ranges(ranges, ret);

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
			for (i=range_min(range); i<=range_max(range); i++)
				children = lappend_int(children, dsm_arr[i]);
		}

		/* expand simple_rte_array and simple_rel_array */
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

		foreach(lc, children)
		{
			childOID = (Oid) lfirst_int(lc);
			append_child_relation(root, rel, rti, rte, childOID);
		}

		/* TODO: clear old path list */
		rel->pathlist = NIL;
		set_append_rel_pathlist(root, rel, rti, rte);
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
	PartRelationInfo *prel;

	Node *node;
	ListCell *lc;

	prel = (PartRelationInfo *)
			hash_search(relations, (const void *) &rte->relid, HASH_FIND, 0);

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
		RestrictInfo *new_rinfo;

		node = (Node *) lfirst(lc);
		new_rinfo = copyObject(node);

		/* replace old relids with new ones */
		// change_varnos_in_restrinct_info(new_rinfo, rel->relid, childrel->relid);
		change_varnos(new_rinfo, rel->relid, childrel->relid);

		childrel->baserestrictinfo = lappend(childrel->baserestrictinfo,
											 new_rinfo);

		/* TODO: temporarily commented out */
		// reconstruct_restrictinfo((Node *) new_rinfo, prel, childOID);
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
	if (IsA(node, RestrictInfo))
	{
		change_varnos_in_restrinct_info((RestrictInfo *) node, context);
		return false;
	}
	if (IsA(node, List))
	{
		ListCell *lc;

		foreach(lc, (List *) node)
			change_varno_walker((Node *) lfirst(lc), context);
		return false;
	}

	/* Should not find an unplanned subquery */
	Assert(!IsA(node, Query));

	return expression_tree_walker(node, change_varno_walker, (void *) context);
}

static void
change_varnos_in_restrinct_info(RestrictInfo *rinfo, change_varno_context *context)
{
	ListCell *lc;

	// change_varnos((Node *) rinfo->clause, old_varno, new_varno);
	change_varno_walker((Node *) rinfo->clause, context);
	if (rinfo->left_em)
		// change_varnos((Node *) rinfo->left_em->em_expr, old_varno, new_varno);
		change_varno_walker((Node *) rinfo->left_em->em_expr, context);
	if (rinfo->right_em)
		// change_varnos((Node *) rinfo->right_em->em_expr, old_varno, new_varno);
		change_varno_walker((Node *) rinfo->right_em->em_expr, context);
	if (rinfo->orclause)
		foreach(lc, ((BoolExpr *) rinfo->orclause)->args)
		{
			// RestrictInfo *rinfo2 = (RestrictInfo *) lfirst(lc);
			Node *node = (Node *) lfirst(lc);
			// if (IsA(node, BoolExpr))
			// {}
			// change_varnos_in_restrinct_info(node, context);
			change_varno_walker(node, context);
		}

	/* TODO: find some elegant way to do this */
	if (bms_is_member(context->old_varno, rinfo->clause_relids))
	{
		bms_del_member(rinfo->clause_relids, context->old_varno);
		bms_add_member(rinfo->clause_relids, context->new_varno);
	}
	if (bms_is_member(context->old_varno, rinfo->left_relids))
	{
		bms_del_member(rinfo->left_relids, context->old_varno);
		bms_add_member(rinfo->left_relids, context->new_varno);
	}
	if (bms_is_member(context->old_varno, rinfo->right_relids))
	{
		bms_del_member(rinfo->right_relids, context->old_varno);
		bms_add_member(rinfo->right_relids, context->new_varno);
	}
}


/*
 * Recursive function that removes expressions that doesn't satisfy specified
 * relation. Function returns false if clause should be removed from expression
 * and true otherwise.
 *
 * node is instance of Expr or RestricInfo
 * relid is relation Oid
 */
static bool
reconstruct_restrictinfo(Node *node, PartRelationInfo *prel, Oid relid)
{
	ListCell *lc;
	List *new_args = NIL;
	BoolExpr *boolexpr;
	bool ret;

	if (!node)
		return false;

	if (IsA(node, RestrictInfo))
	{
		RestrictInfo *rinfo = (RestrictInfo *) node;
		ret = reconstruct_restrictinfo((Node *) rinfo->clause, prel, relid);
		reconstruct_restrictinfo((Node *) rinfo->orclause, prel, relid);
		return ret;
	}
	if (IsA(node, BoolExpr))
	{
		boolexpr = (BoolExpr *) node;
		switch (boolexpr->boolop)
		{
			case OR_EXPR:
				ret = false;
				foreach (lc, boolexpr->args)
				{
					/* if relation does satisfy this clause then append it */
					if(reconstruct_restrictinfo((Node*)lfirst(lc), prel, relid))
					{
						new_args = lappend(new_args, lfirst(lc));
						ret = true;
					}
				}
				boolexpr->args = new_args;
				/* TODO: destroy old args and list itself */
				return ret;
			case AND_EXPR:
				ret = true;
				foreach (lc, boolexpr->args)
				{
					if(!reconstruct_restrictinfo((Node*)lfirst(lc), prel, relid))
						ret = false;
				}
				return ret;
			default:
				break;
		}
	}
	if(IsA(node, OpExpr) || IsA(node, ScalarArrayOpExpr))
	{
		bool all;
		List *relids = walk_expr_tree((Expr *) node, prel, &all);
		if (all)
			return true;
		return list_member_int(relids, (int)relid);
	}

	return true;
}

/*
 * Recursive function to walk through conditions tree
 */
static List *
walk_expr_tree(Expr *expr, const PartRelationInfo *prel, bool *all)
{
	BoolExpr		   *boolexpr;
	OpExpr			   *opexpr;
	ScalarArrayOpExpr  *arrexpr;

	switch (expr->type)
	{
		/* AND, OR, NOT expressions */
		case T_BoolExpr:
			boolexpr = (BoolExpr *) expr;
			return handle_boolexpr(boolexpr, prel, all);
		/* =, !=, <, > etc. */
		case T_OpExpr:
			opexpr = (OpExpr *) expr;
			return handle_opexpr(opexpr, prel, all);
		/* IN expression */
		case T_ScalarArrayOpExpr:
			arrexpr = (ScalarArrayOpExpr *) expr;
			*all = false;
			return handle_arrexpr(arrexpr, prel, all);
		default:
			*all = true;
			return NIL;
	}
}

/*
 *	This function determines which partitions should appear in query plan
 */
static List *
handle_binary_opexpr(const PartRelationInfo *prel, const OpExpr *expr,
					 const Var *v, const Const *c, bool *all)
{
	HashRelationKey		key;
	RangeRelation	   *rangerel;
	int					int_value;
	Datum				value;
	bool found;
	int pos;
	int startidx, endidx;
	FmgrInfo *cmp_func;
	*all = false;

	/* determine operator type */
	TypeCacheEntry *tce = lookup_type_cache(v->vartype,
		TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR | TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);
	int strategy = get_op_opfamily_strategy(expr->opno, tce->btree_opf);
	cmp_func = &tce->cmp_proc_finfo;

	switch (prel->parttype)
	{
		case PT_HASH:
			if (strategy == BTEqualStrategyNumber)
			{
				int_value = DatumGetInt32(c->constvalue);
				key.hash = make_hash(prel, int_value);

				return list_make1_int(make_range(key.hash, key.hash));
			}
		case PT_RANGE:
			value = c->constvalue;
			rangerel = (RangeRelation *)
				hash_search(range_restrictions, (const void *)&prel->oid, HASH_FIND, NULL);
			if (rangerel != NULL)
			{
				RangeEntry *re;
				// bool		found = false;
				// startidx = 0;
				// int counter = 0;
				RangeEntry *ranges = dsm_array_get_pointer(&rangerel->ranges);

				// endidx = rangerel->nranges-1;

				/* check boundaries */
				if (rangerel->nranges == 0)
				{
					*all = true;
					return list_make1_int(make_range(0, RANGE_INFINITY));
				}
				else if ((check_gt(cmp_func, ranges[0].min, value) && (strategy == BTGreaterStrategyNumber || strategy == BTGreaterEqualStrategyNumber)) ||
						 (check_lt(cmp_func, ranges[rangerel->nranges-1].max, value) && (strategy == BTLessStrategyNumber || strategy == BTLessEqualStrategyNumber)))
				{
					*all = true;
					return list_make1_int(make_range(0, RANGE_INFINITY));
				}
				else if (check_gt(cmp_func, ranges[0].min, value) ||
						 check_lt(cmp_func, ranges[rangerel->nranges-1].max, value))
				{
					*all = false;
					return NIL;
				}

				/* binary search */
				// while (true)
				// {
				// 	i = startidx + (endidx - startidx) / 2;
				// 	if (i >= 0 && i < rangerel->nranges)
				// 	{
				// 		re = &ranges[i];
				// 		if (check_le(cmp_func, re->min, value) && check_le(cmp_func, value, re->max))
				// 		{
				// 			found = true;
				// 			break;
				// 		}
				// 		else if (check_lt(cmp_func, value, re->min))
				// 			endidx = i - 1;
				// 		else if (check_gt(cmp_func, value, re->max))
				// 			startidx = i + 1;
				// 	}
				// 	else
				// 		break;
				// 	/* for debug's sake */
				// 	Assert(++counter < 100);
				// }
				pos = range_binary_search(rangerel, cmp_func, value, &found);
				re = &ranges[pos];

				/* filter partitions */
				if (re != NULL)
				{
					switch(strategy)
					{
						case BTLessStrategyNumber:
							startidx = 0;
							endidx = check_eq(cmp_func, re->min, value) ? pos-1 : pos;
							break;
						case BTLessEqualStrategyNumber:
							startidx = 0;
							endidx = pos;
							break;
						case BTEqualStrategyNumber:
							// return list_make1_int(re->child_oid);
							// return list_make1_int(make_range(prel->oid, prel->oid));
							if (found)
								return list_make1_int(make_range(pos, pos));
							else
								return NIL;
						case BTGreaterEqualStrategyNumber:
							startidx = pos;
							endidx = rangerel->nranges-1;
							break;
						case BTGreaterStrategyNumber:
							startidx = check_eq(cmp_func, re->max, value) ? pos+1 : pos;
							endidx = rangerel->nranges-1;
					}
					// for (j=startidx; j<=endidx; j++)
					// 	children = lappend_int(children, ranges[j].child_oid);
					*all = false;
					return list_make1_int(make_range(startidx, endidx));

					// return children;
				}
			}
	}

	*all = true;
	return NIL;
}

/*
 * Calculates hash value
 */
static int
make_hash(const PartRelationInfo *prel, int value) {
	return value % prel->children_count;
}

/*
 * Search for range section. Returns position of the item in array.
 * If item wasn't found then function returns closest position and sets
 * foundPtr to false.
 */
static int
range_binary_search(const RangeRelation *rangerel, FmgrInfo *cmp_func, Datum value, bool *fountPtr)
{
	int		i;
	int		startidx = 0;
	int		endidx = rangerel->nranges-1;
	int		counter = 0;
	RangeEntry *ranges = dsm_array_get_pointer(&rangerel->ranges);
	RangeEntry *re;

	*fountPtr = false;
	while (true)
	{
		i = startidx + (endidx - startidx) / 2;
		if (i >= 0 && i < rangerel->nranges)
		{
			re = &ranges[i];
			if (check_le(cmp_func, re->min, value) && check_lt(cmp_func, value, re->max))
			{
				*fountPtr = true;
				break;
			}
			
			/* if we still didn't find position then it is not in array */
			if (startidx == endidx)
				return i;

			if (check_lt(cmp_func, value, re->min))
				endidx = i - 1;
			else if (check_ge(cmp_func, value, re->max))
				startidx = i + 1;
		}
		else
			break;
		/* for debug's sake */
		Assert(++counter < 100);
	}

	return i;
}

/*
 *
 */
static List *
handle_opexpr(const OpExpr *expr, const PartRelationInfo *prel, bool *all)
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
			return handle_binary_opexpr(prel, expr, (Var*)firstarg, (Const*)secondarg, all);
		}
		else if (secondarg->type == T_Var && firstarg->type == T_Const &&
			((Var*)secondarg)->varattno == prel->attnum)
		{
			return handle_binary_opexpr(prel, expr, (Var*)secondarg, (Const*)firstarg, all);
		}
	}

	*all = true;
	return NIL;
}

/*
 *
 */
static List *
handle_boolexpr(const BoolExpr *expr, const PartRelationInfo *prel, bool *all)
{
	ListCell *lc;
	List *ret = NIL;
	List *b = NIL;

	*all = (expr->boolop == AND_EXPR) ? true : false;

	if (expr->boolop == AND_EXPR)
		ret = list_make1_int(make_range(0, RANGE_INFINITY));

	foreach (lc, expr->args)
	{
		bool sub_all = false;
		b = walk_expr_tree((Expr*)lfirst(lc), prel, &sub_all);
		switch(expr->boolop)
		{
			case OR_EXPR:
				ret = unite_ranges(ret, b);
				break;
			case AND_EXPR:
				ret = intersect_ranges(ret, b);
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
handle_arrexpr(const ScalarArrayOpExpr *expr, const PartRelationInfo *prel, bool *all)
{
	Node	   *varnode = (Node *) linitial(expr->args);
	Node	   *arraynode = (Node *) lsecond(expr->args);
	// HashRelationKey		key;
	
	if (varnode == NULL || !IsA(varnode, Var))
	{
		*all = true;
		return NIL;
	}

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
			int hash = make_hash(prel, elem_values[i]);
			oids = list_append_unique_int(oids, make_range(hash, hash));
		}

		/* free resources */
		pfree(elem_values);
		pfree(elem_nulls);

		*all = false;
		return oids;
	}

	*all = true;
	return NIL;
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
	Path *path;

	/*
	 * We don't support pushing join clauses into the quals of a seqscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sequential scan */
	path = create_seqscan_path(root, rel, required_outer, 0);
	add_path(rel, path);
	set_pathkeys(root, rel, path);

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

void
set_pathkeys(PlannerInfo *root, RelOptInfo *childrel, Path *path)
{
	ListCell *lc;
	PathKey *pathkey;

	foreach (lc, root->sort_pathkeys)
	{
		pathkey = (PathKey *) lfirst(lc);
		path->pathkeys = lappend(path->pathkeys, pathkey);
	}
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
on_partitions_created(PG_FUNCTION_ARGS)
{
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
on_partitions_updated(PG_FUNCTION_ARGS)
{
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
on_partitions_removed(PG_FUNCTION_ARGS)
{
	Oid		relid;

	LWLockAcquire(load_config_lock, LW_EXCLUSIVE);

	/* parent relation oid */
	relid = DatumGetInt32(PG_GETARG_DATUM(0));
	remove_relation_info(relid);

	LWLockRelease(load_config_lock);

	PG_RETURN_NULL();
}

/*
 * Returns partition oid for specified parent relid and value
 */
Datum
find_range_partition(PG_FUNCTION_ARGS)
{
	int		relid = DatumGetInt32(PG_GETARG_DATUM(0));
	Datum	value = PG_GETARG_DATUM(1);
	Oid		value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	int		pos;
	bool	found;
	RangeRelation	*rangerel;
	RangeEntry		*ranges;
	TypeCacheEntry	*tce;
	FmgrInfo		*cmp_func;

	tce = lookup_type_cache(value_type,
		TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
		TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);
	cmp_func = &tce->cmp_proc_finfo;

	rangerel = (RangeRelation *)
		hash_search(range_restrictions, (const void *) &relid, HASH_FIND, NULL);

	if (!rangerel)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges);
	pos = range_binary_search(rangerel, cmp_func, value, &found);

	if (found)
		PG_RETURN_OID(ranges[pos].child_oid);

	PG_RETURN_NULL();
}
