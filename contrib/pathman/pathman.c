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

typedef struct
{
	Oid old_varno;
	Oid new_varno;
} change_varno_context;

typedef struct
{
	const Node	   *orig;
	List		   *args;
	List		   *rangeset;
} WrapperNode;

/* Original hooks */
static set_rel_pathlist_hook_type set_rel_pathlist_hook_original = NULL;
static shmem_startup_hook_type shmem_startup_hook_original = NULL;

void _PG_init(void);
void _PG_fini(void);
static void my_shmem_startup(void);
static void my_hook(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
static PlannedStmt * my_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams);

static void append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti,
				RangeTblEntry *rte, int index, Oid childOID, List *wrappers);
static Node *wrapper_make_expression(WrapperNode *wrap, int index, bool *alwaysTrue);
static void set_pathkeys(PlannerInfo *root, RelOptInfo *childrel, Path *path);
static void disable_inheritance(Query *parse);

static WrapperNode *walk_expr_tree(Expr *expr, const PartRelationInfo *prel);
static int make_hash(const PartRelationInfo *prel, int value);
static void handle_binary_opexpr(const PartRelationInfo *prel, WrapperNode *result, const Var *v, const Const *c);
static WrapperNode *handle_opexpr(const OpExpr *expr, const PartRelationInfo *prel);
static WrapperNode *handle_boolexpr(const BoolExpr *expr, const PartRelationInfo *prel);
static WrapperNode *handle_arrexpr(const ScalarArrayOpExpr *expr, const PartRelationInfo *prel);

static void set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
static void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
static List *accumulate_append_subpath(List *subpaths, Path *path);

static void change_varnos_in_restrinct_info(RestrictInfo *rinfo, change_varno_context *context);
static void change_varnos(Node *node, Oid old_varno, Oid new_varno);
static bool change_varno_walker(Node *node, change_varno_context *context);

/* callbacks */
PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_updated );
PG_FUNCTION_INFO_V1( on_partitions_removed );


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
		List		   *ranges,
					   *wrappers;
		ListCell	   *lc;
		int	i;
		Oid *dsm_arr;

		rte->inh = true;

		dsm_arr = (Oid *) dsm_array_get_pointer(&prel->children);
		// for (i=0; i<prel->children_count; i++)
		// 	// children = lappend_int(children, prel->children[i]);
		// 	children = lappend_int(children, dsm_arr[i]);

		ranges = list_make1_int(make_irange(0, prel->children_count - 1, false));

		/* Run over restrictions and collect children partitions */
		wrappers = NIL;
		foreach(lc, rel->baserestrictinfo)
		{
			WrapperNode *wrap;

			RestrictInfo *rinfo = (RestrictInfo*) lfirst(lc);

			wrap = walk_expr_tree(rinfo->clause, prel);
			wrappers = lappend(wrappers, wrap);
			ranges = irange_list_intersect(ranges, wrap->rangeset);
		}

		/* expand simple_rte_array and simple_rel_array */
		if (list_length(ranges) > 0)
		{
			RelOptInfo **new_rel_array;
			RangeTblEntry **new_rte_array;
			int len = irange_list_length(ranges);

			/* Expand simple_rel_array and simple_rte_array */
			ereport(LOG, (errmsg("Expanding simple_rel_array")));

			new_rel_array = (RelOptInfo **)
				palloc0((root->simple_rel_array_size + len) * sizeof(RelOptInfo *));

			/* simple_rte_array is an array equivalent of the rtable list */
			new_rte_array = (RangeTblEntry **)
				palloc0((root->simple_rel_array_size + len) * sizeof(RangeTblEntry *));

			/* TODO: use memcpy */
			for (i = 0; i < root->simple_rel_array_size; i++)
			{
				new_rel_array[i] = root->simple_rel_array[i];
				new_rte_array[i] = root->simple_rte_array[i];
			}

			root->simple_rel_array_size += len;
			root->simple_rel_array = new_rel_array;
			root->simple_rte_array = new_rte_array;
			/* TODO: free old arrays */
		}

		foreach(lc, ranges)
		{
			IndexRange	irange = lfirst_irange(lc);
			Oid			childOid;

			for (i = irange_lower(irange); i <= irange_upper(irange); i++)
			{
				childOid = dsm_arr[i];
				append_child_relation(root, rel, rti, rte, i, childOid, wrappers);
			}
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

static void
append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti,
	RangeTblEntry *rte, int index, Oid childOid, List *wrappers)
{
	RangeTblEntry *childrte;
	RelOptInfo    *childrel;
	Index		childRTindex;
	AppendRelInfo *appinfo;
	PartRelationInfo *prel;

	Node *node;
	ListCell *lc, *lc2;

	prel = (PartRelationInfo *)
			hash_search(relations, (const void *) &rte->relid, HASH_FIND, 0);

	/* Create RangeTblEntry for child relation */
	childrte = copyObject(rte);
	childrte->relid = childOid;
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
	forboth(lc, wrappers, lc2, rel->baserestrictinfo)
	{
		bool alwaysTrue;
		WrapperNode *wrap = (WrapperNode *) lfirst(lc);
		Node *new_clause = wrapper_make_expression(wrap, index, &alwaysTrue);
		RestrictInfo *new_rinfo;

		if (alwaysTrue)
			continue;
		Assert(new_clause);

		/* TODO: evade double copy of clause */

		new_rinfo = copyObject((Node *) lfirst(lc2));
		new_rinfo->clause = (Expr *)new_clause;

		/* replace old relids with new ones */
		change_varnos((Node *)new_rinfo, rel->relid, childrel->relid);

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
			(errmsg("Relation %u appended", childOid)));
}

static Node *
wrapper_make_expression(WrapperNode *wrap, int index, bool *alwaysTrue)
{
	bool	lossy, found;

	*alwaysTrue = false;
	found = irange_list_find(wrap->rangeset, index, &lossy);
	if (!found)
		return NULL;
	if (!lossy)
	{
		*alwaysTrue = true;
		return NULL;
	}

	if (IsA(wrap->orig, BoolExpr))
	{
		const BoolExpr *expr = (const BoolExpr *) wrap->orig;
		BoolExpr *result;

		if (expr->boolop == OR_EXPR || expr->boolop == AND_EXPR)
		{
			ListCell *lc;
			List *args = NIL;

			foreach (lc, wrap->args)
			{
				Node *arg;

				arg = wrapper_make_expression((WrapperNode *)lfirst(lc), index, alwaysTrue);
#ifdef USE_ASSERT_CHECKING
				if (expr->boolop == OR_EXPR)
					Assert(!(*alwaysTrue));
				if (expr->boolop == AND_EXPR)
					Assert(arg || *alwaysTrue);
#endif
				if (arg)
					args = lappend(args, arg);
			}

			Assert(list_length(args) >= 1);
			if (list_length(args) == 1)
				return (Node *) linitial(args);

			result = (BoolExpr *) palloc(sizeof(BoolExpr));
			result->xpr.type = T_BoolExpr;
			result->args = args;
			result->boolop = expr->boolop;
			result->location = expr->location;
			return (Node *)result;
		}
		else
		{
			return copyObject(wrap->orig);
		}
	}
	else
	{
		return copyObject(wrap->orig);
	}
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
 * Recursive function to walk through conditions tree
 */
static WrapperNode *
walk_expr_tree(Expr *expr, const PartRelationInfo *prel)
{
	BoolExpr		   *boolexpr;
	OpExpr			   *opexpr;
	ScalarArrayOpExpr  *arrexpr;
	WrapperNode		   *result;

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
			result = (WrapperNode *)palloc(sizeof(WrapperNode));
			result->orig = (const Node *)expr;
			result->args = NIL;
			result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, true));
			return result;
	}
}

/*
 *	This function determines which partitions should appear in query plan
 */
static void
handle_binary_opexpr(const PartRelationInfo *prel, WrapperNode *result,
					 const Var *v, const Const *c)
{
	HashRelationKey		key;
	RangeRelation	   *rangerel;
	Datum				value;
	int					i,
						startidx,
						endidx,
						int_value,
						strategy;
	FmgrInfo		   *cmp_func;
	const OpExpr	   *expr = (const OpExpr *)result->orig;
	TypeCacheEntry	   *tce;


	/* determine operator type */
	tce = lookup_type_cache(v->vartype,
		TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR | TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);
	strategy = get_op_opfamily_strategy(expr->opno, tce->btree_opf);
	cmp_func = &tce->cmp_proc_finfo;

	switch (prel->parttype)
	{
		case PT_HASH:
			if (strategy == BTEqualStrategyNumber)
			{
				int_value = DatumGetInt32(c->constvalue);
				key.hash = make_hash(prel, int_value);

				result->rangeset = list_make1_irange(make_irange(key.hash, key.hash, true));
				return;
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

				// int res = (int) FunctionCall2(cmp_func, rangerel->ranges[0].min, value);

				endidx = rangerel->nranges-1;

				/* check boundaries */
				if (rangerel->nranges == 0)
				{
					result->rangeset = list_make1_irange(make_irange(startidx, endidx, true));
					return;
				}
				else if ((check_gt(cmp_func, ranges[0].min, value) && (strategy == BTGreaterStrategyNumber || strategy == BTGreaterEqualStrategyNumber)) ||
						 (check_lt(cmp_func, ranges[rangerel->nranges-1].max, value) && (strategy == BTLessStrategyNumber || strategy == BTLessEqualStrategyNumber)))
				{
					result->rangeset = list_make1_irange(make_irange(startidx, endidx, true));
					return;
				}
				else if (check_gt(cmp_func, ranges[0].min, value) ||
						 check_lt(cmp_func, ranges[rangerel->nranges-1].max, value))
				{
					result->rangeset = list_make1_irange(make_irange(startidx, endidx, true));
					return;
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
						else if (check_lt(cmp_func, value, re->min))
							endidx = i - 1;
						else if (check_gt(cmp_func, value, re->max))
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
						case BTLessStrategyNumber:
							startidx = 0;
							endidx = check_eq(cmp_func, re->min, value) ? i - 1 : i;
							break;
						case BTLessEqualStrategyNumber:
							startidx = 0;
							endidx = i;
							break;
						case BTEqualStrategyNumber:
							// return list_make1_int(re->child_oid);
							// return list_make1_int(make_range(prel->oid, prel->oid));
							result->rangeset = list_make1_irange(make_irange(i, i, true));
							return;
						case BTGreaterEqualStrategyNumber:
							startidx = i;
							endidx = rangerel->nranges-1;
							break;
						case BTGreaterStrategyNumber:
							startidx = check_eq(cmp_func, re->max, value) ? i + 1 : i;
							endidx = rangerel->nranges-1;
					}
					result->rangeset = list_make1_irange(make_irange(startidx, endidx, true));
					return;
				}
			}
	}

	result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, true));
}

/*
 * Calculates hash value
 */
static int
make_hash(const PartRelationInfo *prel, int value)
{
	return value % prel->children_count;
}

/*
 *
 */
static WrapperNode *
handle_opexpr(const OpExpr *expr, const PartRelationInfo *prel)
{
	WrapperNode	*result = (WrapperNode *)palloc(sizeof(WrapperNode));
	Node		*firstarg = NULL,
				*secondarg = NULL;

	result->orig = (const Node *)expr;
	result->args = NIL;

	if (list_length(expr->args) == 2)
	{
		firstarg = (Node *) linitial(expr->args);
		secondarg = (Node *) lsecond(expr->args);

		if (firstarg->type == T_Var &&
			secondarg->type == T_Const &&
			((Var *)firstarg)->varattno == prel->attnum)
		{
			handle_binary_opexpr(prel, result, (Var *)firstarg, (Const *)secondarg);
			return result;
		}
		else if (secondarg->type == T_Var &&
			firstarg->type == T_Const &&
			((Var *)secondarg)->varattno == prel->attnum)
		{
			handle_binary_opexpr(prel, result, (Var *)secondarg, (Const *)firstarg);
			return result;
		}
	}

	result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, true));
	return result;
}

/*
 *
 */
static WrapperNode *
handle_boolexpr(const BoolExpr *expr, const PartRelationInfo *prel)
{
	WrapperNode	*result = (WrapperNode *)palloc(sizeof(WrapperNode));
	ListCell	*lc;

	result->orig = (const Node *)expr;
	result->args = NIL;

	if (expr->boolop == AND_EXPR)
		result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, false));
	else
		result->rangeset = NIL;

	foreach (lc, expr->args)
	{
		WrapperNode *arg;

		arg = walk_expr_tree((Expr *)lfirst(lc), prel);
		result->args = lappend(result->args, arg);
		switch(expr->boolop)
		{
			case OR_EXPR:
				result->rangeset = irange_list_union(result->rangeset, arg->rangeset);
				break;
			case AND_EXPR:
				result->rangeset = irange_list_intersect(result->rangeset, arg->rangeset);
				break;
			default:
				result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, false));
				break;
		}
	}

	return result;
}

/*
 *
 */
static WrapperNode *
handle_arrexpr(const ScalarArrayOpExpr *expr, const PartRelationInfo *prel)
{
	WrapperNode		   *result = (WrapperNode *)palloc(sizeof(WrapperNode));
	Node			   *varnode = (Node *) linitial(expr->args);
	Node			   *arraynode = (Node *) lsecond(expr->args);
	HashRelationKey		key;

	result->orig = (const Node *)expr;
	result->args = NIL;

	if (varnode == NULL || !IsA(varnode, Var))
	{
		result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, true));
		return result;
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
		int			i;

		/* extract values from array */
		arrayval = DatumGetArrayTypeP(((Const *) arraynode)->constvalue);
		get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
							 &elmlen, &elmbyval, &elmalign);
		deconstruct_array(arrayval,
						  ARR_ELEMTYPE(arrayval),
						  elmlen, elmbyval, elmalign,
						  &elem_values, &elem_nulls, &num_elems);

		result->rangeset = NIL;

		/* construct OIDs list */
		for (i = 0; i < num_elems; i++)
		{
			key.hash = make_hash(prel, elem_values[i]);
			result->rangeset = irange_list_union(result->rangeset,
						list_make1_irange(make_irange(key.hash, key.hash, true)));
		}

		/* free resources */
		pfree(elem_values);
		pfree(elem_nulls);

		return result;
	}

	result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, true));
	return result;
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
