/*-------------------------------------------------------------------------
 *
 * resowner.c
 *	  POSTGRES resource owner management code.
 *
 * Query-lifespan resources are tracked by associating them with
 * ResourceOwner objects.  This provides a simple mechanism for ensuring
 * that such resources are freed at the right time.
 * See utils/resowner/README for more info.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/resowner/resowner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner_private.h"
#include "utils/snapmgr.h"

/*
 * ResourceArray is a common structure for storing different types of resources.
 *
 * ResourceOwner can own `HeapTuple`s, `Relation`s, `Snapshot`s, etc. For
 * each resource type there are procedures ResourceOwnerRemember* and
 * ResourceOwnerForget*. There are also ResourceOwnerEnlarge* procedures
 * which should be called before corresponding ResourceOwnerRemember* calls
 * (see below). Internally each type of resource is stored in separate
 * ResourceArray.
 *
 * There are two major reasons for using ResourceArray instead of, say,
 * regular C arrays.
 *
 * Firstly we would like to prevent code duplication. For instance
 * ResourceArray provides generic Remember/Forget/Enlarge procedures, so
 * corresponding ResourceOwner* procedures are just a typesafe wrappers for
 * these procedures.
 *
 * Secondly ResourceArray must be more efficient than regular C array.
 * Current implementation in general could be considered a hash table. It has
 * O(1) complexity of both Remember and Forget procedures.
 */
typedef struct ResourceArray
{
	Datum	   *itemsarr;		/* buffer for storing values */
	Datum		invalidval;		/* value that is considered invalid */
	uint32		capacity;		/* capacity of array */
	uint32		nitems;			/* how many items is stored in items array */
	uint32		maxitems;		/* precalculated RESARRAY_MAX_ITEMS(capacity) */
	uint32		lastidx;		/* index of last item returned by GetAny */
}	ResourceArray;

/*
 * This number is used as initial size of resource array. If given number of
 * items is not enough, we double array size and reallocate memory.
 *
 * Should be power of two since we use (arrsize - 1) as mask for hash value.
 *
 */
#define RESARRAY_INIT_SIZE 16

/*
 * How many items could be stored in a resource array of given capacity. If
 * this number is reached we need to resize an array to prevent hash collisions.
 *
 * This computation actually costs only two additions and one binary shift.
 */
#define RESARRAY_MAX_ITEMS(capacity) ((capacity)*3/4)

/*
 * Initialize ResourceArray
 */
static void
ResourceArrayInit(ResourceArray * resarr, Datum invalidval)
{
	Assert(resarr->itemsarr == NULL);
	Assert(resarr->capacity == 0);
	Assert(resarr->nitems == 0);
	Assert(resarr->maxitems == 0);
	Assert(resarr->invalidval == 0);
	Assert(resarr->lastidx == 0);

	resarr->invalidval = invalidval;
}

/*
 * Add a resource to ResourceArray
 *
 * Caller must have previously done ResourceArrayEnlarge()
 */
static void
ResourceArrayAdd(ResourceArray * resarr, Datum data)
{
	Datum		idx;
	Datum		mask = resarr->capacity - 1;

	Assert(resarr->maxitems > resarr->nitems);
	Assert(resarr->capacity > 0);
	Assert(resarr->itemsarr != NULL);
	Assert(data != resarr->invalidval);

	idx = hash_any((void *) &data, sizeof(data)) & mask;

	while (true)
	{
		if (resarr->itemsarr[idx] == resarr->invalidval)
			break;
		idx = (idx + 1) & mask;
	}

	resarr->itemsarr[idx] = data;
	resarr->nitems++;
}

/*
 * Remove a resource from ResourceArray
 *
 * Returns true on success, false if resource was not found
 */
static bool
ResourceArrayRemove(ResourceArray * resarr, Datum data)
{
	uint32		i;
	Datum		idx;
	Datum		mask = resarr->capacity - 1;

	Assert(resarr->capacity > 0);
	Assert(resarr->itemsarr != NULL);
	Assert(data != resarr->invalidval);

	idx = hash_any((void *) &data, sizeof(data)) & mask;
	for (i = 0; i < resarr->capacity; i++)
	{
		if (resarr->itemsarr[idx] == data)
		{
			resarr->itemsarr[idx] = resarr->invalidval;
			resarr->nitems--;
			return true;
		}
		idx = (idx + 1) & mask;
	}

	return false;
}

/*
 * Make sure there is a room for at least one more resource in an array.
 *
 * This is separate from actually inserting a resource because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
static void
ResourceArrayEnlarge(ResourceArray * resarr)
{
	uint32		i,
				oldcap;
	Datum	   *olditemsarr;

	if (resarr->nitems < resarr->maxitems)
		return;					/* nothing to do */

	olditemsarr = resarr->itemsarr;
	oldcap = resarr->capacity;

	resarr->capacity = oldcap > 0 ? oldcap * 2 : RESARRAY_INIT_SIZE;
	resarr->itemsarr = (Datum *)
		MemoryContextAlloc(TopMemoryContext,
						   resarr->capacity * sizeof(Datum));
	resarr->maxitems = RESARRAY_MAX_ITEMS(resarr->capacity);
	resarr->nitems = 0;

	for (i = 0; i < resarr->capacity; i++)
		resarr->itemsarr[i] = resarr->invalidval;

	if (olditemsarr != NULL)
	{
		while (oldcap > 0)
		{
			oldcap--;
			if (olditemsarr[oldcap] != resarr->invalidval)
				ResourceArrayAdd(resarr, olditemsarr[oldcap]);
		}
		pfree(olditemsarr);
	}
}

/*
 * Get any convenient element.
 *
 * Returns true on success, false on failure.
 */
static bool
ResourceArrayGetAny(ResourceArray * resarr, Datum *out)
{
	uint32		mask;

	if (resarr->nitems == 0)
		return false;

	Assert(resarr->capacity > 0);
	mask = resarr->capacity - 1;

	for (;;)
	{
		resarr->lastidx = resarr->lastidx & mask;
		if (resarr->itemsarr[resarr->lastidx] != resarr->invalidval)
			break;

		resarr->lastidx++;
	}

	*out = resarr->itemsarr[resarr->lastidx];
	return true;
}

/*
 * Return ResourceArray to initial state
 */
static void
ResourceArrayFree(ResourceArray * resarr)
{
	Assert(resarr->nitems == 0);

	resarr->capacity = 0;
	resarr->maxitems = 0;

	if (!resarr->itemsarr)
		return;

	pfree(resarr->itemsarr);
	resarr->itemsarr = NULL;
}

/*
 * To speed up bulk releasing or reassigning locks from a resource owner to
 * its parent, each resource owner has a small cache of locks it owns. The
 * lock manager has the same information in its local lock hash table, and
 * we fall back on that if cache overflows, but traversing the hash table
 * is slower when there are a lot of locks belonging to other resource owners.
 *
 * MAX_RESOWNER_LOCKS is the size of the per-resource owner cache. It's
 * chosen based on some testing with pg_dump with a large schema. When the
 * tests were done (on 9.2), resource owners in a pg_dump run contained up
 * to 9 locks, regardless of the schema size, except for the top resource
 * owner which contained much more (overflowing the cache). 15 seems like a
 * nice round number that's somewhat higher than what pg_dump needs. Note that
 * making this number larger is not free - the bigger the cache, the slower
 * it is to release locks (in retail), when a resource owner holds many locks.
 */
#define MAX_RESOWNER_LOCKS 15

/*
 * ResourceOwner objects look like this
 */
typedef struct ResourceOwnerData
{
	ResourceOwner parent;		/* NULL if no parent (toplevel owner) */
	ResourceOwner firstchild;	/* head of linked list of children */
	ResourceOwner nextchild;	/* next child of same parent */
	const char *name;			/* name (just for debugging) */

	/* We can remember up to MAX_RESOWNER_LOCKS references to local locks. */
	int			nlocks;			/* number of owned locks */
	LOCALLOCK  *locks[MAX_RESOWNER_LOCKS];		/* list of owned locks */

	/* We have built-in support for remembering: */

	ResourceArray catrefarr;	/* `HeapTuple`s */
	ResourceArray catlistrefarr;	/* `ResourceOwner`s */
	ResourceArray relrefarr;	/* `Relation`s */
	ResourceArray planrefarr;	/* `CachedPlan*`s */
	ResourceArray tupdescarr;	/* `TupleDesc`s */
	ResourceArray snapshotarr;	/* `Snapshot`s */
	ResourceArray dsmarr;		/* `dsm_segment*`s */
	ResourceArray bufferarr;	/* `Buffer`s  */
	ResourceArray filearr;		/* `File`s */

}	ResourceOwnerData;


/*****************************************************************************
 *	  GLOBAL MEMORY															 *
 *****************************************************************************/

ResourceOwner CurrentResourceOwner = NULL;
ResourceOwner CurTransactionResourceOwner = NULL;
ResourceOwner TopTransactionResourceOwner = NULL;

/*
 * List of add-on callbacks for resource releasing
 */
typedef struct ResourceReleaseCallbackItem
{
	struct ResourceReleaseCallbackItem *next;
	ResourceReleaseCallback callback;
	void	   *arg;
} ResourceReleaseCallbackItem;

static ResourceReleaseCallbackItem *ResourceRelease_callbacks = NULL;


/* Internal routines */
static void ResourceOwnerReleaseInternal(ResourceOwner owner,
							 ResourceReleasePhase phase,
							 bool isCommit,
							 bool isTopLevel);
static void PrintRelCacheLeakWarning(Relation rel);
static void PrintPlanCacheLeakWarning(CachedPlan *plan);
static void PrintTupleDescLeakWarning(TupleDesc tupdesc);
static void PrintSnapshotLeakWarning(Snapshot snapshot);
static void PrintFileLeakWarning(File file);
static void PrintDSMLeakWarning(dsm_segment *seg);


/*****************************************************************************
 *	  EXPORTED ROUTINES														 *
 *****************************************************************************/


/*
 * ResourceOwnerCreate
 *		Create an empty ResourceOwner.
 *
 * All ResourceOwner objects are kept in TopMemoryContext, since they should
 * only be freed explicitly.
 */
ResourceOwner
ResourceOwnerCreate(ResourceOwner parent, const char *name)
{
	ResourceOwner owner;

	owner = (ResourceOwner) MemoryContextAllocZero(TopMemoryContext,
												   sizeof(ResourceOwnerData));
	owner->name = name;

	if (parent)
	{
		owner->parent = parent;
		owner->nextchild = parent->firstchild;
		parent->firstchild = owner;
	}

	ResourceArrayInit(&(owner->catrefarr), (Datum) (NULL));
	ResourceArrayInit(&(owner->catlistrefarr), (Datum) (NULL));
	ResourceArrayInit(&(owner->relrefarr), (Datum) (NULL));
	ResourceArrayInit(&(owner->planrefarr), (Datum) (NULL));
	ResourceArrayInit(&(owner->tupdescarr), (Datum) (NULL));
	ResourceArrayInit(&(owner->snapshotarr), (Datum) (NULL));
	ResourceArrayInit(&(owner->dsmarr), (Datum) (NULL));
	ResourceArrayInit(&(owner->bufferarr), (Datum) (InvalidBuffer));
	ResourceArrayInit(&(owner->filearr), (Datum) (-1));

	return owner;
}

/*
 * ResourceOwnerRelease
 *		Release all resources owned by a ResourceOwner and its descendants,
 *		but don't delete the owner objects themselves.
 *
 * Note that this executes just one phase of release, and so typically
 * must be called three times.  We do it this way because (a) we want to
 * do all the recursion separately for each phase, thereby preserving
 * the needed order of operations; and (b) xact.c may have other operations
 * to do between the phases.
 *
 * phase: release phase to execute
 * isCommit: true for successful completion of a query or transaction,
 *			false for unsuccessful
 * isTopLevel: true if completing a main transaction, else false
 *
 * isCommit is passed because some modules may expect that their resources
 * were all released already if the transaction or portal finished normally.
 * If so it is reasonable to give a warning (NOT an error) should any
 * unreleased resources be present.  When isCommit is false, such warnings
 * are generally inappropriate.
 *
 * isTopLevel is passed when we are releasing TopTransactionResourceOwner
 * at completion of a main transaction.  This generally means that *all*
 * resources will be released, and so we can optimize things a bit.
 */
void
ResourceOwnerRelease(ResourceOwner owner,
					 ResourceReleasePhase phase,
					 bool isCommit,
					 bool isTopLevel)
{
	/* Rather than PG_TRY at every level of recursion, set it up once */
	ResourceOwner save;

	save = CurrentResourceOwner;
	PG_TRY();
	{
		ResourceOwnerReleaseInternal(owner, phase, isCommit, isTopLevel);
	}
	PG_CATCH();
	{
		CurrentResourceOwner = save;
		PG_RE_THROW();
	}
	PG_END_TRY();
	CurrentResourceOwner = save;
}

static void
ResourceOwnerReleaseInternal(ResourceOwner owner,
							 ResourceReleasePhase phase,
							 bool isCommit,
							 bool isTopLevel)
{
	ResourceOwner child;
	ResourceOwner save;
	ResourceReleaseCallbackItem *item;
	Datum		foundres;

	/* Recurse to handle descendants */
	for (child = owner->firstchild; child != NULL; child = child->nextchild)
		ResourceOwnerReleaseInternal(child, phase, isCommit, isTopLevel);

	/*
	 * Make CurrentResourceOwner point to me, so that ReleaseBuffer etc don't
	 * get confused.  We needn't PG_TRY here because the outermost level will
	 * fix it on error abort.
	 */
	save = CurrentResourceOwner;
	CurrentResourceOwner = owner;

	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS)
	{
		/*
		 * Release buffer pins.  Note that ReleaseBuffer will remove the
		 * buffer entry from my list, so I just have to iterate till there are
		 * none.
		 *
		 * During a commit, there shouldn't be any remaining pins --- that
		 * would indicate failure to clean up the executor correctly --- so
		 * issue warnings.  In the abort case, just clean up quietly.
		 */
		while (ResourceArrayGetAny(&(owner->bufferarr), &foundres))
		{
			Buffer		res = (Buffer) foundres;

			if (isCommit)
				PrintBufferLeakWarning(res);
			ReleaseBuffer(res);
		}

		/* Ditto for relcache references. */
		while (ResourceArrayGetAny(&(owner->relrefarr), &foundres))
		{
			Relation	res = (Relation) foundres;

			if (isCommit)
				PrintRelCacheLeakWarning(res);
			RelationClose(res);
		}

		/* Ditto for dynamic shared memory segments */
		while (ResourceArrayGetAny(&(owner->dsmarr), &foundres))
		{
			dsm_segment *res = (dsm_segment *) foundres;

			if (isCommit)
				PrintDSMLeakWarning(res);
			dsm_detach(res);
		}
	}
	else if (phase == RESOURCE_RELEASE_LOCKS)
	{
		if (isTopLevel)
		{
			/*
			 * For a top-level xact we are going to release all locks (or at
			 * least all non-session locks), so just do a single lmgr call at
			 * the top of the recursion.
			 */
			if (owner == TopTransactionResourceOwner)
			{
				ProcReleaseLocks(isCommit);
				ReleasePredicateLocks(isCommit);
			}
		}
		else
		{
			/*
			 * Release locks retail.  Note that if we are committing a
			 * subtransaction, we do NOT release its locks yet, but transfer
			 * them to the parent.
			 */
			LOCALLOCK **locks;
			int			nlocks;

			Assert(owner->parent != NULL);

			/*
			 * Pass the list of locks owned by this resource owner to the lock
			 * manager, unless it has overflowed.
			 */
			if (owner->nlocks > MAX_RESOWNER_LOCKS)
			{
				locks = NULL;
				nlocks = 0;
			}
			else
			{
				locks = owner->locks;
				nlocks = owner->nlocks;
			}

			if (isCommit)
				LockReassignCurrentOwner(locks, nlocks);
			else
				LockReleaseCurrentOwner(locks, nlocks);
		}
	}
	else if (phase == RESOURCE_RELEASE_AFTER_LOCKS)
	{
		/*
		 * Release catcache references.  Note that ReleaseCatCache will remove
		 * the catref entry from my list, so I just have to iterate till there
		 * are none.
		 *
		 * As with buffer pins, warn if any are left at commit time, and
		 * release back-to-front for speed.
		 */
		while (ResourceArrayGetAny(&(owner->catrefarr), &foundres))
		{
			HeapTuple	res = (HeapTuple) foundres;

			if (isCommit)
				PrintCatCacheLeakWarning(res);
			ReleaseCatCache(res);
		}

		/* Ditto for catcache lists */
		while (ResourceArrayGetAny(&(owner->catlistrefarr), &foundres))
		{
			CatCList   *res = (CatCList *) foundres;

			if (isCommit)
				PrintCatCacheListLeakWarning(res);
			ReleaseCatCacheList(res);
		}

		/* Ditto for plancache references */
		while (ResourceArrayGetAny(&(owner->planrefarr), &foundres))
		{
			CachedPlan *res = (CachedPlan *) foundres;

			if (isCommit)
				PrintPlanCacheLeakWarning(res);
			ReleaseCachedPlan(res, true);
		}

		/* Ditto for tupdesc references */
		while (ResourceArrayGetAny(&(owner->tupdescarr), &foundres))
		{
			TupleDesc	res = (TupleDesc) foundres;

			if (isCommit)
				PrintTupleDescLeakWarning(res);
			DecrTupleDescRefCount(res);
		}

		/* Ditto for snapshot references */
		while (ResourceArrayGetAny(&(owner->snapshotarr), &foundres))
		{
			Snapshot	res = (Snapshot) foundres;

			if (isCommit)
				PrintSnapshotLeakWarning(res);
			UnregisterSnapshot(res);
		}

		/* Ditto for temporary files */
		while (ResourceArrayGetAny(&(owner->filearr), &foundres))
		{
			File		res = (File) foundres;

			if (isCommit)
				PrintFileLeakWarning(res);
			FileClose(res);
		}

		/* Clean up index scans too */
		ReleaseResources_hash();
	}

	/* Let add-on modules get a chance too */
	for (item = ResourceRelease_callbacks; item; item = item->next)
		(*item->callback) (phase, isCommit, isTopLevel, item->arg);

	CurrentResourceOwner = save;
}

/*
 * ResourceOwnerDelete
 *		Delete an owner object and its descendants.
 *
 * The caller must have already released all resources in the object tree.
 */
void
ResourceOwnerDelete(ResourceOwner owner)
{
	/* We had better not be deleting CurrentResourceOwner ... */
	Assert(owner != CurrentResourceOwner);

	/* And it better not own any resources, either */
	Assert(owner->nlocks == 0 || owner->nlocks == MAX_RESOWNER_LOCKS + 1);

	/*
	 * Delete children.  The recursive call will delink the child from me, so
	 * just iterate as long as there is a child.
	 */
	while (owner->firstchild != NULL)
		ResourceOwnerDelete(owner->firstchild);

	/*
	 * We delink the owner from its parent before deleting it, so that if
	 * there's an error we won't have deleted/busted owners still attached to
	 * the owner tree.  Better a leak than a crash.
	 */
	ResourceOwnerNewParent(owner, NULL);

	/* And free the object. */
	ResourceArrayFree(&(owner->catrefarr));
	ResourceArrayFree(&(owner->catlistrefarr));
	ResourceArrayFree(&(owner->relrefarr));
	ResourceArrayFree(&(owner->planrefarr));
	ResourceArrayFree(&(owner->tupdescarr));
	ResourceArrayFree(&(owner->snapshotarr));
	ResourceArrayFree(&(owner->dsmarr));
	ResourceArrayFree(&(owner->bufferarr));
	ResourceArrayFree(&(owner->filearr));
	pfree(owner);
}

/*
 * Fetch parent of a ResourceOwner (returns NULL if top-level owner)
 */
ResourceOwner
ResourceOwnerGetParent(ResourceOwner owner)
{
	return owner->parent;
}

/*
 * Reassign a ResourceOwner to have a new parent
 */
void
ResourceOwnerNewParent(ResourceOwner owner,
					   ResourceOwner newparent)
{
	ResourceOwner oldparent = owner->parent;

	if (oldparent)
	{
		if (owner == oldparent->firstchild)
			oldparent->firstchild = owner->nextchild;
		else
		{
			ResourceOwner child;

			for (child = oldparent->firstchild; child; child = child->nextchild)
			{
				if (owner == child->nextchild)
				{
					child->nextchild = owner->nextchild;
					break;
				}
			}
		}
	}

	if (newparent)
	{
		Assert(owner != newparent);
		owner->parent = newparent;
		owner->nextchild = newparent->firstchild;
		newparent->firstchild = owner;
	}
	else
	{
		owner->parent = NULL;
		owner->nextchild = NULL;
	}
}

/*
 * Register or deregister callback functions for resource cleanup
 *
 * These functions are intended for use by dynamically loaded modules.
 * For built-in modules we generally just hardwire the appropriate calls.
 *
 * Note that the callback occurs post-commit or post-abort, so the callback
 * functions can only do noncritical cleanup.
 */
void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	ResourceReleaseCallbackItem *item;

	item = (ResourceReleaseCallbackItem *)
		MemoryContextAlloc(TopMemoryContext,
						   sizeof(ResourceReleaseCallbackItem));
	item->callback = callback;
	item->arg = arg;
	item->next = ResourceRelease_callbacks;
	ResourceRelease_callbacks = item;
}

void
UnregisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	ResourceReleaseCallbackItem *item;
	ResourceReleaseCallbackItem *prev;

	prev = NULL;
	for (item = ResourceRelease_callbacks; item; prev = item, item = item->next)
	{
		if (item->callback == callback && item->arg == arg)
		{
			if (prev)
				prev->next = item->next;
			else
				ResourceRelease_callbacks = item->next;
			pfree(item);
			break;
		}
	}
}


/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * buffer array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 *
 * We allow the case owner == NULL because the bufmgr is sometimes invoked
 * outside any transaction (for example, during WAL recovery).
 */
void
ResourceOwnerEnlargeBuffers(ResourceOwner owner)
{
	if (owner == NULL)
		return;
	ResourceArrayEnlarge(&(owner->bufferarr));
}

/*
 * Remember that a buffer pin is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeBuffers()
 *
 * We allow the case owner == NULL because the bufmgr is sometimes invoked
 * outside any transaction (for example, during WAL recovery).
 */
void
ResourceOwnerRememberBuffer(ResourceOwner owner, Buffer buffer)
{
	if (owner == NULL)
		return;
	ResourceArrayAdd(&(owner->bufferarr), (Datum) buffer);
}

/*
 * Forget that a buffer pin is owned by a ResourceOwner
 *
 * We allow the case owner == NULL because the bufmgr is sometimes invoked
 * outside any transaction (for example, during WAL recovery).
 */
void
ResourceOwnerForgetBuffer(ResourceOwner owner, Buffer buffer)
{
	bool		res;

	if (owner == NULL)
		return;

	res = ResourceArrayRemove(&(owner->bufferarr), (Datum) buffer);
	if (!res)
		elog(ERROR, "buffer %d is not owned by resource owner %s",
			 buffer, owner->name);
}

/*
 * Remember that a Local Lock is owned by a ResourceOwner
 *
 * This is different from the other Remember functions in that the list of
 * locks is only a lossy cache. It can hold up to MAX_RESOWNER_LOCKS entries,
 * and when it overflows, we stop tracking locks. The point of only remembering
 * only up to MAX_RESOWNER_LOCKS entries is that if a lot of locks are held,
 * ResourceOwnerForgetLock doesn't need to scan through a large array to find
 * the entry.
 */
void
ResourceOwnerRememberLock(ResourceOwner owner, LOCALLOCK *locallock)
{
	Assert(locallock != NULL);

	if (owner->nlocks > MAX_RESOWNER_LOCKS)
		return;					/* we have already overflowed */

	if (owner->nlocks < MAX_RESOWNER_LOCKS)
		owner->locks[owner->nlocks] = locallock;
	else
	{
		/* overflowed */
	}
	owner->nlocks++;
}

/*
 * Forget that a Local Lock is owned by a ResourceOwner
 */
void
ResourceOwnerForgetLock(ResourceOwner owner, LOCALLOCK *locallock)
{
	int			i;

	if (owner->nlocks > MAX_RESOWNER_LOCKS)
		return;					/* we have overflowed */

	Assert(owner->nlocks > 0);
	for (i = owner->nlocks - 1; i >= 0; i--)
	{
		if (locallock == owner->locks[i])
		{
			owner->locks[i] = owner->locks[owner->nlocks - 1];
			owner->nlocks--;
			return;
		}
	}
	elog(ERROR, "lock reference %p is not owned by resource owner %s",
		 locallock, owner->name);
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * catcache reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void
ResourceOwnerEnlargeCatCacheRefs(ResourceOwner owner)
{
	ResourceArrayEnlarge(&(owner->catrefarr));
}

/*
 * Remember that a catcache reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeCatCacheRefs()
 */
void
ResourceOwnerRememberCatCacheRef(ResourceOwner owner, HeapTuple tuple)
{
	ResourceArrayAdd(&(owner->catrefarr), (Datum) tuple);
}

/*
 * Forget that a catcache reference is owned by a ResourceOwner
 */
void
ResourceOwnerForgetCatCacheRef(ResourceOwner owner, HeapTuple tuple)
{
	bool		res = ResourceArrayRemove(&(owner->catrefarr),
										  (Datum) tuple);

	if (!res)
		elog(ERROR, "catcache reference %p is not owned by resource owner %s",
			 tuple, owner->name);
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * catcache-list reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void
ResourceOwnerEnlargeCatCacheListRefs(ResourceOwner owner)
{
	ResourceArrayEnlarge(&(owner->catlistrefarr));
}

/*
 * Remember that a catcache-list reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeCatCacheListRefs()
 */
void
ResourceOwnerRememberCatCacheListRef(ResourceOwner owner, CatCList *list)
{
	ResourceArrayAdd(&(owner->catlistrefarr), (Datum) list);
}

/*
 * Forget that a catcache-list reference is owned by a ResourceOwner
 */
void
ResourceOwnerForgetCatCacheListRef(ResourceOwner owner, CatCList *list)
{
	bool		res = ResourceArrayRemove(&(owner->catlistrefarr),
										  (Datum) list);

	if (!res)
		elog(ERROR, "catcache list reference %p is not owned by resource owner %s",
			 list, owner->name);
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * relcache reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void
ResourceOwnerEnlargeRelationRefs(ResourceOwner owner)
{
	ResourceArrayEnlarge(&(owner->relrefarr));
}

/*
 * Remember that a relcache reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeRelationRefs()
 */
void
ResourceOwnerRememberRelationRef(ResourceOwner owner, Relation rel)
{
	ResourceArrayAdd(&(owner->relrefarr), (Datum) rel);
}

/*
 * Forget that a relcache reference is owned by a ResourceOwner
 */
void
ResourceOwnerForgetRelationRef(ResourceOwner owner, Relation rel)
{
	bool		res = ResourceArrayRemove(&(owner->relrefarr),
										  (Datum) rel);

	if (!res)
		elog(ERROR, "relcache reference %s is not owned by resource owner %s",
			 RelationGetRelationName(rel), owner->name);
}

/*
 * Debugging subroutine
 */
static void
PrintRelCacheLeakWarning(Relation rel)
{
	elog(WARNING, "relcache reference leak: relation \"%s\" not closed",
		 RelationGetRelationName(rel));
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * plancache reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void
ResourceOwnerEnlargePlanCacheRefs(ResourceOwner owner)
{
	ResourceArrayEnlarge(&(owner->planrefarr));
}

/*
 * Remember that a plancache reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargePlanCacheRefs()
 */
void
ResourceOwnerRememberPlanCacheRef(ResourceOwner owner, CachedPlan *plan)
{
	ResourceArrayAdd(&(owner->planrefarr), (Datum) plan);
}

/*
 * Forget that a plancache reference is owned by a ResourceOwner
 */
void
ResourceOwnerForgetPlanCacheRef(ResourceOwner owner, CachedPlan *plan)
{
	bool		res = ResourceArrayRemove(&(owner->planrefarr),
										  (Datum) plan);

	if (!res)
		elog(ERROR, "plancache reference %p is not owned by resource owner %s",
			 plan, owner->name);
}

/*
 * Debugging subroutine
 */
static void
PrintPlanCacheLeakWarning(CachedPlan *plan)
{
	elog(WARNING, "plancache reference leak: plan %p not closed", plan);
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * tupdesc reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void
ResourceOwnerEnlargeTupleDescs(ResourceOwner owner)
{
	ResourceArrayEnlarge(&(owner->tupdescarr));
}

/*
 * Remember that a tupdesc reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeTupleDescs()
 */
void
ResourceOwnerRememberTupleDesc(ResourceOwner owner, TupleDesc tupdesc)
{
	ResourceArrayAdd(&(owner->tupdescarr), (Datum) tupdesc);
}

/*
 * Forget that a tupdesc reference is owned by a ResourceOwner
 */
void
ResourceOwnerForgetTupleDesc(ResourceOwner owner, TupleDesc tupdesc)
{
	bool		res = ResourceArrayRemove(&(owner->tupdescarr),
										  (Datum) tupdesc);

	if (!res)
		elog(ERROR, "tupdesc reference %p is not owned by resource owner %s",
			 tupdesc, owner->name);
}

/*
 * Debugging subroutine
 */
static void
PrintTupleDescLeakWarning(TupleDesc tupdesc)
{
	elog(WARNING,
		 "TupleDesc reference leak: TupleDesc %p (%u,%d) still referenced",
		 tupdesc, tupdesc->tdtypeid, tupdesc->tdtypmod);
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * snapshot reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void
ResourceOwnerEnlargeSnapshots(ResourceOwner owner)
{
	ResourceArrayEnlarge(&(owner->snapshotarr));
}

/*
 * Remember that a snapshot reference is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeSnapshots()
 */
void
ResourceOwnerRememberSnapshot(ResourceOwner owner, Snapshot snapshot)
{
	ResourceArrayAdd(&(owner->snapshotarr), (Datum) snapshot);
}

/*
 * Forget that a snapshot reference is owned by a ResourceOwner
 */
void
ResourceOwnerForgetSnapshot(ResourceOwner owner, Snapshot snapshot)
{
	bool		res = ResourceArrayRemove(&(owner->snapshotarr),
										  (Datum) snapshot);

	if (!res)
		elog(ERROR, "snapshot reference %p is not owned by resource owner %s",
			 snapshot, owner->name);
}

/*
 * Debugging subroutine
 */
static void
PrintSnapshotLeakWarning(Snapshot snapshot)
{
	elog(WARNING,
		 "Snapshot reference leak: Snapshot %p still referenced",
		 snapshot);
}


/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * files reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void
ResourceOwnerEnlargeFiles(ResourceOwner owner)
{
	ResourceArrayEnlarge(&(owner->filearr));
}

/*
 * Remember that a temporary file is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeFiles()
 */
void
ResourceOwnerRememberFile(ResourceOwner owner, File file)
{
	ResourceArrayAdd(&(owner->filearr), (Datum) file);
}

/*
 * Forget that a temporary file is owned by a ResourceOwner
 */
void
ResourceOwnerForgetFile(ResourceOwner owner, File file)
{
	bool		res = ResourceArrayRemove(&(owner->filearr),
										  (Datum) file);

	if (!res)
		elog(ERROR, "temporary file %d is not owned by resource owner %s",
			 file, owner->name);
}


/*
 * Debugging subroutine
 */
static void
PrintFileLeakWarning(File file)
{
	elog(WARNING,
		 "temporary file leak: File %d still referenced",
		 file);
}

/*
 * Make sure there is room for at least one more entry in a ResourceOwner's
 * dynamic shmem segment reference array.
 *
 * This is separate from actually inserting an entry because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 */
void
ResourceOwnerEnlargeDSMs(ResourceOwner owner)
{
	ResourceArrayEnlarge(&(owner->dsmarr));
}

/*
 * Remember that a dynamic shmem segment is owned by a ResourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlargeDSMs()
 */
void
ResourceOwnerRememberDSM(ResourceOwner owner, dsm_segment *seg)
{
	ResourceArrayAdd(&(owner->dsmarr), (Datum) seg);
}

/*
 * Forget that a dynamic shmem segment is owned by a ResourceOwner
 */
void
ResourceOwnerForgetDSM(ResourceOwner owner, dsm_segment *seg)
{
	bool		res = ResourceArrayRemove(&(owner->dsmarr),
										  (Datum) seg);

	if (!res)
		elog(ERROR, "dynamic shared memory segment %u is not owned by resource"
			 " owner %s", dsm_segment_handle(seg), owner->name);
}


/*
 * Debugging subroutine
 */
static void
PrintDSMLeakWarning(dsm_segment *seg)
{
	elog(WARNING,
		 "dynamic shared memory leak: segment %u still referenced",
		 dsm_segment_handle(seg));
}
