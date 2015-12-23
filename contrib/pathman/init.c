#include "pathman.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"

HTAB   *relations = NULL;
HTAB   *hash_restrictions = NULL;
HTAB   *range_restrictions = NULL;
bool	initialization_needed = true;

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

	/* if hashtable is empty */
	if (hash_get_num_entries(relations) == 0)
	{
		SPI_connect();
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

			switch(prinfo->parttype)
			{
				case PT_RANGE:
					load_range_restrictions(oid);
					break;
				case PT_HASH:
					load_hash_restrictions(oid);
					break;
			}
		}
		SPI_finish();
	}
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
							  32, 32,
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
									  128, 128,
									  &ctl, HASH_ELEM | HASH_BLOBS);
}

void
load_range_restrictions(Oid parent_oid)
{
	bool		found;
	PartRelationInfo *prel;
	RangeRelation *rangerel;
	// HashRelation *hashrel;
	// HashRelationKey key;
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

	/* if already loaded then quit */
	if (prel->children_count > 0)
		return;

	// SPI_connect();
	ret = SPI_execute_with_args("SELECT p.relfilenode, c.relfilenode, "
								"rr.min_num, rr.max_num, "
								"rr.min_dt, "
								"rr.max_dt - '1 microsecond'::INTERVAL, "
								"rr.min_dt::DATE, "
								"(rr.max_dt - '1 day'::INTERVAL)::DATE, "
								"rr.min_num::INTEGER, "
								"rr.max_num::INTEGER - 1 "
								"FROM pg_pathman_range_rels rr "
								"JOIN pg_class p ON p.relname = rr.parent "
								"JOIN pg_class c ON c.relname = rr.child "
								"WHERE p.relfilenode = $1 "
								"ORDER BY rr.parent, rr.min_num, rr.min_dt",
								1, oids, vals, nulls, true, 0);
	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL)
    {
    	TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        Oid *children;
        RangeEntry *ranges;

		rangerel = (RangeRelation *)
			hash_search(range_restrictions, (void *) &parent_oid, HASH_ENTER, &found);
		rangerel->nranges = 0;

        alloc_dsm_array(&prel->children, sizeof(Oid), proc);
        children = (Oid *) dsm_array_get_pointer(&prel->children);

        alloc_dsm_array(&rangerel->ranges, sizeof(RangeEntry), proc);
        ranges = (RangeEntry *) dsm_array_get_pointer(&rangerel->ranges);

        for (i=0; i<proc; i++)
        {
        	Datum min;
        	Datum max;
			RangeEntry re;
            HeapTuple tuple = tuptable->vals[i];

			// int parent_oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &arg1_isnull));
			re.child_oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 2, &arg1_isnull));

			/* date */
			// switch (prinfo->atttype)
			// {
			// 	case AT_INT:
			// 		min = SPI_getbinval(tuple, tupdesc, 3, &isnull);
			// 		max = SPI_getbinval(tuple, tupdesc, 4, &isnull);
			// 		re.min.integer = DatumGetInt32(min);
			// 		re.max.integer = DatumGetInt32(max);
			// 		break;
			// 	case AT_DATE:
					// min = SPI_getbinval(tuple, tupdesc, 5, &isnull);
					// max = SPI_getbinval(tuple, tupdesc, 6, &isnull);
					// re.min.date = DatumGetDateADT(min);
					// re.max.date = DatumGetDateADT(max);
			// 		break;
			// }

			switch(prel->atttype)
			{
				case DATEOID:
					re.min = SPI_getbinval(tuple, tupdesc, 7, &arg1_isnull);
					re.max = SPI_getbinval(tuple, tupdesc, 8, &arg2_isnull);
					break;
				case TIMESTAMPOID:
					re.min = SPI_getbinval(tuple, tupdesc, 5, &arg1_isnull);
					re.max = SPI_getbinval(tuple, tupdesc, 6, &arg2_isnull);
					break;
				case INT2OID:
				case INT4OID:
				case INT8OID:
					re.min = SPI_getbinval(tuple, tupdesc, 9, &arg1_isnull);
					re.max = SPI_getbinval(tuple, tupdesc, 10, &arg2_isnull);
					break;
				default:
					re.min = SPI_getbinval(tuple, tupdesc, 3, &arg1_isnull);
					re.max = SPI_getbinval(tuple, tupdesc, 4, &arg2_isnull);
					break;
			}

			ranges[rangerel->nranges++] = re;
			// prel->children[prel->children_count++] = re.child_oid;
			children[prel->children_count++] = re.child_oid;
        }
    }

	// SPI_finish();
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
									   512, 512,
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
