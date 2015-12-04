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
	load_part_relations_hashtable();
	// load_hash_restrictions_hashtable();
	// load_range_restrictions_hashtable();
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
	ret = SPI_exec("SELECT pg_class.relfilenode, pg_attribute.attnum, pg_pathman_rels.parttype "
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
			prinfo = (PartRelationInfo*)hash_search(relations, (const void *)&oid, HASH_ENTER, NULL);
			prinfo->oid = oid;
			prinfo->attnum = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));
			prinfo->parttype = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, &isnull));

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
			prel->children[prel->children_count++] = child_oid;
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
								"rr.min_int, rr.max_int, "
								"rr.min_dt - '1 microsecond'::INTERVAL, "
								"rr.max_dt - '1 microsecond'::INTERVAL "
								"FROM pg_pathman_range_rels rr "
								"JOIN pg_class p ON p.relname = rr.parent "
								"JOIN pg_class c ON c.relname = rr.child "
								"WHERE p.relfilenode = $1 "
								"ORDER BY rr.parent, rr.min_int, rr.min_dt",
								1, oids, vals, nulls, true, 0);
	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL)
    {
    	TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;

        for (i=0; i<proc; i++)
        {
        	Datum min;
        	Datum max;
			RangeEntry re;
            HeapTuple tuple = tuptable->vals[i];

			// int parent_oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &arg1_isnull));
			re.child_oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 2, &arg1_isnull));

			rangerel = (RangeRelation *)
				hash_search(range_restrictions, (void *) &parent_oid, HASH_ENTER, &found);

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

			re.min = SPI_getbinval(tuple, tupdesc, 3, &arg1_isnull);
			re.max = SPI_getbinval(tuple, tupdesc, 4, &arg2_isnull);
			prel->atttype = AT_INT;

			if (arg1_isnull || arg2_isnull)
			{
				re.min = SPI_getbinval(tuple, tupdesc, 5, &arg1_isnull);
				re.max = SPI_getbinval(tuple, tupdesc, 6, &arg2_isnull);
				prel->atttype = AT_DATE;

				if (arg1_isnull || arg2_isnull)
					ereport(ERROR, (errmsg("Range relation should be of type either INTEGER or DATE")));
			}
			rangerel->ranges[rangerel->nranges++] = re;

			prel->children[prel->children_count++] = re.child_oid;
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
									   16, 16,
									   &ctl, HASH_ELEM | HASH_BLOBS);
}
