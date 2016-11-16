/*
 * dict.c
 *
 *  Created on: 18 мая 2015 г.
 *      Author: smagen
 */

#include "postgres.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_jsonbc_dict.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/json_generic.h"
#include "utils/jsonbc_dict.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#define JSONBC_DICT_TAB "pg_jsonbc_dict"

void
jsonbcDictRemoveEntryById(JsonbcDictId dict, JsonbcKeyId key)
{
	Relation	relation;
	HeapTuple	tup;

	relation = heap_open(JsonbcDictionaryRelationId, RowExclusiveLock);

	tup = SearchSysCache2(JSONBCDICTID,
						  JsonbcDictIdGetDatum(dict),
						  JsonbcKeyIdGetDatum(key));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for jsonbc dictionary entry %u %d",
			 dict, key);

	CatalogTupleDelete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}

static JsonbcKeyId
jsonbcDictInsertEntry(JsonbcDictId dict, JsonbcKeyName name,
					  JsonbcKeyId nextKeyId)
{
	Relation	rel;
	bool		nulls[Natts_pg_jsonbc_dict];
	Datum		values[Natts_pg_jsonbc_dict];
	HeapTuple	tup;
	ObjectAddress myself;
	ObjectAddress referenced;

	/*
	 * Insert tuple into pg_jsonbc_dict.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_jsonbc_dict_dict - 1] = JsonbcDictIdGetDatum(dict);
	values[Anum_pg_jsonbc_dict_id - 1] =  JsonbcKeyIdGetDatum(nextKeyId);
	values[Anum_pg_jsonbc_dict_name - 1] =
			PointerGetDatum(cstring_to_text_with_len(name.s, name.len));

	rel = heap_open(JsonbcDictionaryRelationId, RowExclusiveLock);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	CatalogTupleInsert(rel, tup);

	heap_close(rel, RowExclusiveLock);

	heap_freetuple(tup);

	ObjectAddressSubSet(myself, JsonbcDictionaryRelationId, dict, nextKeyId);
	ObjectAddressSet(referenced, RelationRelationId, dict);

	recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);

	return nextKeyId;
}

JsonbcKeyId
jsonbcDictGetIdByNameSeqCached(JsonbcDictId dict, JsonbcKeyName name)
{
	text	   *txt = cstring_to_text_with_len(name.s, name.len);
	HeapTuple	tuple = SearchSysCache2(JSONBCDICTNAME,
										JsonbcDictIdGetDatum(dict),
										PointerGetDatum(txt));
	JsonbcKeyId	id;

	if (HeapTupleIsValid(tuple))
	{
		id = ((Form_pg_jsonbc_dict) GETSTRUCT(tuple))->id;
		ReleaseSysCache(tuple);
	}
	else
		id = JsonbcInvalidKeyId;

	pfree(txt);

	return id;
}

static void
jsonbcDictInvalidateCache(JsonbcDictId dict, JsonbcKeyName name)
{
	text	   *txt = cstring_to_text_with_len(name.s, name.len);
	uint32		hash = GetSysCacheHashValue(JSONBCDICTNAME,
											JsonbcDictIdGetDatum(dict),
											PointerGetDatum(txt),
											PointerGetDatum(NULL),
											PointerGetDatum(NULL));
	pfree(txt);

	CatalogCacheIdInvalidate(JSONBCDICTNAME, hash);
}

static JsonbcKeyId
jsonbcDictGetIdByNameSeq(JsonbcDictId dict, JsonbcKeyName name, bool insert)
{
	JsonbcKeyId	id = jsonbcDictGetIdByNameSeqCached(dict, name);

	if (id == JsonbcInvalidKeyId && insert)
	{
		JsonbcKeyId nextKeyId = (JsonbcKeyId) nextval_internal(dict, false);

		id = jsonbcDictWorkerGetIdByName(dict, name, nextKeyId);

		jsonbcDictInvalidateCache(dict, name);
	}

	return id;
}

static JsonbcKeyId
jsonbcDictGetIdByNameEnum(JsonbcDictId dict, JsonbcKeyName name)
{
	Oid			enumOid = JsonbcDictIdGetEnumOid(dict);
	NameData	nameData;
	HeapTuple	tuple;
	JsonbcKeyId	id;

	if (name.len >= NAMEDATALEN)
		return JsonbcInvalidKeyId;

	memcpy(NameStr(nameData), name.s, name.len);
	NameStr(nameData)[name.len] = '\0';

	tuple = SearchSysCache2(ENUMTYPOIDNAME,
							ObjectIdGetDatum(enumOid),
							NameGetDatum(&nameData));

	if (!HeapTupleIsValid(tuple))
		return JsonbcInvalidKeyId;

	id = HeapTupleGetOid(tuple);

	ReleaseSysCache(tuple);

	return id;
}

JsonbcKeyId
jsonbcDictGetIdByName(JsonbcDictId dict, JsonbcKeyName name, bool insert)
{
	return JsonbcDictIdIsEnum(dict)
				? jsonbcDictGetIdByNameEnum(dict, name)
				: jsonbcDictGetIdByNameSeq(dict, name, insert);
}

static JsonbcKeyName
jsonbcDictGetNameByIdSeq(JsonbcDictId dict, JsonbcKeyId id)
{
	JsonbcKeyName	name;
	HeapTuple		tuple = SearchSysCache2(JSONBCDICTID,
											JsonbcDictIdGetDatum(dict),
											JsonbcKeyIdGetDatum(id));
	if (HeapTupleIsValid(tuple))
	{
		text   *text = &((Form_pg_jsonbc_dict) GETSTRUCT(tuple))->name;

		name.len = VARSIZE_ANY_EXHDR(text);
		name.s = memcpy(palloc(name.len), VARDATA_ANY(text), name.len);

		ReleaseSysCache(tuple);
	}
	else
	{
		name.s = NULL;
		name.len = 0;
	}

	return name;
}

static JsonbcKeyName
jsonbcDictGetNameByIdEnum(JsonbcDictId dict, JsonbcKeyId id)
{
	JsonbcKeyName	name;
	HeapTuple		tuple = SearchSysCache1(ENUMOID,
											ObjectIdGetDatum((Oid) id));

	if (HeapTupleIsValid(tuple))
	{
		Form_pg_enum	enumTuple = (Form_pg_enum) GETSTRUCT(tuple);
		Name			label = &enumTuple->enumlabel;

		Assert(JsonbcDictIdGetEnumOid(dict) == enumTuple->enumtypid);

		name.len = strlen(NameStr(*label));
		name.s = memcpy(palloc(name.len), NameStr(*label), name.len);

		ReleaseSysCache(tuple);
	}
	else
	{
		name.s = NULL;
		name.len = 0;
	}

	return name;
}

JsonbcKeyName
jsonbcDictGetNameById(JsonbcDictId dict, JsonbcKeyId id)
{
	return JsonbcDictIdIsEnum(dict) ? jsonbcDictGetNameByIdEnum(dict, id)
									: jsonbcDictGetNameByIdSeq(dict, id);
}

#ifndef JSONBC_DICT_UPSERT
JsonbcKeyId
jsonbcDictGetIdByNameSlow(JsonbcDictId dict, JsonbcKeyName name,
						  JsonbcKeyId nextKeyId)
{
	return jsonbcDictInsertEntry(dict, name, nextKeyId);
}

#else
static SPIPlanPtr savedPlanInsert = NULL;

JsonbcKeyId
jsonbcDictGetIdByNameSlow(JsonbcDictId dict, JsonbcKeyName name,
						  JsonbcKeyId nextKeyId)
{
	Oid		argTypes[3] = {JsonbcDictIdTypeOid, TEXTOID, JsonbcKeyIdTypeOid};
	Datum	args[3];
	JsonbcKeyId	id;
	bool	null;

	SPI_connect();

	if (!savedPlanInsert)
	{
		savedPlanInsert = SPI_prepare(
			"INSERT INTO "JSONBC_DICT_TAB" (dict, name, id) VALUES ($1, $2, $3)"
			" ON CONFLICT (dict, name) DO UPDATE SET id = "JSONBC_DICT_TAB".id"
			" RETURNING id;",
			lengthof(argTypes), argTypes);
		if (!savedPlanInsert)
			elog(ERROR, "Error preparing query");
		if (SPI_keepplan(savedPlanInsert))
			elog(ERROR, "Error keeping plan");
	}

	args[0] = JsonbcDictIdGetDatum(dict);
	args[1] = PointerGetDatum(cstring_to_text_with_len(name.s, name.len));
	args[2] = JsonbcKeyIdGetDatum(nextKeyId);

	if (SPI_execute_plan(savedPlanInsert, args, NULL, false, 1) < 0 ||
		SPI_processed != 1)
		elog(ERROR, "Failed to insert into dictionary");

	id = DatumGetJsonbcKeyId(SPI_getbinval(SPI_tuptable->vals[0],
										   SPI_tuptable->tupdesc, 1, &null));

	SPI_finish();

	return id;
}
#endif

void
jsonbcDictAddRef(Form_pg_attribute attr, JsonbcDictId dict)
{
	ObjectAddress	dep;
	ObjectAddress	ref;

	ObjectAddressSubSet(dep, RelationRelationId, attr->attrelid, attr->attnum);

	if (JsonbcDictIdIsEnum(dict))
		ObjectAddressSet(ref, TypeRelationId, JsonbcDictIdGetEnumOid(dict));
	else
		ObjectAddressSet(ref, RelationRelationId, dict);

	recordDependencyOn(&dep, &ref, DEPENDENCY_NORMAL);
}

static bool
jsonbcFindOtherDictReferences(JsonbcDictId dict, Oid attrelid, int16 attnum)
{
	Relation	depRel = heap_open(DependRelationId, AccessShareLock);
	ScanKeyData	keys[2];
	SysScanDesc	scan;
	HeapTuple	tup;
	bool		found = false;

	ScanKeyInit(&keys[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&keys[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_INT4EQ,
				ObjectIdGetDatum(dict));

	scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL,
							  2, keys);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend depend = (Form_pg_depend) GETSTRUCT(tup);

		if (depend->classid == RelationRelationId &&
			(depend->objid != attrelid || depend->objsubid != attnum))
		{
			found = true;
			break;
		}
	}

	systable_endscan(scan);

	heap_close(depRel, AccessShareLock);

	return found;
}

void
jsonbcDictRemoveRef(Form_pg_attribute att, JsonbcDictId dict)
{
	long	totalCount;
	int		cnt;

	if (att->attisdropped)
		return;

	if (JsonbcDictIdIsEnum(dict))
	{
		deleteDependencyRecordsForClass(RelationRelationId,
										att->attrelid, att->attnum,
										TypeRelationId, DEPENDENCY_NORMAL);
		return;
	}

	deleteDependencyRecordsForClass(RelationRelationId,
									att->attrelid, att->attnum,
									RelationRelationId, DEPENDENCY_NORMAL);

	cnt = changeDependencyFor(RelationRelationId, dict,
							  RelationRelationId, att->attrelid, att->attnum,
							  InvalidOid, &totalCount);

	Assert(cnt <= 1);

	if (cnt == totalCount && cnt == 1 &&
		!jsonbcFindOtherDictReferences(dict, att->attrelid, att->attnum))
	{
		ObjectAddress seqaddr;

		ObjectAddressSet(seqaddr, RelationRelationId, dict);
		CommandCounterIncrement(); /* FIXME */
		performDeletion(&seqaddr, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
	}
}

JsonbcDictId
jsonbcDictCreate(Form_pg_attribute attr)
{
	CreateSeqStmt	   *stmt = makeNode(CreateSeqStmt);
	Relation			rel;
	List			   *attnamelist;
	char			   *name;
	char			   *namespace;
	Oid					namespaceid;
	ObjectAddress		addr;

	rel = relation_open(attr->attrelid, NoLock);

	namespaceid = RelationGetNamespace(rel);
	namespace = get_namespace_name(namespaceid);
	name = ChooseRelationName(RelationGetRelationName(rel),
							  NameStr(attr->attname),
							  "jsonbc_dict_seq",
							  namespaceid);

	stmt->sequence = makeRangeVar(namespace, name, -1);
	stmt->ownerId = rel->rd_rel->relowner;
	attnamelist = list_make3(makeString(namespace),
							 makeString(RelationGetRelationName(rel)),
							 makeString(NameStr(attr->attname)));
	stmt->options = list_make1(makeDefElem("owned_by", (Node *) attnamelist, -1));
	stmt->for_identity = true;
	stmt->if_not_exists = false;

	addr = DefineSequence(NULL, stmt);

	relation_close(rel, NoLock);

	return addr.objectId;
}

PG_FUNCTION_INFO_V1(jsonbc_get_id_by_name);
PG_FUNCTION_INFO_V1(jsonbc_get_name_by_id);

Datum
jsonbc_get_id_by_name(PG_FUNCTION_ARGS)
{
	JsonbcDictId	dict = DatumGetJsonbcDictId(PG_GETARG_DATUM(0));
	text		   *nameText = PG_GETARG_TEXT_PP(1);
	JsonbcKeyId		id;
	JsonbcKeyName	name;

	name.s = VARDATA_ANY(nameText);
	name.len = VARSIZE_ANY_EXHDR(nameText);
	id = jsonbcDictGetIdByName(dict, name, false);

	PG_RETURN_DATUM(JsonbcKeyIdGetDatum(id));
}

Datum
jsonbc_get_name_by_id(PG_FUNCTION_ARGS)
{
	JsonbcDictId	dict = DatumGetJsonbcDictId(PG_GETARG_DATUM(0));
	JsonbcKeyId		id = PG_GETARG_INT32(1);
	JsonbcKeyName	name;

	name = jsonbcDictGetNameById(dict, id);
	if (name.s)
		PG_RETURN_TEXT_P(cstring_to_text_with_len(name.s, name.len));
	else
		PG_RETURN_NULL();
}
