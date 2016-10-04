/*-------------------------------------------------------------------------
 *
 * compressioncmds.c
 *	  Routines for SQL commands that manipulate compression methods.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/compressioncmds.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/compression.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_compression.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Convert a handler function name to an Oid.  If the return type of the
 * function doesn't match the given AM type, an error is raised.
 *
 * This function either return valid function Oid or throw an error.
 */
static Oid
LookupCompressionHandlerFunc(List *handlerName)
{
	static const Oid	funcargtypes[1] = {INTERNALOID};
	Oid					handlerOid;

	if (handlerName == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("handler function is not specified")));

	/* handlers have one argument of type internal */
	handlerOid = LookupFuncName(handlerName, 1, funcargtypes, false);

	/* check that handler has the correct return type */
	if (get_func_rettype(handlerOid) != COMPRESSION_HANDLEROID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function %s must return type %s",
						NameListToString(handlerName),
						"compression_handler")));

	return handlerOid;
}

static ObjectAddress
CreateCompressionMethod(char *cmName, List *handlerName)
{
	Relation	rel;
	ObjectAddress myself;
	ObjectAddress referenced;
	Oid			cmoid;
	Oid			cmhandler;
	bool		nulls[Natts_pg_compression];
	Datum		values[Natts_pg_compression];
	HeapTuple	tup;

	rel = heap_open(CompressionMethodRelationId, RowExclusiveLock);

	/* Check if name is used */
	cmoid = GetSysCacheOid1(COMPRESSIONMETHODNAME, CStringGetDatum(cmName));
	if (OidIsValid(cmoid))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("compression method \"%s\" already exists",
						cmName)));

	/*
	 * Get the handler function oid.
	 */
	cmhandler = LookupCompressionHandlerFunc(handlerName);

	/*
	 * Insert tuple into pg_compression.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_compression_cmname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(cmName));
	values[Anum_pg_compression_cmhandler - 1] = ObjectIdGetDatum(cmhandler);
	values[Anum_pg_compression_cmtype - 1] = '\0';

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	cmoid = CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, CompressionMethodRelationId, cmoid);

	/* Record dependency on handler function */
	ObjectAddressSet(referenced, ProcedureRelationId, cmhandler);

	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	recordDependencyOnCurrentExtension(&myself, false);

	heap_close(rel, RowExclusiveLock);

	return myself;
}

ObjectAddress
DefineCompressionMethod(List *names, List *parameters)
{
	char	   *cmName;
	ListCell   *pl;
	DefElem    *handlerEl = NULL;

	if (list_length(names) != 1)
		elog(ERROR, "compression method name cannot be qualified");

	cmName = strVal(linitial(names));

	/* Must be super user */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create compression method \"%s\"",
						cmName),
				 errhint("Must be superuser to create an compression method.")));

	foreach(pl, parameters)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);
		DefElem   **defelp;

		if (pg_strcasecmp(defel->defname, "handler") == 0)
			defelp = &handlerEl;
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("compression method attribute \"%s\" not recognized",
							defel->defname)));
			break;
		}

		*defelp = defel;
	}

	if (!handlerEl)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("compression method handler is not specified")));

	return CreateCompressionMethod(cmName, (List *) handlerEl->arg);
}

void
RemoveCompressionMethodById(Oid cmOid)
{
	Relation	relation;
	HeapTuple	tup;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drop compression methods")));

	relation = heap_open(CompressionMethodRelationId, RowExclusiveLock);

	tup = SearchSysCache1(COMPRESSIONMETHODOID, ObjectIdGetDatum(cmOid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for compression method %u", cmOid);

	CatalogTupleDelete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}

/*
 * get_compression_method_oid
 *
 * If missing_ok is false, throw an error if compression method not found.
 * If missing_ok is true, just return InvalidOid.
 */
Oid
get_compression_method_oid(const char *cmname, bool missing_ok)
{
	HeapTuple	tup;
	Oid			oid = InvalidOid;

	tup = SearchSysCache1(COMPRESSIONMETHODNAME, CStringGetDatum(cmname));
	if (HeapTupleIsValid(tup))
	{
		oid = HeapTupleGetOid(tup);
		ReleaseSysCache(tup);
	}

	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("compression method \"%s\" does not exist", cmname)));

	return oid;
}

/*
 * get_compression_method_name
 *
 * given an compression method OID name, look up its name.
 */
char *
get_compression_method_name(Oid cmOid)
{
	HeapTuple	tup;
	char	   *result = NULL;

	tup = SearchSysCache1(COMPRESSIONMETHODOID, ObjectIdGetDatum(cmOid));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_compression	cmform = (Form_pg_compression) GETSTRUCT(tup);

		result = pstrdup(NameStr(cmform->cmname));
		ReleaseSysCache(tup);
	}
	return result;
}

/*
 * GetCompressionMethodRoutine - call the specified compression method handler
 * routine to get its CompressionMethodRoutine struct,
 * which will be palloc'd in the caller's context.
 */
CompressionMethodRoutine *
GetCompressionMethodRoutine(Oid cmhandler, Oid typeid)
{
	Datum						datum;
	CompressionMethodRoutine   *routine;
	CompressionMethodOpArgs		opargs;

	opargs.op = CMOP_GET_ROUTINE;
	opargs.args.getRoutine.typeid = typeid;

	datum = OidFunctionCall1(cmhandler, PointerGetDatum(&opargs));
	routine = (CompressionMethodRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, CompressionMethodRoutine))
		elog(ERROR, "compression method handler function %u "
					"did not return an CompressionMethodRoutine struct",
			 cmhandler);

	return routine;
}

/*
 * GetCompressionMethodRoutineByCmId -
 * look up the handler of the compression method with the given OID,
 * and get its CompressionMethodRoutine struct.
 */
CompressionMethodRoutine *
GetCompressionMethodRoutineByCmId(Oid cmoid, Oid typeid)
{
	HeapTuple			tuple;
	Form_pg_compression	cmform;
	regproc				cmhandler;

	/* Get handler function OID for the access method */
	tuple = SearchSysCache1(COMPRESSIONMETHODOID, ObjectIdGetDatum(cmoid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for compression method %u", cmoid);

	cmform = (Form_pg_compression) GETSTRUCT(tuple);

	cmhandler = cmform->cmhandler;

	/* Complain if handler OID is invalid */
	if (!RegProcedureIsValid(cmhandler))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("compression method \"%s\" does not have a handler",
						NameStr(cmform->cmname))));

	ReleaseSysCache(tuple);

	/* And finally, call the handler function to get the API struct. */
	return GetCompressionMethodRoutine(cmhandler, typeid);
}
