/*
 * json_gin.c
 *
 * Portions Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/json_gin.c
 *
 */

#define gin_compare_jsonb				gin_compare_json
#define gin_extract_jsonb				gin_extract_json
#define gin_extract_jsonb_query			gin_extract_json_query
#define gin_consistent_jsonb			gin_consistent_json
#define gin_triconsistent_jsonb			gin_triconsistent_json
#define gin_extract_jsonb_path			gin_extract_json_path
#define gin_extract_jsonb_query_path	gin_extract_json_query_path
#define gin_consistent_jsonb_path		gin_consistent_json_path
#define gin_triconsistent_jsonb_path	gin_triconsistent_json_path

#define JsonxContainerOps				(&jsontContainerOps)
#define JsonxGetUniquified(json)		(json)
#ifdef JSON_FLATTEN_INTO_TARGET
# define JsonxGetDatum(json)	\
		PointerGetDatum(cstring_to_text(JsonToCString(JsonRoot(json))))
#else
# define JsonxGetDatum(json)	JsontGetDatum(json)
#endif

#include "utils/json_generic.h"

#ifndef JSON_FLATTEN_INTO_TARGET
static inline Datum
JsontGetDatum(Json *json)
{
	json->is_json = true;
	return JsonGetEOHDatum(json);
}
#endif

#include "jsonb_gin.c"
