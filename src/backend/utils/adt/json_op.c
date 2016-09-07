/*
 * json_op.c
 *
 * Portions Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/json_op.c
 *
 */

#define jsonb_exists		json_exists
#define jsonb_exists_any	json_exists_any
#define jsonb_exists_all	json_exists_all
#define jsonb_contains		json_contains
#define jsonb_contained		json_contained
#define jsonb_ne			json_ne
#define jsonb_lt			json_lt
#define jsonb_gt			json_gt
#define jsonb_le			json_le
#define jsonb_ge			json_ge
#define jsonb_eq			json_eq
#define jsonb_cmp			json_cmp
#define jsonb_hash			json_hash

#define JsonxContainerOps			(&jsontContainerOps)
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

#include "jsonb_op.c"