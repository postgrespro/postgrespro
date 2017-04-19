/*-------------------------------------------------------------------------
 *
 * bson.c
 *
 * Copyright (c) 2016-2017, Postgres Professional
 *
 * IDENTIFICATION
 *	 contrib/bson/bson.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "access/compression.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/json_generic.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

PG_MODULE_MAGIC;

#define DatumGetBson(d)		((Bson *) PG_DETOAST_DATUM(d))
#define BsonGetDatum(p)		PointerGetDatum(p)
#define PG_GETARG_BSON(x)	DatumGetBson(PG_GETARG_DATUM(x))
#define PG_RETURN_BSON(x)	PG_RETURN_POINTER(BsonGetDatum(x))

typedef enum
{
	BSON_ELEM_END		= 0x00,
	BSON_ELEM_DOUBLE	= 0x01,
	BSON_ELEM_STRING	= 0x02,
	BSON_ELEM_DOCUMENT	= 0x03,
	BSON_ELEM_ARRAY		= 0x04,
	BSON_ELEM_BINARY	= 0x05,
	BSON_ELEM_UNDEFINED	= 0x06, /* deprecated */
	BSON_ELEM_OBJECTID	= 0x07,
	BSON_ELEM_BOOLEAN	= 0x08,
	BSON_ELEM_UTCDT		= 0x09,
	BSON_ELEM_NULL		= 0x0A,
	BSON_ELEM_REGEX		= 0x0B,
	BSON_ELEM_DBPOINTER	= 0x0C, /* deprecated */
	BSON_ELEM_JSCODE	= 0x0D,
	BSON_ELEM_DEPRECATED= 0x0E,
	BSON_ELEM_JSCODEWS	= 0x0F,
	BSON_ELEM_INT32		= 0x10,
	BSON_ELEM_TIMESTAMP	= 0x11,
	BSON_ELEM_INT64		= 0x12,

	BSON_ELEM_MINKEY	= 0xFF,
	BSON_ELEM_MAXKEY	= 0x7F,
} BsonElementType;

typedef int32 BsonSize;

typedef struct BsonContainer
{
	BsonSize	size;		/* unaligned */
	char		data[1];
	/* the data for each child element follows. */
} BsonContainer;

/* The top-level on-disk format for a bson datum. */
typedef struct
{
	int32			vl_len_;	/* varlena header (do not touch directly!) */
	BsonContainer	root;
} Bson;

JsonContainerOps bsonContainerOps;

extern Datum bson_handler(PG_FUNCTION_ARGS);

static BsonElementType bsonEncodeValue(StringInfo, const JsonbValue *, int lvl);

static void
bsonInitContainer(JsonContainerData *jc, BsonContainer *jbc, JsonValueType type)
{
	BsonSize len;

	memcpy(&len, &jbc->size, sizeof(len));

	jc->ops = &bsonContainerOps;
	jc->data = jbc;
	jc->options = NULL;
	jc->len = len;
	jc->size = len <= sizeof(len) + 1 ? 0 : -1;
	jc->type = type;
}

static void
bsonInit(JsonContainerData *jc, Datum value, CompressionOptions options)
{
	Bson	   *jb = DatumGetBson(value);

	bsonInitContainer(jc, &jb->root, jbvObject);
}

static BsonElementType
bsonEncodeBinary(StringInfo buffer, const JsonValue *val, int level)
{
	JsonContainer *jc = val->val.binary.data;

	if (jc->ops == &bsonContainerOps && !JsonContainerIsScalar(jc))
	{
		appendToBuffer(buffer, jc->data, jc->len);
		return JsonContainerIsObject(jc) ? BSON_ELEM_DOCUMENT : BSON_ELEM_ARRAY;
	}

	return bsonEncodeValue(buffer, JsonValueUnpackBinary(val), level);
}

#define BSON_MAXLEN 0x7FFFFFFF

static BsonElementType
bsonEncodeArray(StringInfo buf, const JsonValue *val, int level)
{
	JsonValue  *elem = val->val.array.elems;
	int			nelems = val->val.array.nElems;
	int			base_offset;
	int			i;
	BsonSize	size;

	Assert(nelems >= 0);

	/* Reserve space for array size. */
	base_offset = reserveFromBuffer(buf, sizeof(size));

	for (i = 0; i < nelems; i++, elem++)
	{
		BsonElementType	type;
		int				offset = buf->len;

		appendStringInfo(buf, "%c%ud", 0, (unsigned int) i);
		appendStringInfoCharMacro(buf, '\0');
		type = bsonEncodeValue(buf, elem, level + 1);
		buf->data[offset] = type;

		if (buf->len > BSON_MAXLEN)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("total size of bson object elements exceeds the maximum of %u bytes",
							BSON_MAXLEN)));
	}

	appendStringInfoCharMacro(buf, BSON_ELEM_END);

	size = buf->len - base_offset;
	memcpy(&buf->data[base_offset], &size, sizeof(size));

	return BSON_ELEM_ARRAY;
}

static BsonElementType
bsonEncodeObject(StringInfo buf, const JsonValue *val, int level)
{
	JsonPair   *pairs = val->val.object.pairs;
	int			npairs = val->val.object.nPairs;
	int			base_offset;
	int			i;
	BsonSize	size;

	Assert(npairs >= 0);

	/* Reserve space for object size. */
	base_offset = reserveFromBuffer(buf, sizeof(size));

	for (i = 0; i < npairs; i++)
	{
		JsonPair   *pair = &pairs[i];
		int			keylen = pair->key.val.string.len;
		int			offset = reserveFromBuffer(buf, keylen + 2);

		buf->data[offset] = bsonEncodeValue(buf, &pair->value, level + 1);
		memcpy(&buf->data[offset + 1], pair->key.val.string.val, keylen);
		buf->data[offset + keylen + 1] = '\0';

		if (buf->len > BSON_MAXLEN)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("total size of bson object elements exceeds the maximum of %u bytes",
							BSON_MAXLEN)));
	}

	appendStringInfoCharMacro(buf, BSON_ELEM_END);

	size = buf->len - base_offset;
	memcpy(&buf->data[base_offset], &size, sizeof(size));

	return BSON_ELEM_DOCUMENT;
}

static BsonElementType
bsonEncodeValue(StringInfo buf, const JsonbValue *val, int level)
{
	check_stack_depth();

	if (!val)
		return BSON_ELEM_END;

	Assert(JsonValueIsUniquified(val));

	switch (val->type)
	{
		case jbvNull:
			return BSON_ELEM_NULL;

		case jbvString:
		{
			BsonSize size = val->val.string.len + 1;
			appendToBuffer(buf, &size, sizeof(size));
			appendToBuffer(buf, val->val.string.val, size);
			return BSON_ELEM_STRING;
		}

		case jbvNumeric:
		{
			uint32 small;

			if (numeric_get_small(val->val.numeric, &small))
			{
				appendToBuffer(buf, &small, sizeof(small));
				return BSON_ELEM_INT32;
			}
			else
			{
				float8 f = DatumGetFloat8(DirectFunctionCall1(
							numeric_float8, NumericGetDatum(val->val.numeric)));
				appendToBuffer(buf, &f, sizeof(f));
				return BSON_ELEM_DOUBLE;
			}
		}

		case jbvBool:
			appendStringInfoCharMacro(buf, val->val.boolean ? 1: 0);
			return BSON_ELEM_BOOLEAN;

		case jbvArray:
			return bsonEncodeArray(buf, val, level);

		case jbvObject:
			return bsonEncodeObject(buf, val, level);

		case jbvBinary:
			return bsonEncodeBinary(buf, val, level);

		default:
			elog(ERROR, "unknown type of json value");
			return BSON_ELEM_END;
	}
}

static void
bsonEncode(StringInfoData *buffer, const JsonValue *val, CompressionOptions o)
{
	if (val->type != jbvObject)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("invalid json value type"),
				 errdetail("bson does not support scalars or arrays at the top level")));

	(void) bsonEncodeObject(buffer, val, 0);
}

static const char *
bsonFillValue(BsonElementType type, const char *base, JsonValue *result)
{
	switch (type)
	{
		case BSON_ELEM_NULL:
			result->type = jbvNull;
			return base;

		case BSON_ELEM_STRING:
		{
			BsonSize	len;
			memcpy(&len, base, sizeof(len));
			JsonValueInitStringWithLen(result, base + sizeof(len), len - 1);
			return base + sizeof(len) + len;
		}

		case BSON_ELEM_DOUBLE:
		{
			float8	f;
			memcpy(&f, base, sizeof(f));
			JsonValueInitDouble(result, f);
			return base + sizeof(f);
		}

		case BSON_ELEM_INT32:
		{
			int32	i;
			memcpy(&i, base, sizeof(i));
			JsonValueInitInteger(result, i);
			return base + sizeof(i);
		}

		case BSON_ELEM_INT64:
		{
			int64	i;
			memcpy(&i, base, sizeof(i));
			JsonValueInitInteger(result, i);
			return base + sizeof(i);
		}

		case BSON_ELEM_BOOLEAN:
			result->type = jbvBool;
			result->val.boolean = !!*base;
			return base + 1;

		case BSON_ELEM_DOCUMENT:
		case BSON_ELEM_ARRAY:
		{
			JsonContainerData *cont = JsonContainerAlloc();
			bsonInitContainer(cont, (BsonContainer *) base,
							  type == BSON_ELEM_DOCUMENT ? jbvObject : jbvArray);
			JsonValueInitBinary(result, cont);
			return base + cont->len;
		}

		case BSON_ELEM_BINARY:
		case BSON_ELEM_DBPOINTER:
		case BSON_ELEM_JSCODE:
		case BSON_ELEM_JSCODEWS:
		case BSON_ELEM_MINKEY:
		case BSON_ELEM_MAXKEY:
		case BSON_ELEM_REGEX:
		case BSON_ELEM_TIMESTAMP:
		case BSON_ELEM_UNDEFINED:
		case BSON_ELEM_UTCDT:
		default:
			elog(ERROR, "unsupported bson elemet type %d", type);
			return base;
	}
}

typedef struct BsonContainterIterator
{
	JsonIterator		ji;
	const char		   *ptr;
	const char		   *end;
	BsonElementType		elemtype;
	JsonbIterState		state;
} BsonContainterIterator;

static JsonIterator *
bsonIteratorFromContainer(JsonContainer *container,
						  BsonContainterIterator *parent);

static JsonIteratorToken
bsonIteratorNext(JsonIterator **pit, JsonValue *val, bool skipNested)
{
	BsonContainterIterator *it;

	if (!*pit)
		return WJB_DONE;

recurse:
	it = (BsonContainterIterator *) *pit;

	switch (it->state)
	{
		case JBI_ARRAY_START:
			JsonValueInitArray(val, it->ptr + 1 >= it->end ? 0 : -1, 0,
							   false, true);
			it->state = JBI_ARRAY_ELEM;
			return WJB_BEGIN_ARRAY;

		case JBI_ARRAY_ELEM:
		{
			BsonElementType type;

			if (it->ptr >= it->end || (type = *it->ptr++) == BSON_ELEM_END)
			{
				*pit = JsonIteratorFreeAndGetParent(*pit);
				return WJB_END_ARRAY;
			}

			while (it->ptr < it->end && *it->ptr++)
				continue; /* skip index key */

			if (it->ptr >= it->end)
			{
				*pit = JsonIteratorFreeAndGetParent(*pit);
				return WJB_END_ARRAY; /* FIXME elog(ERROR) */
			}

			it->ptr = bsonFillValue(type, it->ptr, val);

			if (!JsonValueIsScalar(val) && !skipNested)
			{
				/* Recurse into container. */
				*pit = bsonIteratorFromContainer(val->val.binary.data, it);
				goto recurse;
			}

			return WJB_ELEM;
		}

		case JBI_OBJECT_START:
			JsonValueInitObject(val, it->ptr + 1 >= it->end ? 0 : -1, 0, true);
			it->state = JBI_OBJECT_KEY;
			return WJB_BEGIN_OBJECT;

		case JBI_OBJECT_KEY:
		{
			const char	   *key;
			BsonSize		keylen = 0;

			if (it->ptr >= it->end ||
				(it->elemtype = *it->ptr++) == BSON_ELEM_END)
			{
				*pit = JsonIteratorFreeAndGetParent(*pit);
				return WJB_END_OBJECT;
			}

			key = it->ptr;

			while (it->ptr < it->end && *it->ptr++)
				keylen++;

			if (it->ptr >= it->end)
			{
				*pit = JsonIteratorFreeAndGetParent(*pit);
				return WJB_END_OBJECT; /* FIXME elog(ERROR) */
			}

			JsonValueInitStringWithLen(val, key, keylen);

			it->state = JBI_OBJECT_VALUE;
			return WJB_KEY;
		}

		case JBI_OBJECT_VALUE:
			it->state = JBI_OBJECT_KEY;
			it->ptr = bsonFillValue(it->elemtype, it->ptr, val);

			if (!JsonValueIsScalar(val) && !skipNested)
			{
				*pit = bsonIteratorFromContainer(val->val.binary.data, it);
				goto recurse;
			}

			return WJB_VALUE;
	}

	elog(ERROR, "invalid iterator state");
	return -1;
}

static JsonIterator *
bsonIteratorFromContainer(JsonContainer *jsc, BsonContainterIterator *parent)
{
	BsonContainer		   *container = (BsonContainer *) jsc->data;
	BsonContainterIterator *it;

	it = palloc(sizeof(BsonContainterIterator));

	it->ji.container = jsc;
	it->ji.parent = &parent->ji;
	it->ji.next = bsonIteratorNext;

	it->ptr = container->data;
	it->end = (const char *) container + jsc->len;
	it->state = jsc->type == jbvObject ? JBI_OBJECT_START : JBI_ARRAY_START;

	return &it->ji;
}

static JsonIterator *
bsonIteratorInit(JsonContainer *container)
{
	return bsonIteratorFromContainer(container, NULL);
}

JsonContainerOps
bsonContainerOps =
{
	JsonContainerUnknown,
	NULL,
	bsonInit,
	bsonIteratorInit,
	jsonFindKeyInObject,
	jsonFindValueInArray,
	jsonGetArrayElement,
	jsonGetArraySize,
	JsonbToCStringRaw,
	JsonCopyFlat,
};

#define JsonValueToBson(val) \
		JsonValueFlatten(val, bsonEncode, &bsonContainerOps, NULL)

#ifndef JSON_FULL_DECOMPRESSION
static inline Datum bsonCompress(Datum value, Json *json)
{
	JsonValue	jvbuf;
	JsonValue  *jv = JsonToJsonValue(json, &jvbuf);
	Bson	   *bson = JsonValueToBson(jv);
	JsonFreeIfCopy(json, value);
#else
static inline Datum bsonCompress(Datum value)
{
	text	   *json = DatumGetTextP(value);
	JsonValue  *jv = JsonValueFromCString(VARDATA(json),
										  VARSIZE(json) - VARHDRSZ);
	Bson	   *bson = JsonValueToBson(jv);
#endif
	return BsonGetDatum(bson);
}

static Datum
bsonCompressJsont(Datum value, CompressionOptions options)
{
#ifndef JSON_FULL_DECOMPRESSION
	return bsonCompress(value, DatumGetJsont(value));
#else
	return bsonCompress(value);
#endif
}

static Datum
bsonCompressJsonb(Datum value, CompressionOptions options)
{
#ifndef JSON_FULL_DECOMPRESSION
	return bsonCompress(value, DatumGetJsonb(value));
#else
	return bsonCompress(value);
#endif
}

static Datum
bsonDecompress(Datum value, CompressionOptions options)
{
#ifndef JSON_FULL_DECOMPRESSION
	Json *json = DatumGetJson(value, &bsonContainerOps, options, NULL);
	return JsonGetDatum(json);
#else
	JsonContainerData	jc;
	Datum				res;
	char			   *json;

	bsonInit(&jc, value, options);

	json = JsonToCString(&jc);
	res = PointerGetDatum(cstring_to_text(json));

	pfree(json);

	return res;
#endif
}

static void
bsonAddAttr(Form_pg_attribute attr, List *options)
{
	Oid	type = getBaseType(attr->atttypid);

	if (type != JSONOID && type != JSONBOID)
		elog(ERROR,
			 "bson compression method is only applicable to json/jsonb types");

	if (options != NIL)
		elog(ERROR, "bson compression method has no options");
}

PG_FUNCTION_INFO_V1(bson_handler);

Datum
bson_handler(PG_FUNCTION_ARGS)
{
	CompressionMethodOpArgs	   *opargs = (CompressionMethodOpArgs *)
														PG_GETARG_POINTER(0);
	CompressionMethodRoutine   *cmr = makeNode(CompressionMethodRoutine);
	Oid							typeid = opargs->args.getRoutine.typeid;

	if (OidIsValid(typeid) && typeid != JSONOID && typeid != JSONBOID)
		elog(ERROR, "unexpected type %d for bson compression method", typeid);

	bsonContainerOps.type = opargs->args.getRoutine.cmhanderid; /* FIXME */

	cmr->flags = CM_EXTENDED_REPRESENTATION;
	cmr->options = NULL;
	cmr->addAttr = bsonAddAttr;
	cmr->dropAttr = NULL;
	cmr->compress = !OidIsValid(typeid) ? NULL :
					typeid == JSONOID ? bsonCompressJsont : bsonCompressJsonb;
	cmr->decompress = bsonDecompress;

	PG_RETURN_POINTER(cmr);
}
