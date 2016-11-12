/*-------------------------------------------------------------------------
 *
 * json_generic.h
 *	  Declarations for generic json data type support.
 *
 * Copyright (c) 2014-2016, PostgreSQL Global Development Group
 *
 * src/include/utils/json_generic.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef UTILS_JSON_GENERIC_H
#define UTILS_JSON_GENERIC_H

#define JSON_GENERIC

#include "postgres.h"
#include "access/compression.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/expandeddatum.h"
#include "utils/jsonb.h"

typedef JsonbPair JsonPair;
typedef JsonbValue JsonValue;
typedef JsonbValueType JsonValueType;
typedef JsonbIteratorToken JsonIteratorToken;

typedef struct JsonContainerOps JsonContainerOps;

typedef struct JsonContainerData
{
	JsonContainerOps   *ops;
	void			   *data;
	void			   *options;
	int					len;
	int					size;
	JsonValueType		type;
} JsonContainerData;

typedef const JsonContainerData JsonContainer;

typedef struct JsonIteratorData JsonIterator;

typedef JsonIteratorToken (*JsonIteratorNextFunc)(JsonIterator **iterator,
												  JsonValue *value,
												  bool skipNested);

struct JsonIteratorData
{
	JsonIterator		   *parent;
	JsonContainer		   *container;
	JsonIteratorNextFunc	next;
};

typedef struct JsonCompressionOptionsOps
{
	Size	(*encodeOptions)(JsonContainer *, void *buf);
	Size	(*decodeOptions)(const void *buf, CompressionOptions *);
	bool	(*optionsAreEqual)(JsonContainer *, CompressionOptions);
	void   *(*copyOptions)(void *options);
} JsonCompressionOptionsOps;

typedef enum JsonContainerTypes
{
	JsonContainerUnknown = 0,
	JsonContainerJsont = -1,
	JsonContainerJsonb = -2,
	JsonContainerJsonv = -3,
} JsonContainerTypes;

typedef Oid JsonContainerType;

struct JsonContainerOps
{
	JsonContainerType type;
	JsonCompressionOptionsOps *compressionOps;

	void			(*init)(JsonContainerData *jc, Datum value,
							CompressionOptions options);
	JsonIterator   *(*iteratorInit)(JsonContainer *jc);
	JsonValue	   *(*findKeyInObject)(JsonContainer *object,
									   const JsonValue *key);
	JsonValue	   *(*findValueInArray)(JsonContainer *array,
										const JsonValue *value);
	JsonValue	   *(*getArrayElement)(JsonContainer *array, uint32 index);
	uint32			(*getArraySize)(JsonContainer *array);
	char		   *(*toString)(StringInfo out, JsonContainer *jc,
								int estimated_len);
	JsonContainer  *(*copy)(JsonContainer *jc);
};

typedef struct CompressedObject
{
	ExpandedObjectHeader	eoh;
	Datum					value;
	CompressionOptions	   	options;
	bool					freeValue;
} CompressedObject;

typedef struct Json
{
	CompressedObject	obj;
	JsonContainerData	root;
	bool				is_json;	/* json or jsonb */
} Json;

#define JsonIsTemporary(json) \
		((json)->obj.eoh.vl_len_ != EOH_HEADER_MAGIC)

#ifndef JsonxContainerOps
# define JsonxContainerOps			(&jsonbContainerOps)
#endif

#define JsonFlattenToJsonbDatum(json) \
		PointerGetDatum(JsonFlatten(json, JsonbEncode, &jsonbContainerOps, NULL))

#define JsonGetEOHDatum(json)		EOHPGetRODatum(&JsonGetNonTemporary(json)->obj.eoh)

/* #define JSON_FLATTEN_INTO_TARGET */

#undef JsonbGetDatum
#ifdef JSON_FLATTEN_INTO_TARGET
# define JsonbGetDatum(json)		JsonFlattenToJsonbDatum(JsonGetUniquified(json))
#else
# define JsonbGetDatum(json)		JsonGetEOHDatum(JsonGetUniquified(json))
#endif

#ifdef JsonxGetDatum
# define JsonGetDatum(json)			JsonxGetDatum(json)
#elif defined(JsonxGetUniquified)
# define JsonGetDatum(json)			JsonGetEOHDatum(JsonxGetUniquified(json))
#else
# define JsonGetDatum(json)			JsonbGetDatum(json)
#endif

#undef DatumGetJsonb
#define DatumGetJsonb(datum) \
		DatumGetJson(datum, &jsonbContainerOps, NULL, NULL)
		
#define DatumGetJsont(datum) \
		DatumGetJson(datum, &jsontContainerOps, NULL, NULL)

#define DatumGetJsonx(datum) \
		DatumGetJson(datum, JsonxContainerOps, NULL, NULL)

#define DatumGetJsonxTmp(datum, tmp) \
		DatumGetJson(datum, JsonxContainerOps, NULL, tmp)

#undef PG_RETURN_JSONB
#define PG_RETURN_JSONB(x)			PG_RETURN_DATUM(JsonGetDatum(x))

#define PG_GETARG_JSONX_TMP(n, tmp)	DatumGetJsonxTmp(PG_GETARG_DATUM(n), tmp)

#undef	PG_GETARG_JSONB
#define PG_GETARG_JSONB(n)			PG_GETARG_JSONX_TMP(n, alloca(sizeof(Json))) /* FIXME conditional alloca() */

#define PG_FREE_IF_COPY_JSONB(json, n) \
	do { \
		if (!VARATT_IS_EXTERNAL_EXPANDED(PG_GETARG_POINTER(n))) \
			JsonFree(json); \
		else \
			Assert(DatumGetEOHP(PG_GETARG_DATUM(n)) == &(json)->obj.eoh); \
	} while (0)


#define JsonRoot(json)				(&(json)->root)
#define JsonGetSize(json)			(JsonRoot(json)->len)
#undef JsonbRoot
#undef JsonbGetSize
#define JsonbRoot(json)				JsonRoot(json)
#define JsonbGetSize(json)			JsonGetSize(json)

#define JsonContainerIsArray(c)		(((c)->type & ~jbvScalar) == jbvArray)
#define JsonContainerIsScalar(c)	((c)->type == (jbvArray | jbvScalar))
#define JsonContainerIsObject(c)	((c)->type == jbvObject)
#define JsonContainerSize(c)		((c)->size)
#define JsonContainerIsEmpty(c)		((c)->size == 0)

#define JsonContainerIsUniquified(jc) \
		((jc)->ops != &jsontContainerOps && \
		 ((jc)->ops != &jsonvContainerOps || \
		  JsonValueIsUniquified((JsonValue *) jc->data)))

#define JsonIsUniquified(json)		JsonContainerIsUniquified(JsonRoot(json))

#define JsonValueIsScalar(jsval)	IsAJsonbScalar(jsval)

#define JsonContainerGetType(jc) ((jc)->ops->type)
#define JsonContainerGetOpsByType(type) \
		((type) == JsonContainerJsont ? &jsontContainerOps : \
		 (type) == JsonContainerJsonb ? &jsonbContainerOps : NULL)

#ifdef JSONB_UTIL_C
#define JsonbValueToJsonb JsonValueToJsonb
#else
#define Jsonb Json
#define JsonbIterator JsonIterator
#define JsonbContainer JsonContainer
#define JsonbIteratorInit JsonIteratorInit
#define JsonbIteratorNext JsonIteratorNext
#define JsonbValueToJsonb JsonValueToJson

#undef JB_ROOT_COUNT
#undef JB_ROOT_IS_SCALAR
#undef JB_ROOT_IS_OBJECT
#undef JB_ROOT_IS_ARRAY
#define JB_ROOT_COUNT(json)		JsonContainerSize(JsonRoot(json))
#define JB_ROOT_IS_SCALAR(json)	JsonContainerIsScalar(JsonRoot(json))
#define JB_ROOT_IS_OBJECT(json)	JsonContainerIsObject(JsonRoot(json))
#define JB_ROOT_IS_ARRAY(json)	JsonContainerIsArray(JsonRoot(json))
#endif

#define JsonOp(op, jscontainer) \
		(*(jscontainer)->ops->op)

#define JsonOp0(op, jscontainer) \
		JsonOp(op, jscontainer)(jscontainer)

#define JsonOp1(op, jscontainer, arg) \
		JsonOp(op, jscontainer)(jscontainer, arg)

#define JsonIteratorInit(jscontainer) \
		JsonOp0(iteratorInit, jscontainer)

#define JsonFindValueInArray(jscontainer, key) \
		JsonOp1(findValueInArray, jscontainer, key)

#define JsonFindKeyInObject(jscontainer, key) \
		JsonOp1(findKeyInObject, jscontainer, key)

#define JsonGetArrayElement(jscontainer, index) \
		JsonOp1(getArrayElement, jscontainer, index)

#define JsonGetArraySize(json) \
		JsonOp0(getArraySize, json)

#define JsonCopy(jscontainer) \
		JsonOp0(copy, jscontainer)

static inline JsonIteratorToken
JsonIteratorNext(JsonIterator **it, JsonValue *val, bool skipNested)
{
	JsonIteratorToken tok;

	if (!*it)
		return WJB_DONE;

	do
		tok = (*it)->next(it, val, skipNested);
	while (tok == WJB_RECURSE);

	return tok;
}

#define getIthJsonbValueFromContainer	JsonGetArrayElement
#define findJsonbValueFromContainer		JsonFindValueInContainer
#define findJsonbValueFromContainerLen	JsonFindValueInContainerLen
#define compareJsonbContainers			JsonCompareContainers
#define equalsJsonbScalarValue			JsonValueScalarEquals

extern JsonContainerOps jsonbContainerOps;
extern JsonContainerOps jsontContainerOps;
extern JsonContainerOps jsonvContainerOps;

extern Json *DatumGetJson(Datum val, JsonContainerOps *ops,
						  CompressionOptions options, Json *tmp);

extern void JsonFree(Json *json);
extern Json *JsonCopyTemporary(Json *tmp);
extern Json *JsonUniquify(Json *json);

#define JsonContainerAlloc() \
	((JsonContainerData *) palloc(sizeof(JsonContainerData)))

extern JsonValue *JsonFindValueInContainer(JsonContainer *json, uint32 flags,
										   JsonValue *key);

static inline JsonValue *
JsonFindValueInContainerLen(JsonContainer *json, uint32 flags,
							const char *key, uint32 keylen)
{
	JsonValue	k;

	k.type = jbvString;
	k.val.string.val = key;
	k.val.string.len = keylen;

	return JsonFindValueInContainer(json, flags, &k);
}

static inline JsonIterator *
JsonIteratorFreeAndGetParent(JsonIterator *it)
{
	JsonIterator *parent = it->parent;
	pfree(it);
	return parent;
}

static inline void
JsonIteratorFree(JsonIterator *it)
{
	while (it)
		it = JsonIteratorFreeAndGetParent(it);
}

static inline Json *
JsonGetNonTemporary(Json *json)
{
	return JsonIsTemporary(json) ? JsonCopyTemporary(json) : json;
}

static inline Json *
JsonGetUniquified(Json *json)
{
	return JsonIsUniquified(json) ? json : JsonUniquify(json);
}

static inline JsonValue *
JsonValueInitObject(JsonValue *val, int nPairs, int nPairsAllocated,
					bool uniquified)
{
	val->type = jbvObject;
	val->val.object.nPairs = nPairs;
	val->val.object.pairs = nPairsAllocated ?
							palloc(sizeof(JsonPair) * nPairsAllocated) : NULL;
	val->val.object.uniquified = uniquified;
	val->val.object.valuesUniquified = uniquified;
	val->val.object.fieldSeparator = ' ';
	val->val.object.braceSeparator = 0;
	val->val.object.colonSeparator.before = 0;
	val->val.object.colonSeparator.after = ' ';

	return val;
}

static inline JsonValue *
JsonValueInitArray(JsonValue *val, int nElems, int nElemsAllocated,
				   bool rawScalar, bool uniquified)
{
	val->type = jbvArray;
	val->val.array.nElems = nElems;
	val->val.array.elems = nElemsAllocated ?
							palloc(sizeof(JsonValue) * nElemsAllocated) : NULL;
	val->val.array.rawScalar = rawScalar;
	if (!rawScalar)
	{
		val->val.array.uniquified = uniquified;
		val->val.array.elemsUniquified = uniquified;
		val->val.array.elementSeparator[0] = ' ';
		val->val.array.elementSeparator[1] = 0;
		val->val.array.elementSeparator[2] = 0;
	}

	return val;
}

static inline JsonValue *
JsonValueInitBinary(JsonValue *val, JsonContainer *cont)
{
	val->type = jbvBinary;
	val->val.binary.data = cont;
	val->val.binary.uniquified = JsonContainerIsUniquified(cont);

	return val;
}

static inline JsonbValue *
JsonValueInitString(JsonbValue *jbv, const char *str)
{
	jbv->type = jbvString;
	jbv->val.string.len = strlen(str);
	jbv->val.string.val = memcpy(palloc(jbv->val.string.len + 1), str,
								 jbv->val.string.len + 1);
	return jbv;
}

static inline JsonbValue *
JsonValueInitStringWithLen(JsonbValue *jbv, const char *str, int len)
{
	jbv->type = jbvString;
	jbv->val.string.val = str;
	jbv->val.string.len = len;
	return jbv;
}

static inline JsonbValue *
JsonValueInitText(JsonbValue *jbv, text *txt)
{
	jbv->type = jbvString;
	jbv->val.string.val = VARDATA_ANY(txt);
	jbv->val.string.len = VARSIZE_ANY_EXHDR(txt);
	return jbv;
}

static inline JsonbValue *
JsonValueInitNumeric(JsonbValue *jbv, Numeric num)
{
	jbv->type = jbvNumeric;
	jbv->val.numeric = num;
	return jbv;
}

static inline JsonbValue *
JsonValueInitInteger(JsonbValue *jbv, int64 i)
{
	jbv->type = jbvNumeric;
	jbv->val.numeric = DatumGetNumeric(DirectFunctionCall1(
											int8_numeric, Int64GetDatum(i)));
	return jbv;
}

static inline JsonbValue *
JsonValueInitFloat(JsonbValue *jbv, float4 f)
{
	jbv->type = jbvNumeric;
	jbv->val.numeric = DatumGetNumeric(DirectFunctionCall1(
											float4_numeric, Float4GetDatum(f)));
	return jbv;
}

extern Json *JsonValueToJson(JsonValue *val);
extern JsonValue *JsonToJsonValue(Json *json, JsonValue *jv);
extern JsonValue *JsonValueUnpackBinary(const JsonValue *jbv);
extern JsonContainer *JsonValueToContainer(const JsonValue *val);
extern JsonValue *JsonValueCopy(JsonValue *res, const JsonValue *val);
extern const JsonValue *JsonValueUnwrap(const JsonValue *val, JsonValue *buf);
extern JsonValue *JsonValueWrapInBinary(const JsonValue *val, JsonValue *bin);
extern JsonContainer *JsonCopyFlat(JsonContainer *flatContainer);
extern JsonValue *JsonExtractScalar(JsonContainer *jc, JsonValue *scalar);

extern int JsonCompareContainers(JsonContainer *a, JsonContainer *b);

extern bool JsonbDeepContains(JsonContainer *val, JsonContainer *mContained);

extern JsonValue *JsonContainerExtractKeys(JsonContainer *jsc);

/* jsonb.c support functions */
extern JsonValue *JsonValueFromCString(char *json, int len);


extern char *JsonbToCStringRaw(StringInfo out, JsonContainer *in,
			   int estimated_len);
extern char *JsonbToCStringIndent(StringInfo out, JsonContainer *in,
					 int estimated_len);
extern char *JsonbToCStringCanonical(StringInfo out, JsonContainer *in,
					 int estimated_len);

#define JsonToCString(jc)	JsonToCStringExt(NULL, jc, (jc)->len)

#define JsonToCStringExt(out, in, estimated_len) \
	((*(in)->ops->toString)(out, in, estimated_len))

#define JsonbToCString(out, in, estimated_len) \
		JsonToCStringExt(out, in, estimated_len)

extern JsonValue   *jsonFindKeyInObject(JsonContainer *obj, const JsonValue *key);
extern JsonValue   *jsonFindLastKeyInObject(JsonContainer *obj, const JsonValue *key);
extern JsonValue   *jsonFindValueInArray(JsonContainer *array, const JsonValue *elem);
extern uint32		jsonGetArraySize(JsonContainer *array);
extern JsonValue   *jsonGetArrayElement(JsonContainer *array, uint32 index);

extern bool JsonValueScalarEquals(const JsonValue *aScalar,
								  const JsonValue *bScalar);

typedef void (*JsonValueEncoder)(StringInfo, const JsonValue *,
								 CompressionOptions);

extern void *JsonContainerFlatten(JsonContainer *jc, JsonValueEncoder encoder,
								  JsonContainerOps *ops,
								  CompressionOptions options,
								  const JsonValue *binary);

extern void *JsonValueFlatten(const JsonValue *val, JsonValueEncoder encoder,
							  JsonContainerOps *ops, CompressionOptions opts);

static inline void *
JsonFlatten(Json *json, JsonValueEncoder encoder, JsonContainerOps *ops,
			CompressionOptions options)
{
	return JsonContainerFlatten(JsonRoot(json), encoder, ops, options, NULL);
}

extern void JsonbEncode(StringInfo, const JsonValue *, CompressionOptions);

#define JsonValueToJsonb(val) \
		JsonValueFlatten(val, JsonbEncode, &jsonbContainerOps, NULL)

extern int lengthCompareJsonbStringValue(const void *a, const void *b);

#endif /* UTILS_JSON_GENERIC_H */
