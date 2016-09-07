/*
 * json_generic.c
 *
 * Copyright (c) 2014-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/json_generic.c
 *
 */

#include "access/compression.h"
#include "utils/builtins.h"
#include "utils/json_generic.h"
#include "utils/memutils.h"

static Json *JsonExpand(Datum value, JsonContainerOps *ops,
						CompressionOptions opts);

static JsonContainerOps jsonvContainerOps;

#if 0
static JsonValue *
JsonValueCopy(JsonValue *val)
{
	JsonValue *copy = palloc(sizeof(JsonValue));
	memcpy(copy, val, sizeof(JsonValue));
	return copy;
}
#endif

static inline JsonValue *
jsonFindKeyInObjectInternal(JsonContainer *obj, const JsonValue *key, bool last)
{
	JsonValue		   *res = NULL;
	JsonValue			jbv;
	JsonIterator	   *it;
	JsonIteratorToken	tok;

	Assert(JsonContainerIsObject(obj));
	Assert(key->type == jbvString);

	it = JsonIteratorInit(obj);

	while ((tok = JsonIteratorNext(&it, &jbv, true)) != WJB_DONE)
	{
		if (tok == WJB_KEY && !lengthCompareJsonbStringValue(key, &jbv))
		{
			if (!last || !res)
				res = palloc(sizeof(JsonValue));

			tok = JsonIteratorNext(&it, res, true);
			Assert(tok == WJB_VALUE);

			if (last)
				continue;

			JsonIteratorFree(it);
			break;
		}
	}

	return res;
}

JsonValue *
jsonFindKeyInObject(JsonContainer *obj, const JsonValue *key)
{
	return jsonFindKeyInObjectInternal(obj, key, false);
}

JsonValue *
jsonFindLastKeyInObject(JsonContainer *obj, const JsonValue *key)
{
	return jsonFindKeyInObjectInternal(obj, key, true);
}

JsonValue *
jsonFindValueInArray(JsonContainer *array, const JsonValue *elem)
{
	JsonValue		   *val = palloc(sizeof(JsonValue));
	JsonIterator	   *it;
	JsonIteratorToken	tok;

	Assert(JsonContainerIsArray(array));
	Assert(IsAJsonbScalar(elem));

	it = JsonIteratorInit(array);

	while ((tok = JsonIteratorNext(&it, val, true)) != WJB_DONE)
	{
		if (tok == WJB_ELEM && val->type == elem->type &&
			equalsJsonbScalarValue(val, elem))
		{
			JsonIteratorFree(it);
			return val;
		}
	}

	pfree(val);
	return NULL;
}

JsonValue *
jsonGetArrayElement(JsonContainer *array, uint32 index)
{
	JsonValue		   *val = palloc(sizeof(JsonValue));
	JsonIterator	   *it;
	JsonIteratorToken	tok;

	Assert(JsonContainerIsArray(array));

	it = JsonIteratorInit(array);

	while ((tok = JsonIteratorNext(&it, val, true)) != WJB_DONE)
	{
		if (tok == WJB_ELEM)
		{
			if (index-- == 0)
			{
				JsonIteratorFree(it);
				return val;
			}
		}
	}

	pfree(val);

	return NULL;
}

uint32
jsonGetArraySize(JsonContainer *array)
{
	JsonValue		    val;
	JsonIterator	   *it;
	JsonIteratorToken	tok;
	uint32				size = 0;

	Assert(JsonContainerIsArray(array));

	it = JsonIteratorInit(array);

	while ((tok = JsonIteratorNext(&it, &val, true)) != WJB_DONE)
	{
		if (tok == WJB_ELEM)
			size++;
	}

	return size;
}

static void
jsonvInitContainer(JsonContainerData *jc, const JsonValue *val)
{
	jc->ops = &jsonvContainerOps;
	jc->data = (void *) val;
	jc->len = 0;
	jc->size = val->type == jbvBinary ? val->val.binary.data->size :
			   val->type == jbvObject ? val->val.object.nPairs :
			   val->type == jbvArray  ? val->val.array.nElems : 1;
	jc->type = val->type == jbvBinary ? val->val.binary.data->type :
			   val->type == jbvObject ? jbvObject :
			   val->type == jbvArray && !val->val.array.rawScalar ? jbvArray :
										jbvArray | jbvScalar;
}

JsonContainer *
JsonValueToContainer(const JsonValue *val)
{
	if (val->type == jbvBinary)
		return val->val.binary.data;
	else
	{
		JsonContainerData *jc = JsonContainerAlloc();
		jsonvInitContainer(jc, val);
		return jc;
	}
}

typedef struct JsonvScalarIterator
{
	JsonIterator		ji;
	JsonIteratorToken	next;
} JsonvScalarIterator;

typedef struct JsonvArrayIterator
{
	JsonIterator	ji;
	int				index;
} JsonvArrayIterator;

typedef struct JsonvObjectIterator
{
	JsonIterator	ji;
	int				index;
	bool			value;
} JsonvObjectIterator;

static JsonIterator *
jsonvIteratorInitFromValue(JsonValue *val, JsonContainer *jsc);

static JsonIteratorToken
jsonvScalarIteratorNext(JsonIterator **it, JsonValue *res, bool skipNested)
{
	JsonvScalarIterator	*sit = (JsonvScalarIterator *) *it;
	JsonValue			*val = (*it)->container->data;

	Assert(IsAJsonbScalar(val));

	switch (sit->next)
	{
		case WJB_BEGIN_ARRAY:
			res->type = jbvArray;
			res->val.array.rawScalar = true;
			res->val.array.nElems = 1;
			res->val.array.elems = NULL;
			sit->next = WJB_ELEM;
			return WJB_BEGIN_ARRAY;

		case WJB_ELEM:
			*res = *val;
			sit->next = WJB_END_ARRAY;
			return WJB_ELEM;

		case WJB_END_ARRAY:
			sit->next = WJB_DONE;
			*it = JsonIteratorFreeAndGetParent(*it);
			return WJB_END_ARRAY;

		default:
			return WJB_DONE;
	}
}

static JsonIteratorToken
jsonvArrayIteratorNext(JsonIterator **it, JsonValue *res, bool skipNested)
{
	JsonvArrayIterator	*ait = (JsonvArrayIterator *) *it;
	JsonValue			*arr = (*it)->container->data;
	JsonValue			*val;

	Assert(arr->type == jbvArray);

	if (ait->index == -1)
	{
		ait->index = 0;
		*res = *arr;
		return WJB_BEGIN_ARRAY;
	}

	if (ait->index >= arr->val.array.nElems)
	{
		*it = JsonIteratorFreeAndGetParent(*it);
		return WJB_END_ARRAY;
	}

	val = &arr->val.array.elems[ait->index++]; /* FIXME palloced copy */
	*res = *val;

	if (!IsAJsonbScalar(res))
	{
		if (!skipNested)
		{
			JsonIterator *child = jsonvIteratorInitFromValue(val, NULL);
			child->parent = *it;
			*it = child;
			return WJB_RECURSE;
		}
		else if (res->type != jbvBinary)
		{
			Assert(res->type == jbvArray || res->type == jbvObject);
			res->val.binary.data = JsonValueToContainer(val);
			res->val.binary.len = 0;
			res->type = jbvBinary;
		}
	}

	return WJB_ELEM;
}

static JsonIteratorToken
jsonvObjectIteratorNext(JsonIterator **it, JsonValue *res, bool skipNested)
{
	JsonvObjectIterator	*oit = (JsonvObjectIterator *) *it;
	JsonValue			*obj = (*it)->container->data;
	JsonPair			*pair;

	Assert(obj->type == jbvObject);

	if (oit->index == -1)
	{
		oit->index = 0;
		*res = *obj;
		return WJB_BEGIN_OBJECT;
	}

	if (oit->index >= obj->val.object.nPairs)
	{
		*it = JsonIteratorFreeAndGetParent(*it);
		return WJB_END_OBJECT;
	}

	pair = &obj->val.object.pairs[oit->index];

	if (oit->value)
	{
		*res = pair->value;
		oit->value = false;
		oit->index++;

		if (!IsAJsonbScalar(res))
		{
			if (!skipNested)
			{
				JsonIterator *chld =
						jsonvIteratorInitFromValue(&pair->value, NULL);
				chld->parent = *it;
				*it = chld;
				return WJB_RECURSE;
			}
			else if (res->type != jbvBinary)
			{
				Assert(res->type == jbvArray || res->type == jbvObject);
				res->val.binary.data = JsonValueToContainer(&pair->value);
				res->val.binary.len = 0;
				res->type = jbvBinary;
			}
		}

		return WJB_VALUE;
	}
	else
	{
		*res = pair->key;
		oit->value = true;
		return WJB_KEY;
	}
}

static JsonIterator *
JsonIteratorCreate(Size size, JsonContainer *jsc, JsonIteratorNextFunc next)
{
	JsonIterator *it = (JsonIterator *) palloc(size);

	it->container = jsc;
	it->parent = NULL;
	it->next = next;

	return it;
}

static JsonIterator *
JsonvArrayIteratorInit(JsonValue *val, JsonContainer *jsc)
{
	JsonvArrayIterator *it = (JsonvArrayIterator *)
			JsonIteratorCreate(sizeof(JsonvArrayIterator),
							   jsc ? jsc : JsonValueToContainer(val),
							   jsonvArrayIteratorNext);
	it->index = -1;

	return &it->ji;

}

static JsonIterator *
JsonvObjectIteratorInit(JsonValue *val, JsonContainer *jsc)
{
	JsonvObjectIterator *it = (JsonvObjectIterator *)
			JsonIteratorCreate(sizeof(JsonvObjectIterator),
							   jsc ? jsc : JsonValueToContainer(val),
							   jsonvObjectIteratorNext);
	it->index = -1;
	it->value = false;

	return &it->ji;
}

static JsonIterator *
JsonvScalarIteratorInit(JsonValue *val, JsonContainer *jsc)
{
	JsonvScalarIterator *it = (JsonvScalarIterator *)
			JsonIteratorCreate(sizeof(JsonvScalarIterator),
							   jsc ? jsc : JsonValueToContainer(val),
							   jsonvScalarIteratorNext);

	it->next = WJB_BEGIN_ARRAY;

	return &it->ji;
}

static JsonIterator *
jsonvIteratorInitFromValue(JsonValue *val, JsonContainer *jsc)
{
	if (val->type == jbvObject)
		return JsonvObjectIteratorInit(val, jsc);
	else if (val->type == jbvArray)
		return JsonvArrayIteratorInit(val, jsc);
	else if (val->type == jbvBinary)
		return JsonIteratorInit(val->val.binary.data);
	else if (IsAJsonbScalar(val))
		return JsonvScalarIteratorInit(val, jsc);
	else
	{
		elog(ERROR, "unexpected json value container type: %d", val->type);
		return NULL;
	}
}

static JsonIterator *
jsonvIteratorInit(JsonContainer *jsc)
{
	return jsonvIteratorInitFromValue(jsc->data, jsc);
}

static JsonValue *
jsonvFindKeyInObject(JsonContainer *objc, const JsonValue *key)
{
	JsonValue  *obj = (JsonValue *) objc->data;
	int			i;

	Assert(JsonContainerIsObject(objc));
	Assert(key->type == jbvString);

	if (obj->type == jbvBinary)
	{
		JsonContainer *jsc = obj->val.binary.data;
		Assert(jsc->type == jbvObject);
		return (*jsc->ops->findKeyInObject)(jsc, key);
	}

	Assert(obj->type == jbvObject);

	for (i = 0; i < obj->val.object.nPairs; i++)
	{
		JsonPair *pair = &obj->val.object.pairs[i];
		if (!lengthCompareJsonbStringValue(key, &pair->key))
		{
			if (pair->value.type == jbvObject ||
				pair->value.type == jbvArray)
			{	/* FIXME need to wrap containers into binary JsonValue */
				JsonContainer *jc = JsonValueToContainer(&pair->value);
				JsonValue  *jv = (JsonValue *) palloc(sizeof(JsonValue));
				jv->type = jbvBinary;
				jv->val.binary.data = jc;
				jv->val.binary.len = jc->len;
				return jv;
			}

			return &pair->value; /* FIXME palloced copy */
		}
	}

	return NULL;
}

static JsonValue *
jsonvFindValueInArray(JsonContainer *arrc, const JsonValue *val)
{
	JsonValue  *arr = (JsonValue *) arrc->data;

	Assert(JsonContainerIsArray(arrc));
	Assert(IsAJsonbScalar(val));

	if (arr->type == jbvBinary)
	{
		JsonContainer *jsc = arr->val.binary.data;
		Assert(JsonContainerIsArray(jsc));
		return (*jsc->ops->findValueInArray)(jsc, val);
	}
	else if (arr->type == jbvArray)
	{
		int	i;

		for (i = 0; i < arr->val.array.nElems; i++)
		{
			JsonValue *elem = &arr->val.array.elems[i];
			if (val->type == elem->type && equalsJsonbScalarValue(val, elem))
				return elem; /* FIXME palloced copy */
		}
	}
	else
	{
		Assert(IsAJsonbScalar(arr));
		if (arr->type == val->type && equalsJsonbScalarValue(val, arr))
			return arr;
	}

	return NULL;
}

static JsonValue *
jsonvGetArrayElement(JsonContainer *arrc, uint32 index)
{
	JsonValue  *arr = (JsonValue *) arrc->data;

	Assert(JsonContainerIsArray(arrc));

	if (arr->type == jbvBinary)
	{
		JsonContainer *jsc = arr->val.binary.data;
		Assert(jsc->type == jbvArray);
		return (*jsc->ops->getArrayElement)(jsc, index);
	}
	else if (arr->type == jbvArray)
	{
		if (index >= arr->val.array.nElems)
			return NULL;

		return &arr->val.array.elems[index]; /* FIXME palloced copy */
	}
	else
	{
		Assert(IsAJsonbScalar(arr));
		Assert(!index);
		return index ? NULL : arr;
	}
}

static uint32
jsonvGetArraySize(JsonContainer *arrc)
{
	JsonValue  *arr = (JsonValue *) arrc->data;

	Assert(JsonContainerIsArray(arrc));

	if (arr->type == jbvBinary)
	{
		JsonContainer *jsc = arr->val.binary.data;
		Assert(jsc->type == jbvArray);
		if (jsc->size < 0)
			((JsonContainerData *) jsc)->size = (*jsc->ops->getArraySize)(jsc);
		return jsc->size;
	}
	else if (arr->type == jbvArray)
		return arr->val.array.nElems;
	else
	{
		Assert(IsAJsonbScalar(arr));
		return 1;
	}
}

static JsonContainerOps
jsonvContainerOps =
{
	JsonContainerJsonv,
	NULL,
	NULL,
	jsonvIteratorInit,
	jsonvFindKeyInObject,
	jsonvFindValueInArray,
	jsonvGetArrayElement,
	jsonvGetArraySize,
	JsonbToCStringRaw,
};

JsonValue *
JsonToJsonValue(Json *json, JsonValue *jv)
{
	if (JsonRoot(json)->ops == &jsonvContainerOps)
		return (JsonValue *) JsonRoot(json)->data;

	if (!jv)
		jv = palloc(sizeof(JsonValue));

	jv->type = jbvBinary;
	jv->val.binary.data = &json->root;
	jv->val.binary.len = json->root.len;

	return jv;
}

#define JSON_FLATTEN_INTO_TARGET
/*
#define JSON_FLATTEN_INTO_JSONEXT
#define JSON_FLATTEN_INTO_JSONB
#define flatContainerOps &jsonbContainerOps
*/

#ifdef JSON_FLATTEN_INTO_JSONEXT
typedef struct varatt_extended_json
{
	varatt_extended_hdr vaext;
	JsonContainerType	type;
	char				data[FLEXIBLE_ARRAY_MEMBER];
} varatt_extended_json;

static Size
jsonGetExtendedSize(JsonContainer *jc)
{
	return VARHDRSZ_EXTERNAL + offsetof(varatt_extended_json, data) +
			VARHDRSZ + jc->len +
			(jc->ops->compressionOps ?
			 jc->ops->compressionOps->encodeOptions(jc, NULL) : 0);
}

static void
jsonWriteExtended(JsonContainer *jc, void *ptr, Size allocated_size)
{
	varatt_extended_json	extjs,
						   *pextjs;
	Size					optionsSize;

	Assert(allocated_size >= jsonGetExtendedSize(jc));

	extjs.vaext.size = jsonGetExtendedSize(jc) - VARHDRSZ_EXTERNAL;
	extjs.type = JsonContainerGetType(jc);
	Assert(extjs.type != JsonContainerUnknown);

	SET_VARTAG_EXTERNAL(ptr, VARTAG_EXTENDED);
	pextjs = (varatt_extended_json *) VARDATA_EXTERNAL(ptr);
	memcpy(pextjs, &extjs, offsetof(varatt_extended_json, data));

	optionsSize = jc->ops->compressionOps ?
				  jc->ops->compressionOps->encodeOptions(jc, &pextjs->data) : 0;

	SET_VARSIZE(&pextjs->data[optionsSize], VARHDRSZ + jc->len);
	memcpy(VARDATA(&pextjs->data[optionsSize]), jc->data, jc->len);
}

static Json *
JsonInitExtended(const struct varlena *toasted,
				 const struct varlena *detoasted)
{
	JsonContainerOps	   *ops;
	CompressionMethodRoutine *cmr;
	varatt_extended_json   *pextjs,
							extjs;
	Datum					value;
	CompressionOptions		options;
	Size					totalSize;
	Size					optionsSize;
	Size 				  (*decodeOptions)(const void *buf,
										   CompressionOptions *options);

	Assert(VARATT_IS_EXTERNAL_EXTENDED(detoasted));

	pextjs = (varatt_extended_json *) VARDATA_EXTERNAL(detoasted);
	memcpy(&extjs, pextjs, offsetof(varatt_extended_json, data));

	totalSize = extjs.vaext.size - offsetof(varatt_extended_json, data);

	ops = JsonContainerGetOpsByType(extjs.type);

	if (ops)
	{
		cmr = NULL;
		decodeOptions = ops->compressionOps ?
						ops->compressionOps->decodeOptions : NULL;
	}
	else
	{
		cmr = GetCompressionMethodRoutine(extjs.type, InvalidOid);

		if (!cmr)
			elog(ERROR, "unrecognized json container type %d", extjs.type);

		decodeOptions = cmr->options ? cmr->options->decode : NULL;
	}

	optionsSize = decodeOptions ? decodeOptions(&pextjs->data, &options) : 0;

	/* FIXME alignment
	 * value = PointerGetDatum(&pextjs->data[optionsSize]);
	 */
	value = PointerGetDatum(memcpy(palloc(totalSize - optionsSize),
								   &pextjs->data[optionsSize],
								   totalSize - optionsSize));

	if (ops)
		return JsonExpand(value, ops, options);

	value = cmr->decompress(value, options);

	Assert(VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value)));

	return (Json *) DatumGetEOHP(value);
}
#endif

static void
JsonInit(Json *json)
{
	const void *data = DatumGetPointer(json->obj.compressed);

	Assert(json->root.data || data);

	if (json->root.data || !data)
		return;

	data = PG_DETOAST_DATUM(json->obj.compressed);

	Assert(!VARATT_IS_EXTERNAL_EXTENDED(data));
	json->obj.compressed = PointerGetDatum(data);

	json->root.ops->init(&json->root, json->obj.compressed, json->obj.options);
}

static Size
jsonGetFlatSize2(Json *json)
{
	Size		size;

#ifdef JSON_FLATTEN_INTO_TARGET
	if (json->is_json)
#endif
#if defined(JSON_FLATTEN_INTO_TARGET) || defined(JSON_FLATTEN_INTO_JSON)
	{
		if (json->root.ops == &jsontContainerOps)
			size = VARHDRSZ + json->root.len;
		else
		{
			char	   *str = JsonToCString(&json->root);
			size = VARHDRSZ + strlen(str);
			pfree(str);
		}
	}
#endif
#ifdef JSON_FLATTEN_INTO_TARGET
	else
#endif
#if defined(JSON_FLATTEN_INTO_TARGET) || defined(JSON_FLATTEN_INTO_JSONB)
	{
		if (json->root.ops == &jsonbContainerOps)
			size = VARHDRSZ + json->root.len;
		else
		{
			JsonValue	val;
			void	   *js = JsonValueToJsonb(JsonToJsonValue(json, &val));
			size = VARSIZE(js);
			pfree(js);
		}
	}
#endif

	return size;
}

static void *
jsonFlatten(Json *json)
{
#ifdef JSON_FLATTEN_INTO_TARGET
	if (json->is_json)
#endif
#if defined(JSON_FLATTEN_INTO_TARGET) || defined(JSON_FLATTEN_INTO_JSON)
	{
		if (json->root.ops == &jsontContainerOps)
			return cstring_to_text_with_len(json->root.data, json->root.len);
		else
		{
			char   *str = JsonToCString(JsonRoot(json));
			text   *text = cstring_to_text(str);
			pfree(str);
			return text;
		}
	}
#endif
#ifdef JSON_FLATTEN_INTO_TARGET
	else
#endif
#if defined(JSON_FLATTEN_INTO_TARGET) || defined(JSON_FLATTEN_INTO_JSONB)
	{
		if (json->root.ops == &jsonbContainerOps)
		{
			void	   *res = palloc(VARHDRSZ + json->root.len);
			SET_VARSIZE(res, VARHDRSZ + json->root.len);
			memcpy(VARDATA(res), json->root.data, json->root.len);
			return res;
		}
		else
		{
			JsonValue	val;
			return JsonValueToJsonb(JsonToJsonValue(json, &val));
		}
	}
#endif
}

static Size
jsonGetFlatSize(ExpandedObjectHeader *eoh)
{
	Json   *json = (Json *) eoh;

	JsonInit(json);

#ifdef JSON_FLATTEN_INTO_JSONEXT
	{
		JsonContainer	   *flat = JsonRoot(json);
		JsonContainerData	tmp;

		if (json->root.ops == &jsonvContainerOps)
		{
			tmp.data = NULL;
			tmp.ops = flatContainerOps;
			tmp.len = jsonGetFlatSize2(json) - VARHDRSZ;
			flat = &tmp;
		}

		return jsonGetExtendedSize(flat);
	}
#else
	return jsonGetFlatSize2(json);
#endif
}

static void
jsonFlattenInto(ExpandedObjectHeader *eoh, void *result, Size allocated_size)
{
	Json   *json = (Json *) eoh;

	JsonInit(json);

#ifdef JSON_FLATTEN_INTO_JSONEXT
	{
		JsonContainer	   *flat = JsonRoot(json);
		JsonContainerData	tmp;
		void			   *tmpData = NULL;

		if (flat->ops == &jsonvContainerOps)
		{
			tmpData = jsonFlatten(json);

			tmp.ops = flatContainerOps;
			tmp.data = VARDATA(tmpData);
			tmp.len = VARSIZE_ANY_EXHDR(tmpData);

			flat = &tmp;
		}

		jsonWriteExtended(flat, result, allocated_size);

		if (tmpData)
			pfree(tmpData);
	}
#else
	{
		void *data = jsonFlatten(json);
		memcpy(result, data, allocated_size);
		pfree(data);
	}
#endif
}

static ExpandedObjectMethods
jsonExpandedObjectMethods =
{
	jsonGetFlatSize,
	jsonFlattenInto
};

static Json *
JsonExpand(Datum value, JsonContainerOps *ops, CompressionOptions options)
{
	MemoryContext	objcxt;
	Json		   *json;

	/*
	 * Allocate private context for expanded object.  We start by assuming
	 * that the json won't be very large; but if it does grow a lot, don't
	 * constrain aset.c's large-context behavior.
	 */
	objcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "expanded json",
								   ALLOCSET_SMALL_MINSIZE,
								   ALLOCSET_SMALL_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);

	json = (Json *) MemoryContextAlloc(objcxt, sizeof(Json));

	EOH_init_header(&json->obj.eoh, &jsonExpandedObjectMethods, objcxt);

	json->obj.compressed = value;
	json->obj.options = options;
	json->root.data = NULL;
	json->root.len = 0;
	json->root.ops = ops;
	json->root.size = -1;
	json->root.type = jbvBinary;
	json->is_json = false;

	return json;
}

static Json *
JsonExpandDatum(Datum value, JsonContainerOps *ops, CompressionOptions options)
{
	struct varlena *toasted = (struct varlena *) DatumGetPointer(value);
	Json	   *json;

	if (VARATT_IS_EXTERNAL_EXPANDED(toasted))
		json = (Json *) DatumGetEOHP(value);
	else
	{
		struct varlena *detoasted = pg_detoast_datum(toasted);

		if (VARATT_IS_EXTERNAL_EXTENDED(detoasted))
#ifdef JSON_FLATTEN_INTO_JSONEXT
			return JsonInitExtended(toasted, detoasted);
#else
			elog(ERROR, "unexpected extended json");
#endif

		json = JsonExpand(PointerGetDatum(detoasted), ops, options);
	}

	return json;
}

Json *
DatumGetJson(Datum value, JsonContainerOps *ops, CompressionOptions options)
{
	Json *json = JsonExpandDatum(value, ops, options);
	JsonInit(json);
	return json;
}

Json *
JsonValueToJson(JsonValue *val)
{
	if (val->type == jbvBinary)
	{
		JsonContainer  *jc = val->val.binary.data;
		Json		   *json = JsonExpand(PointerGetDatum(NULL), jc->ops, NULL);
		json->root = *jc;
		return json;
	}
	else
	{
		Json *json = JsonExpand(PointerGetDatum(NULL), &jsonvContainerOps, NULL);
		jsonvInitContainer(&json->root, val);
		return json;
	}
}

JsonValue *
JsonContainerExtractKeys(JsonContainer *jsc)
{
	JsonIterator	   *it;
	JsonbParseState	   *state = NULL;
	JsonValue		   *res = NULL;
	JsonValue			val;
	JsonIteratorToken	tok;

	Assert(JsonContainerIsObject(jsc));

	it = JsonIteratorInit(jsc);

	while ((tok = JsonIteratorNext(&it, &val, false)) != WJB_DONE)
	{
		res = pushJsonbValue(&state, tok, tok < WJB_BEGIN_ARRAY ? &val : NULL);

		if (tok == WJB_KEY)
		{
			tok = JsonIteratorNext(&it, &val, true);
			Assert(tok == WJB_VALUE);
			pushJsonbValueScalar(&state, tok, &val);
		}
	}

	return res;
}