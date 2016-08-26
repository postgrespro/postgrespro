/*-------------------------------------------------------------------------
 *
 * jsonbc_util.c
 *	  jsonbc compression method for json/jsonb types
 *
 * Copyright (c) 2015-2017, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonbc_util.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "access/compression.h"
#include "access/hash.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/json_generic.h"
#include "utils/jsonbc_dict.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

/* flags for the header-field in JsonbcContainer */
#define JBC_CSHIFT				2
#define JBC_FSCALAR				0
#define JBC_FOBJECT				1
#define JBC_FARRAY				2
#define JBC_MASK				3

#define JENTRY_SHIFT			0x3
#define JENTRY_MAXOFFLEN		0x10000000

#undef JENTRY_TYPEMASK
#undef JENTRY_ISSTRING
#undef JENTRY_ISNUMERIC
#undef JENTRY_ISINTEGER
#undef JENTRY_ISBOOL_FALSE
#undef JENTRY_ISBOOL_TRUE
#undef JENTRY_ISNULL
#undef JENTRY_ISCONTAINER

#define JENTRY_TYPEMASK			0x7

/* values stored in the type bits */
#define JENTRY_ISSTRING			0x1
#define JENTRY_ISNUMERIC		0x2
#define JENTRY_ISINTEGER		0x3
#define JENTRY_ISBOOL_FALSE		0x4
#define JENTRY_ISBOOL_TRUE		0x5
#define JENTRY_ISNULL			0x6
#define JENTRY_ISCONTAINER		0x7 /* array or object */

#define JENTRY(type, len)		((JEntry) ((JENTRY_IS##type) | \
										   ((len) << JENTRY_SHIFT)))

/* Access macros.  Note possible multiple evaluations */
#define JBE_LENGTH(je_)			((je_) >> JENTRY_SHIFT)
#define JBE_TYPE(je_)			((je_) & JENTRY_TYPEMASK)
#define JBE_ISSTRING(je_)		(JBE_TYPE(je_) == JENTRY_ISSTRING)
#define JBE_ISNUMERIC(je_)		(JBE_TYPE(je_) == JENTRY_ISNUMERIC)
#define JBE_ISCONTAINER(je_)	(JBE_TYPE(je_) == JENTRY_ISCONTAINER)
#define JBE_ISNULL(je_)			(JBE_TYPE(je_) == JENTRY_ISNULL)
#define JBE_ISBOOL_TRUE(je_)	(JBE_TYPE(je_) == JENTRY_ISBOOL_TRUE)
#define JBE_ISBOOL_FALSE(je_)	(JBE_TYPE(je_) == JENTRY_ISBOOL_FALSE)
#define JBE_ISBOOL(je_)			(JBE_ISBOOL_TRUE(je_) || JBE_ISBOOL_FALSE(je_))
#define JBE_ISINTEGER(je_)		(JBE_TYPE(je_) == JENTRY_ISINTEGER)

/*
 * We store an offset, not a length, every JB_OFFSET_STRIDE children.
 * Caution: this macro should only be referenced when creating a JSONB
 * value.  When examining an existing value, pay attention to the HAS_OFF
 * bits instead.  This allows changes in the offset-placement heuristic
 * without breaking on-disk compatibility.
 */
#define	JBC_OFFSETS_CHUNK_SIZE	32

#define DatumGetJsonbc(d)	((Jsonbc *) PG_DETOAST_DATUM(d))
#define JsonbcGetDatum(p)	PointerGetDatum(p)
#define PG_GETARG_JSONBC_RAW(x)	DatumGetJsonbc(PG_GETARG_DATUM(x))
#define PG_RETURN_JSONBC_RAW(x)	PG_RETURN_POINTER(JsonbcGetDatum(x))

#define JsonValueToJsonbc(val, options) \
		JsonValueFlatten(val, JsonbcEncode, &jsonbcContainerOps, options)

#define GetJsonbcDictIdFromCompressionOptions(options) \
		((JsonbcDictId)(intptr_t)(options))

#define GetCompressionOptionsFromJsonbcDictId(dict) \
		((CompressionOptions)(intptr_t)(dict))

#define GetContainerOptionsFromJsonbcDictId(dict) \
		((void *)(intptr_t)(dict))


/*
 * Key/value pair within an Object.
 *
 * This struct type is only used briefly while constructing a Jsonbc; it is
 * *not* the on-disk representation.
 *
 * Pairs with duplicate keys are de-duplicated.  We store the originally
 * observed pair ordering for the purpose of removing duplicates in a
 * well-defined way (which is "last observed wins").
 */
typedef struct JsonbcPair
{
	int32		key;
	JsonbValue	value;			/* May be of any type */
	uint32		order;			/* Pair's index in original sequence */
} JsonbcPair;

static JEntry jsonbcEncodeValue(StringInfo buffer, const JsonbValue *val,
								int level, JsonbcDictId dict);

static JsonbcKeyId
jsonbcConvertKeyNameToId(JsonbcDictId dict, const JsonValue *string,
						 bool insert)
{
	JsonbcKeyName	keyName;
	JsonbcKeyId		keyId;

	keyName.s = string->val.string.val;
	keyName.len = string->val.string.len;

	keyId = jsonbcDictGetIdByName(dict, keyName, insert);

	if (insert && keyId == JsonbcInvalidKeyId)
		elog(ERROR, "could not insert key \"%.*s\" into jsonbc dictionary",
			 keyName.len, keyName.s);

	return keyId;
}

#define MAX_VARBYTE_SIZE 5

/*
 * Varbyte-encode 'val' into *ptr. *ptr is incremented to next integer.
 */
static void
varbyte_encode(uint32 val, unsigned char **ptr)
{
	unsigned char *p = *ptr;

	while (val > 0x7F)
	{
		*(p++) = 0x80 | (val & 0x7F);
		val >>= 7;
	}
	*(p++) = (unsigned char) val;

	*ptr = p;
}

/*
 * Decode varbyte-encoded integer at *ptr. *ptr is incremented to next integer.
 */
static uint32
varbyte_decode(const unsigned char **ptr)
{
	const unsigned char *p = *ptr;
	uint64		val;
	uint64		c;

	c = *(p++);
	val = c & 0x7F;
	if (c & 0x80)
	{
		c = *(p++);
		val |= (c & 0x7F) << 7;
		if (c & 0x80)
		{
			c = *(p++);
			val |= (c & 0x7F) << 14;
			if (c & 0x80)
			{
				c = *(p++);
				val |= (c & 0x7F) << 21;
				if (c & 0x80)
				{
					c = *(p++);
					val |= c << 28; /* last byte, no continuation bit */
				}
			}
		}
	}

	*ptr = p;

	return val;
}

static int
varbyte_size(uint32 value)
{
	if (value < 0x80)
		return 1;
	else if (value < 0x4000)
		return 2;
	else if (value < 0x200000)
		return 3;
	else if (value < 0x10000000)
		return 4;
	else
		return 5;
}

/*
 * A jsonbc array or object node, within a Jsonbc Datum.
 *
 * An array has one child for each element, stored in array order.
 *
 * An object has two children for each key/value pair.  The keys all appear
 * first, in key sort order; then the values appear, in an order matching the
 * key order.  This arrangement keeps the keys compact in memory, making a
 * search for a particular key more cache-friendly.
 */
typedef struct JsonbcContainer
{
	unsigned char	data[1];

	/* the data for each child node follows. */
} JsonbcContainer;

/* The top-level on-disk format for a jsonbc datum. */
typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	JsonbcContainer root;
} Jsonbc;

JsonContainerOps jsonbcContainerOps;

static inline uint32
jsonbcGetHeader(const JsonbcContainer *jbc, const unsigned char **pptr)
{
	const unsigned char *ptr = (const unsigned char *) jbc->data;
	uint32 header = varbyte_decode(&ptr);

	if (pptr)
		*pptr = ptr;

	return header;
}

static inline JsonbcDictId
jsonbcGetDictId(JsonContainer *container)
{
	return GetJsonbcDictIdFromCompressionOptions(container->options);
}

static void
jsonbcInitContainer(JsonContainerData *jc, JsonbcContainer *jbc, int len,
					JsonbcDictId dictId)
{
	uint32 header = jsonbcGetHeader(jbc, NULL);
	uint32 type = header & JBC_MASK;
	uint32 size = header >> JBC_CSHIFT;

	jc->ops = &jsonbcContainerOps;
	jc->data = jbc;
	jc->options = GetContainerOptionsFromJsonbcDictId(dictId);
	jc->len = len;
	jc->size = type == JBC_FSCALAR ? 1 : size ? -1 : 0;
	jc->type = type == JBC_FOBJECT ? jbvObject :
			   type == JBC_FSCALAR ? jbvArray | jbvScalar :
									 jbvArray;
}

static void
jsonbcInit(JsonContainerData *jc, Datum value, CompressionOptions options)
{
	Jsonbc		   *jb = DatumGetJsonbc(value);
	JsonbcDictId	dictId = GetJsonbcDictIdFromCompressionOptions(options);

	jsonbcInitContainer(jc, &jb->root, VARSIZE_ANY_EXHDR(jb), dictId);
}

static JEntry
jsonbcEncodeBinary(StringInfo buffer, const JsonValue *val, int level,
				   JsonbcDictId dict)
{
	JsonContainer *jc = val->val.binary.data;

	if (jc->ops == &jsonbcContainerOps && jsonbcGetDictId(jc) == dict &&
		!JsonContainerIsScalar(jc))
	{
		appendToBuffer(buffer, jc->data, jc->len);
		return JENTRY(CONTAINER, jc->len);
	}

	return jsonbcEncodeValue(buffer, JsonValueUnpackBinary(val), level, dict);
}

static JEntry
jsonbcEncodeArray(StringInfo buffer, const JsonValue *val, int level,
				  JsonbcDictId dict)
{
	JEntry			header;
	int				base_offset;
	int				totallen,
					offsets_len;
	unsigned char  *offsets,
				   *ptr,
				   *chunk_end;
	int				i,
					nElems = val->val.array.nElems;

	Assert(nElems >= 0);

	offsets_len = MAX_VARBYTE_SIZE * nElems;
	offsets_len += offsets_len / (JBC_OFFSETS_CHUNK_SIZE - MAX_VARBYTE_SIZE + 1)
							   * (2 * MAX_VARBYTE_SIZE);

	offsets = (unsigned char *) palloc(offsets_len);

	/* Remember where in the buffer this array starts. */
	base_offset = buffer->len;

	/* Reserve space for the JEntries of the elements. */
	ptr = offsets;
	chunk_end = offsets + JBC_OFFSETS_CHUNK_SIZE;

	totallen = 0;
	for (i = 0; i < nElems; i++)
	{
		JsonValue  *elem = &val->val.array.elems[i];
		int			len;
		JEntry		meta;

		/*
		 * Convert element, producing a JEntry and appending its
		 * variable-length data to buffer
		 */
		meta = jsonbcEncodeValue(buffer, elem, level + 1, dict);

		if (ptr + varbyte_size(meta) > chunk_end)
		{
			memset(ptr, 0, chunk_end - ptr);
			ptr = chunk_end;
			varbyte_encode(i, &ptr);
			varbyte_encode(totallen, &ptr);
			chunk_end += JBC_OFFSETS_CHUNK_SIZE;
		}

		len = JBE_LENGTH(meta);
		totallen += len;

		/*
		 * Bail out if total variable-length data exceeds what will fit in a
		 * JEntry length field.  We check this in each iteration, not just
		 * once at the end, to forestall possible integer overflow.
		 */
		if (totallen > JENTRY_MAXOFFLEN)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("total size of jsonb array elements exceeds the maximum of %u bytes",
							JENTRY_MAXOFFLEN)));

		varbyte_encode(meta, &ptr);
	}

	offsets_len = ptr - offsets;
	header = (offsets_len << JBC_CSHIFT);

	if (val->val.array.rawScalar)
	{
		Assert(nElems == 1);
		Assert(level == 0);
		header |= JBC_FSCALAR;
	}
	else
	{
		header |= JBC_FARRAY;
	}

	offsets_len += varbyte_size(header);

	reserveFromBuffer(buffer, offsets_len);
	memmove(buffer->data + base_offset + offsets_len,
			buffer->data + base_offset,
			buffer->len - base_offset - offsets_len);

	ptr = (unsigned char *) buffer->data + base_offset;
	varbyte_encode(header, &ptr);
	memcpy(ptr, offsets, offsets_len - varbyte_size(header));

	/* Total data size is everything we've appended to buffer */
	totallen = buffer->len - base_offset;

	/* Check length again, since we didn't include the metadata above */
	if (totallen > JENTRY_MAXOFFLEN)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("total size of jsonb array elements exceeds the maximum of %u bytes",
						JENTRY_MAXOFFLEN)));

	/* Initialize the header of this node in the container's JEntry array */
	return JENTRY(CONTAINER, totallen);
}

static int
compareJsonbcPair(const void *a, const void *b, void *binequal)
{
	const JsonbcPair *pa = (const JsonbcPair *) a;
	const JsonbcPair *pb = (const JsonbcPair *) b;

	if (pa->key != pb->key)
		return (pa->key < pb->key) ? -1 : 1;

	if (binequal)
		*((bool *) binequal) = true;

	return (pa->order > pb->order) ? -1 : 1;
}

static JEntry
jsonbcEncodeObject(StringInfo buffer, const JsonValue *val, int level,
				   JsonbcDictId dict)
{
	int			base_offset;
	int			i;
	int			totallen, offsets_len;
	unsigned char *offsets, *ptr, *chunk_end;
	JEntry		header;
	int			nPairs = val->val.object.nPairs;
	uint32		prev_key;
	JsonbPair  *jbpairs = val->val.object.pairs;
	JsonbcPair *jbcpairs = NULL;

	Assert(nPairs >= 0);

	/* Remember where in the buffer this object starts. */
	base_offset = buffer->len;

	offsets_len = MAX_VARBYTE_SIZE * nPairs * 2;
	offsets_len += offsets_len / (JBC_OFFSETS_CHUNK_SIZE - 2 * MAX_VARBYTE_SIZE + 1) * (2 * MAX_VARBYTE_SIZE);

	offsets = (unsigned char *) palloc(offsets_len);
	ptr = offsets;
	chunk_end = offsets + JBC_OFFSETS_CHUNK_SIZE;

	if (nPairs > 0)
	{
		jbcpairs = (JsonbcPair *) palloc(sizeof(JsonbcPair) * nPairs);

		for (i = 0; i < nPairs; i++)
		{
			jbcpairs[i].key = jsonbcConvertKeyNameToId(dict, &jbpairs[i].key,
													   true);
			jbcpairs[i].value = jbpairs[i].value;
			jbcpairs[i].order = jbpairs[i].order;
		}

		qsort_arg(jbcpairs, nPairs, sizeof(JsonbcPair),
				  compareJsonbcPair, NULL);
	}

	/*
	 * Iterate over the keys, then over the values, since that is the ordering
	 * we want in the on-disk representation.
	 */
	totallen = 0;
	prev_key = 0;
	for (i = 0; i < nPairs; i++)
	{
		JsonbcPair *pair = &jbcpairs[i];
		int			len;
		JEntry		meta;
		int32		key;

		/*
		 * Convert value, producing a JEntry and appending its variable-length
		 * data to buffer
		 */
		meta = jsonbcEncodeValue(buffer, &pair->value, level + 1, dict);

		key = pair->key;
		/* key = convertKeyNameToId(&pair->key); */

		Assert(key > prev_key);

		if (ptr + varbyte_size(key - prev_key) + varbyte_size(meta) > chunk_end)
		{
			memset(ptr, 0, chunk_end - ptr);
			ptr = chunk_end;
			varbyte_encode(prev_key, &ptr);
			varbyte_encode(totallen, &ptr);
			chunk_end += JBC_OFFSETS_CHUNK_SIZE;
		}

		len = JBE_LENGTH(meta);
		totallen += len;

		/*
		 * Bail out if total variable-length data exceeds what will fit in a
		 * JEntry length field.  We check this in each iteration, not just
		 * once at the end, to forestall possible integer overflow.
		 */
		if (totallen > JENTRY_MAXOFFLEN)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("total size of jsonb object elements exceeds the maximum of %u bytes",
							JENTRY_MAXOFFLEN)));

		varbyte_encode(key - prev_key, &ptr);
		varbyte_encode(meta, &ptr);

		prev_key = key;
	}

	if (jbcpairs)
		pfree(jbcpairs);

	offsets_len = ptr - offsets;
	header = (offsets_len << JBC_CSHIFT) | JBC_FOBJECT;
	offsets_len += varbyte_size(header);

	reserveFromBuffer(buffer, offsets_len);
	memmove(buffer->data + base_offset + offsets_len, buffer->data + base_offset,
			buffer->len - base_offset - offsets_len);

	ptr = (unsigned char *)buffer->data + base_offset;
	varbyte_encode(header, &ptr);
	memcpy(ptr, offsets, offsets_len - varbyte_size(header));

	/* Total data size is everything we've appended to buffer */
	totallen = buffer->len - base_offset;

	/* Check length again, since we didn't include the metadata above */
	if (totallen > JENTRY_MAXOFFLEN)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("total size of jsonb object elements exceeds the maximum of %u bytes",
						JENTRY_MAXOFFLEN)));

	/* Initialize the header of this node in the container's JEntry array */
	return JENTRY(CONTAINER, totallen);
}

static JEntry
jsonbcEncodeScalar(StringInfo buffer, const JsonbValue *scalarVal)
{
	int			numlen;
	short		padlen = 0;
	uint32		small;

	switch (scalarVal->type)
	{
		case jbvNull:
			return JENTRY_ISNULL;

		case jbvString:
			appendToBuffer(buffer, scalarVal->val.string.val,
						   scalarVal->val.string.len);

			return JENTRY(STRING, scalarVal->val.string.len);

		case jbvNumeric:
			if (numeric_get_small(scalarVal->val.numeric, &small))
			{
				int size = varbyte_size(small);
				unsigned char *ptr;

				reserveFromBuffer(buffer, size);
				ptr = (unsigned char *)buffer->data + buffer->len - size;
				varbyte_encode(small, &ptr);

				return JENTRY(INTEGER, size);
			}
			else
			{
				numlen = VARSIZE_ANY(scalarVal->val.numeric);
				/*padlen = padBufferToInt(buffer);*/

				appendToBuffer(buffer, (char *) scalarVal->val.numeric, numlen);

				return JENTRY(NUMERIC, padlen + numlen);
			}

		case jbvBool:
			return (scalarVal->val.boolean) ?
				JENTRY_ISBOOL_TRUE : JENTRY_ISBOOL_FALSE;

		default:
			elog(ERROR, "invalid jsonb scalar type");
			return 0;
	}
}

/*
 * Subroutine of convertJsonbc: serialize a single JsonbcValue into buffer.
 *
 * The JEntry header for this node is returned in *header.  It is filled in
 * with the length of this value and appropriate type bits.  If we wish to
 * store an end offset rather than a length, it is the caller's responsibility
 * to adjust for that.
 *
 * If the value is an array or an object, this recurses. 'level' is only used
 * for debugging purposes.
 */
static JEntry
jsonbcEncodeValue(StringInfo buffer, const JsonbValue *val, int level,
				  JsonbcDictId dict)
{
	check_stack_depth();

	if (!val)
		return 0;

	Assert(JsonValueIsUniquified(val));

	if (IsAJsonbScalar(val))
		return jsonbcEncodeScalar(buffer, val);
	else if (val->type == jbvArray)
		return jsonbcEncodeArray(buffer, val, level, dict);
	else if (val->type == jbvObject)
		return jsonbcEncodeObject(buffer, val, level, dict);
	else if (val->type == jbvBinary)
		return jsonbcEncodeBinary(buffer, val, level, dict);
	else
	{
		elog(ERROR, "unknown type of jsonb container");
		return 0;
	}
}

/*
 * A helper function to fill in a JsonbcValue to represent an element of an
 * array, or a key or value of an object.
 *
 * The node's JEntry is at container->children[index], and its variable-length
 * data is at base_addr + offset.  We make the caller determine the offset
 * since in many cases the caller can amortize that work across multiple
 * children.  When it can't, it can just call getJsonbcOffset().
 *
 * A nested array or object will be returned as jbvBinary, ie. it won't be
 * expanded.
 */
static void
jsonbcFillValue(JEntry entry, const char *base, uint32 offset,
				JsonValue *result, JsonbcDictId dictId)
{
	if (JBE_ISNULL(entry))
	{
		result->type = jbvNull;
	}
	else if (JBE_ISSTRING(entry))
	{
		result->type = jbvString;
		result->val.string.val = base + offset;
		result->val.string.len = JBE_LENGTH(entry);
		Assert(result->val.string.len >= 0);
	}
	else if (JBE_ISNUMERIC(entry))
	{
		result->type = jbvNumeric;
		result->val.numeric = (Numeric) (base + offset/* + INTALIGN(offset)*/);
	}
	else if (JBE_ISINTEGER(entry))
	{
		const unsigned char *ptr = (const unsigned char *) base + offset;
		result->type = jbvNumeric;
		result->val.numeric = small_to_numeric(varbyte_decode(&ptr));
	}
	else if (JBE_ISBOOL_TRUE(entry))
	{
		result->type = jbvBool;
		result->val.boolean = true;
	}
	else if (JBE_ISBOOL_FALSE(entry))
	{
		result->type = jbvBool;
		result->val.boolean = false;
	}
	else
	{
		JsonContainerData *cont = JsonContainerAlloc();
		Assert(JBE_ISCONTAINER(entry));
		jsonbcInitContainer(cont,
							(JsonbcContainer *)(base + offset),
							JBE_LENGTH(entry),
							dictId);
		JsonValueInitBinary(result, cont);
	}
}

typedef struct JsonbcContainterIterator
{
	JsonIterator		ji;

	JsonbcDictId		dict;
	uint32				childrenSize;
	uint32				curKey;
	bool				isScalar;		/* Pseudo-array scalar value? */
	const unsigned char *children;		/* JEntrys for child nodes */
	const unsigned char *childrenPtr;
	const unsigned char *chunkEnd;

	/* Data proper.  This points to the beginning of the variable-length data */
	const char		   *dataProper;

	/* Data offset corresponding to current item */
	uint32				curDataOffset;

	/*
	 * If the container is an object, we want to return keys and values
	 * alternately; so curDataOffset points to the current key, and
	 * curValueOffset points to the current value.
	 */
	uint32				curValueOffset;

	/* Private state */
	JsonbIterState		state;
} JsonbcContainterIterator;

static JsonIterator *
jsonbcIteratorFromContainer(JsonContainer *container,
							JsonbcContainterIterator *parent);

/*
 * Get next JsonbcValue while iterating
 *
 * Caller should initially pass their own, original iterator.  They may get
 * back a child iterator palloc()'d here instead.  The function can be relied
 * on to free those child iterators, lest the memory allocated for highly
 * nested objects become unreasonable, but only if callers don't end iteration
 * early (by breaking upon having found something in a search, for example).
 *
 * Callers in such a scenario, that are particularly sensitive to leaking
 * memory in a long-lived context may walk the ancestral tree from the final
 * iterator we left them with to its oldest ancestor, pfree()ing as they go.
 * They do not have to free any other memory previously allocated for iterators
 * but not accessible as direct ancestors of the iterator they're last passed
 * back.
 *
 * Returns "Jsonbc sequential processing" token value.  Iterator "state"
 * reflects the current stage of the process in a less granular fashion, and is
 * mostly used here to track things internally with respect to particular
 * iterators.
 *
 * Clients of this function should not have to handle any jbvBinary values
 * (since recursive calls will deal with this), provided skipNested is false.
 * It is our job to expand the jbvBinary representation without bothering them
 * with it.  However, clients should not take it upon themselves to touch array
 * or Object element/pair buffers, since their element/pair pointers are
 * garbage.  Also, *val will not be set when returning WJB_END_ARRAY or
 * WJB_END_OBJECT, on the assumption that it's only useful to access values
 * when recursing in.
 */
static JsonIteratorToken
JsonbcIteratorNext(JsonIterator **pit, JsonValue *val, bool skipNested)
{
	JsonbcContainterIterator *it;
	JEntry			entry;
	uint32			keyIncr;

	if (*pit == NULL)
		return WJB_DONE;

	/*
	 * When stepping into a nested container, we jump back here to start
	 * processing the child. We will not recurse further in one call, because
	 * processing the child will always begin in JBI_ARRAY_START or
	 * JBI_OBJECT_START state.
	 */
recurse:
	it = (JsonbcContainterIterator *) *pit;

	switch (it->state)
	{
		case JBI_ARRAY_START:
			/*
			 * Set v to array on first array call
			 * v->val.array.elems is not actually set, because we aren't doing
			 * a full conversion
			 */
			JsonValueInitArray(val,
							   it->childrenSize > 0 ? it->isScalar ? 1 : -1 : 0,
							   0, it->isScalar, true);

			it->childrenPtr = it->children;
			it->curDataOffset = 0;
			it->curValueOffset = 0;	/* not actually used */
			/* Set state for next call */
			it->state = JBI_ARRAY_ELEM;
			return WJB_BEGIN_ARRAY;

		case JBI_ARRAY_ELEM:
			if (it->childrenPtr >= it->children + it->childrenSize)
			{
				/*
				 * All elements within array already processed.  Report this
				 * to caller, and give it back original parent iterator (which
				 * independently tracks iteration progress at its level of
				 * nesting).
				 */
				*pit = JsonIteratorFreeAndGetParent(*pit);
				return WJB_END_ARRAY;
			}

			if (it->childrenPtr >= it->chunkEnd)
			{
				it->childrenPtr = it->chunkEnd;
				varbyte_decode(&it->childrenPtr);
				varbyte_decode(&it->childrenPtr);
				it->chunkEnd += JBC_OFFSETS_CHUNK_SIZE;
			}

			entry = varbyte_decode(&it->childrenPtr);
			if (entry == 0)
			{
				it->childrenPtr = it->chunkEnd;
				varbyte_decode(&it->childrenPtr);
				varbyte_decode(&it->childrenPtr);
				entry = varbyte_decode(&it->childrenPtr);
				it->chunkEnd += JBC_OFFSETS_CHUNK_SIZE;
			}

			jsonbcFillValue(entry, it->dataProper, it->curDataOffset, val,
							it->dict);

			it->curDataOffset += JBE_LENGTH(entry);

			if (!JsonValueIsScalar(val) && !skipNested)
			{
				/* Recurse into container. */
				*pit = jsonbcIteratorFromContainer(val->val.binary.data, it);
				goto recurse;
			}
			else
			{
				/*
				 * Scalar item in array, or a container and caller didn't want
				 * us to recurse into it.
				 */
				return WJB_ELEM;
			}

		case JBI_OBJECT_START:
			/*
			 * Set v to object on first object call
			 * v->val.object.pairs is not actually set, because we aren't
			 * doing a full conversion
			 */
			JsonValueInitObject(val, it->childrenSize > 0 ? -1 : 0, 0, true);

			it->childrenPtr = it->children;
			it->curKey = 0;
			it->curDataOffset = 0;
			it->curValueOffset = 0;	/* not actually used */
			/* Set state for next call */
			it->state = JBI_OBJECT_KEY;
			return WJB_BEGIN_OBJECT;

		case JBI_OBJECT_KEY:
			if (it->childrenPtr >= it->children + it->childrenSize)
			{
				/*
				 * All pairs within object already processed.  Report this to
				 * caller, and give it back original containing iterator
				 * (which independently tracks iteration progress at its level
				 * of nesting).
				 */
				*pit = JsonIteratorFreeAndGetParent(*pit);
				return WJB_END_OBJECT;
			}
			else
			{
				JsonbcKeyName keyName;

				if (it->childrenPtr >= it->chunkEnd)
				{
					it->childrenPtr = it->chunkEnd;
					varbyte_decode(&it->childrenPtr);
					varbyte_decode(&it->childrenPtr);
					it->chunkEnd += JBC_OFFSETS_CHUNK_SIZE;
				}

				keyIncr = varbyte_decode(&it->childrenPtr);
				if (keyIncr == 0)
				{
					it->childrenPtr = it->chunkEnd;
					varbyte_decode(&it->childrenPtr);
					varbyte_decode(&it->childrenPtr);
					keyIncr = varbyte_decode(&it->childrenPtr);
					it->chunkEnd += JBC_OFFSETS_CHUNK_SIZE;
				}

				it->curKey += keyIncr;

				keyName = jsonbcDictGetNameById(it->dict, it->curKey);

				val->type = jbvString;
				val->val.string.val = keyName.s;
				val->val.string.len = keyName.len;

				/* Set state for next call */
				it->state = JBI_OBJECT_VALUE;
				return WJB_KEY;
			}

		case JBI_OBJECT_VALUE:
			/* Set state for next call */
			it->state = JBI_OBJECT_KEY;

			entry = varbyte_decode(&it->childrenPtr);

			jsonbcFillValue(entry, it->dataProper, it->curDataOffset, val,
							it->dict);

			it->curDataOffset += JBE_LENGTH(entry);

			/*
			 * Value may be a container, in which case we recurse with new,
			 * child iterator (unless the caller asked not to, by passing
			 * skipNested).
			 */
			if (!JsonValueIsScalar(val) && !skipNested)
			{
				*pit = jsonbcIteratorFromContainer(val->val.binary.data, it);
				goto recurse;
			}
			else
				return WJB_VALUE;
	}

	elog(ERROR, "invalid iterator state");
	return -1;
}

/*
 * Initialize an iterator for iterating all elements in a container.
 */
static JsonIterator *
jsonbcIteratorFromContainer(JsonContainer *jsc,
							JsonbcContainterIterator *parent)
{
	JsonbcContainer			   *container = (JsonbcContainer *) jsc->data;
	JsonbcContainterIterator   *it;
	const unsigned char		   *ptr;
	uint32						header = jsonbcGetHeader(container, &ptr);

	it = palloc(sizeof(JsonbcContainterIterator));

	it->ji.container = jsc;
	it->ji.parent = &parent->ji;
	it->ji.next = JsonbcIteratorNext;

	it->dict = jsonbcGetDictId(jsc);
	it->childrenSize = (header >> JBC_CSHIFT);

	/* Array starts just after header */
	it->children = ptr;
	it->chunkEnd = ptr + JBC_OFFSETS_CHUNK_SIZE;
	it->dataProper = (const char *)(ptr + it->childrenSize);

	switch (header & JBC_MASK)
	{
		case JBC_FSCALAR:
			it->state = JBI_ARRAY_START;
			it->isScalar = true;
			break;
		case JBC_FARRAY:
			it->state = JBI_ARRAY_START;
			it->isScalar = false;
			break;

		case JBC_FOBJECT:
			it->state = JBI_OBJECT_START;
			break;

		default:
			elog(ERROR, "unknown type of jsonbc container");
	}

	return &it->ji;
}

/*
 * Given a JsonbcContainer, expand to JsonbcIterator to iterate over items
 * fully expanded to in-memory representation for manipulation.
 *
 * See JsonbcIteratorNext() for notes on memory management.
 */
static JsonIterator *
JsonbcIteratorInit(JsonContainer *container)
{
	return jsonbcIteratorFromContainer(container, NULL);
}

typedef struct JsonbcIterator
{
	 const unsigned char   *ptr;
	 const unsigned char   *end;
	 const unsigned char   *nextChunk;
	 uint32					offset;
	 JEntry					entry;
	 JsonbcDictId			dict;
} JsonbcIterator;

static inline bool
jsonbcIteratorInit(JsonbcIterator *it, JsonContainer *jc,
				   bool array, bool error)
{
	const unsigned char	   *ptr;
	JsonbcContainer  	   *cont = jc->data;
	uint32					header = jsonbcGetHeader(cont, &ptr);

	if (array ? (header & JBC_MASK) != JBC_FARRAY &&
				(header & JBC_MASK) != JBC_FSCALAR
			  : (header & JBC_MASK) != JBC_FOBJECT)
	{
		if (error)
			elog(ERROR, "not a jsonbc %s", array ? "array" : "object");
		return false;
	}

	it->ptr = ptr;
	it->end = ptr + (header >> JBC_CSHIFT);
	it->nextChunk = ptr + JBC_OFFSETS_CHUNK_SIZE;
	it->offset = 0;
	it->entry = 0;
	it->dict = jsonbcGetDictId(jc);

	return (header >> JBC_CSHIFT) != 0;
}

static inline bool
jsonbcIteratorNextChunk(JsonbcIterator *it,
						const unsigned char *chunkOffsetPtr)
{
	if (chunkOffsetPtr)
		it->ptr = chunkOffsetPtr;
	else
	{
		it->ptr = it->nextChunk;
		varbyte_decode(&it->ptr);	/* skip index */
	}

	it->offset = varbyte_decode(&it->ptr);
	it->nextChunk += JBC_OFFSETS_CHUNK_SIZE;

	return it->ptr < it->end;
}

static inline bool
jsonbcIteratorNextEntry(JsonbcIterator *it, uint32 *entry)
{
	if (it->ptr >= it->end)
		return false;

	if (it->ptr < it->nextChunk)
		it->offset += JBE_LENGTH(it->entry);
	else if (!jsonbcIteratorNextChunk(it, NULL))
		return false;

	*entry = varbyte_decode(&it->ptr);

	if (*entry == 0)
	{
		if (!jsonbcIteratorNextChunk(it, NULL))
			return false;
		*entry = varbyte_decode(&it->ptr);
		Assert(*entry != 0);
	}

	return true;
}

static inline JsonValue *
jsonbcIteratorCurrentValue(const JsonbcIterator *it, JsonValue *result)
{
	if (!result)
		result = palloc(sizeof(JsonValue));

	jsonbcFillValue(it->entry, (const char *) it->end, it->offset, result,
					it->dict);

	return result;
}

static inline bool
jsonbcIteratorNextElement(JsonbcIterator *it, JsonValue **presult)
{
	if (!jsonbcIteratorNextEntry(it, &it->entry))
		return false;

	if (presult)
		*presult = jsonbcIteratorCurrentValue(it, *presult);

	return true;
}

static inline bool
jsonbcIteratorNextKey(JsonbcIterator *it, JsonValue **presult, uint32 *key)
{
	uint32	keyInc;

	if (!jsonbcIteratorNextEntry(it, &keyInc))
		return false;

	if (it->ptr >= it->end)
		return false;

	*key += keyInc;

	it->entry = varbyte_decode(&it->ptr);

	if (presult)
		*presult = jsonbcIteratorCurrentValue(it, *presult);

	return true;
}

static inline uint32
jsonbcIteratorPeekNextChunkIndex(JsonbcIterator *it,
								 const unsigned char **chunkOffsetPtr)
{
	*chunkOffsetPtr = it->nextChunk;
	return varbyte_decode(chunkOffsetPtr);
}

static inline uint32
jsonbcIteratorSkipChunksToIth(JsonbcIterator *it, uint32 i)
{
	uint32 index = 0;

	while (it->nextChunk < it->end)
	{
		const unsigned char *chunkOffsetPtr;
		uint32 nexti = jsonbcIteratorPeekNextChunkIndex(it, &chunkOffsetPtr);

		if (nexti > i)
			break;

		index = nexti;

		jsonbcIteratorNextChunk(it, chunkOffsetPtr);
	}

	return index;
}

static inline JsonValue *
jsonbcIteratorGetIthElement(JsonbcIterator *it, uint32 i)
{
	JsonValue  *value = NULL;
	uint32		j = jsonbcIteratorSkipChunksToIth(it, i);

	while (j++ < i)
	{
		if (!jsonbcIteratorNextElement(it, NULL))
			return NULL;
	}

	jsonbcIteratorNextElement(it, &value);

	return value;
}

static inline JsonValue *
jsonbcIteratorFindKey(JsonbcIterator *it, uint32 key)
{
	uint32	k = key ? jsonbcIteratorSkipChunksToIth(it, key - 1) : 0;

	Assert(key && k < key);

	while (k < key && jsonbcIteratorNextKey(it, NULL, &k))
		if (k == key)
			return jsonbcIteratorCurrentValue(it, NULL);

	return NULL;
}

static JsonValue *
jsonbcFindKeyInObject(JsonContainer *jc, const JsonValue *key)
{
	JsonbcIterator	it;
	JsonbcKeyId		keyId;

	/* Object key passed by caller must be a string */
	Assert(key->type == jbvString);

	if (!jsonbcIteratorInit(&it, jc, false, false))
		return NULL;

	keyId = jsonbcConvertKeyNameToId(it.dict, key, false);

	if (keyId == JsonbcInvalidKeyId)
		return NULL;

	return jsonbcIteratorFindKey(&it, keyId);
}

static JsonValue *
jsonbcFindValueInArray(JsonContainer *jc, const JsonValue *key)
{
	JsonbcIterator	it;
	JsonValue	   *result;

	if (!jsonbcIteratorInit(&it, jc, true, false))
		return NULL;

	result = palloc(sizeof(JsonbValue));

	while (jsonbcIteratorNextElement(&it, &result))
		if (key->type == result->type && JsonValueScalarEquals(key, result))
			return result;

	pfree(result);
	return NULL;
}

static JsonValue *
jsonbcGetArrayElement(JsonContainer *jc, uint32 index)
{
	JsonbcIterator	it;

	if (!jsonbcIteratorInit(&it, jc, true, true))
		return NULL;

	return jsonbcIteratorGetIthElement(&it, index);
}

static uint32
jsonbcGetArraySize(JsonContainer *jc)
{
	JsonbcIterator	it;
	uint32			size;

	if (!jsonbcIteratorInit(&it, jc, true, true))
		return 0;

	size = jsonbcIteratorSkipChunksToIth(&it, UINT32_MAX);

	while (jsonbcIteratorNextElement(&it, NULL))
			size++;

	return size;
}

static Size
jsonbcEncodeOptions(JsonContainer *jc, void *buf)
{
	if (buf)
	{
		JsonbcDictId dict = jsonbcGetDictId(jc);
		memcpy(buf, &dict, sizeof(JsonbcDictId));
	}

	return sizeof(JsonbcDictId);
}

static Size
jsonbcDecodeOptions(const void *buf, CompressionOptions *options)
{
	JsonbcDictId dict;
	memcpy(&dict, buf, sizeof(JsonbcDictId));
	*options = GetCompressionOptionsFromJsonbcDictId(dict);
	return sizeof(JsonbcDictId);
}

static bool
jsonbcOptionsAreEqual(JsonContainer *jc, CompressionOptions options)
{
	return jsonbcGetDictId(jc) == GetJsonbcDictIdFromCompressionOptions(options);
}

static JsonCompressionOptionsOps
jsonbcCompressionOptionsOps =
{
	jsonbcEncodeOptions,
	jsonbcDecodeOptions,
	jsonbcOptionsAreEqual
};

JsonContainerOps
jsonbcContainerOps =
{
	JsonContainerUnknown,
	&jsonbcCompressionOptionsOps,
	jsonbcInit,
	JsonbcIteratorInit,
	jsonbcFindKeyInObject,
	jsonbcFindValueInArray,
	jsonbcGetArrayElement,
	jsonbcGetArraySize,
	JsonbToCStringRaw,
	JsonCopyFlat,
};

static void
JsonbcEncode(StringInfoData *buffer, const JsonValue *val,
			 CompressionOptions options)
{
	JsonbcDictId	dict = GetJsonbcDictIdFromCompressionOptions(options);

	(void) jsonbcEncodeValue(buffer, val, 0, dict);
}

static inline Datum
jsonbcCompress(Json *json, CompressionOptions options)
{
	JsonValue	jvbuf;
	JsonValue  *jv = JsonToJsonValue(json, &jvbuf);
	Jsonbc	   *jsonbc = JsonValueToJsonbc(jv, options);

	return JsonbcGetDatum(jsonbc);
}

static Datum
jsonbcCompressJsont(Datum value, CompressionOptions options)
{
	Json   *json = DatumGetJsont(value);
	Datum	res = jsonbcCompress(json, options);

	JsonFreeIfCopy(json, value);

	return res;
}

static Datum
jsonbcCompressJsonb(Datum value, CompressionOptions options)
{
	Json   *json = DatumGetJsonb(value);
	Datum	res = jsonbcCompress(json, options);

	JsonFreeIfCopy(json, value);

	return res;
}

static Datum
jsonbcDecompress(Datum value, CompressionOptions options)
{
	Json *json = DatumGetJson(value, &jsonbcContainerOps, options, NULL);

	return JsonGetDatum(json);
}

#define JSONBC_DICT_ID_OPTION	"dict_id"
#define JSONBC_DICT_ENUM_OPTION	"dict_enum"

static JsonbcDictId
jsonbcOptionsProcess(List *options, bool replaceNamesWithOids)
{
	ListCell   *cell;
	Oid			dictId = InvalidOid;
	Oid			dictEnum = InvalidOid;
	char		dictIdStr[20];

	foreach(cell, options)
	{
		DefElem    *def = lfirst(cell);

		if (!strcmp(def->defname, JSONBC_DICT_ID_OPTION))
		{
			dictId = (int32) DatumGetObjectId(DirectFunctionCall1(
							regclassin, CStringGetDatum(defGetString(def))));

			if (!OidIsValid(dictId))
				elog(ERROR,
					 "jsonbc compression method: invalid value for option '%s'",
					 def->defname);

			if (replaceNamesWithOids)
			{
				snprintf(dictIdStr, sizeof(dictIdStr), "%d", dictId);
				def->arg = (Node *) makeString(pstrdup(dictIdStr));
			}
		}
		else if (!strcmp(def->defname, JSONBC_DICT_ENUM_OPTION))
		{
			dictEnum = (int32) DatumGetObjectId(DirectFunctionCall1(
							regtypein, CStringGetDatum(defGetString(def))));

			if (!OidIsValid(dictEnum))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("type \"%s\" does not exist", defGetString(def))));

			if (!type_is_enum(dictEnum))
				elog(ERROR,
					 "jsonbc compression method: '%s' is not an enum type",
					 format_type_be(dictEnum));

			if (replaceNamesWithOids)
			{
				snprintf(dictIdStr, sizeof(dictIdStr), "%d", dictEnum);
				def->arg = (Node *) makeString(pstrdup(dictIdStr));
			}
		}
		else
			elog(ERROR, "jsonbc compression method: unrecognized option '%s'",
				 def->defname);
	}

	if (!OidIsValid(dictId) && !OidIsValid(dictEnum))
		elog(ERROR, "jsonbc compression method: option '%s' is required",
			 JSONBC_DICT_ID_OPTION);

	if (OidIsValid(dictId) && OidIsValid(dictEnum))
		elog(ERROR, "jsonbc compression method: options '%s' and '%s' are mutually exclusive",
			 JSONBC_DICT_ID_OPTION, JSONBC_DICT_ENUM_OPTION);

	return dictId ? dictId : dictEnum | JsonbcDictIdEnumFlag;
}

static List *
jsonbcOptionsValidate(Form_pg_attribute attr, List *options)
{
	Oid			basetypid = getBaseType(attr->atttypid);

	if (basetypid != JSONOID && basetypid != JSONBOID)
		elog(ERROR, "jsonbc compression method is only applicable to json type");

	if (options != NIL)
		jsonbcOptionsProcess(options, true);
	else
	{
		JsonbcDictId	dictId = jsonbcDictCreate(attr);
		char			dictIdStr[20];
		DefElem		   *def;
		Value		   *val;

		snprintf(dictIdStr, sizeof(dictIdStr), "%d", dictId);
		val = makeString(pstrdup(dictIdStr));
		def = makeDefElem(pstrdup(JSONBC_DICT_ID_OPTION), (Node *) val, -1);
		options = list_make1(def);
	}

	return options;
}

static CompressionOptions
jsonbcOptionsConvert(Form_pg_attribute attr, List *options)
{
	return GetCompressionOptionsFromJsonbcDictId(
					jsonbcOptionsProcess(options, false));
}

static bool
jsonbcOptionsEqual(CompressionOptions options1, CompressionOptions options2)
{
	return GetJsonbcDictIdFromCompressionOptions(options1) ==
		   GetJsonbcDictIdFromCompressionOptions(options2);
}

static CompressionMethodOptionsRoutines
jsonbcCompressionMethodOptionsRoutines =
{
	jsonbcOptionsValidate,
	jsonbcOptionsConvert,
	NULL,
	NULL,
	jsonbcOptionsEqual,
	jsonbcDecodeOptions,
};

static void
jsonbcAddAttr(Form_pg_attribute attr, List *options)
{
	JsonbcDictId dict = jsonbcOptionsProcess(options, false);
	jsonbcDictAddRef(attr, dict);
}

static void
jsonbcDropAttr(Form_pg_attribute attr, List *options)
{
	JsonbcDictId dict = jsonbcOptionsProcess(options, false);
	jsonbcDictRemoveRef(attr, dict);
}

Datum
jsonbc_handler(PG_FUNCTION_ARGS)
{
	CompressionMethodOpArgs	   *opargs = (CompressionMethodOpArgs *)
														PG_GETARG_POINTER(0);
	CompressionMethodRoutine   *cmr = makeNode(CompressionMethodRoutine);
	Oid							typeid = opargs->args.getRoutine.typeid;

	if (OidIsValid(typeid) && typeid != JSONOID && typeid != JSONBOID)
		elog(ERROR, "unexpected type %d for jsonbc compression method", typeid);

	jsonbcContainerOps.type = opargs->args.getRoutine.cmhanderid; /* FIXME */

	cmr->flags = CM_EXTENDED_REPRESENTATION;
	cmr->options = &jsonbcCompressionMethodOptionsRoutines;
	cmr->addAttr = jsonbcAddAttr;
	cmr->dropAttr = jsonbcDropAttr;
	cmr->compress = !OidIsValid(typeid) ? NULL :
			typeid == JSONOID ? jsonbcCompressJsont : jsonbcCompressJsonb;
	cmr->decompress = jsonbcDecompress;

	PG_RETURN_POINTER(cmr);
}

PG_FUNCTION_INFO_V1(jsonbc_get_dict_id);

Datum
jsonbc_get_dict_id(PG_FUNCTION_ARGS)
{
	Json *json = DatumGetJsont(PG_GETARG_DATUM(0));

	if (json->root.ops == &jsonbcContainerOps)
		PG_RETURN_INT32(jsonbcGetDictId(JsonRoot(json)));
	else
		PG_RETURN_NULL();
}
