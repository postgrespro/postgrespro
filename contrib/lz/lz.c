/*
 * contrib/lz/lz.c
 *
 *  Created on: Oct 11, 2016
 *      Author: Nikita Glukhov
 */

#include "postgres.h"
#include "fmgr.h"
#include "access/compression.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_compression.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#define HAVE_LZ4
#define HAVE_ZSTD
#define HAVE_SNAPPY

#ifdef HAVE_LZ4
#include <lz4.h>
#include <lz4hc.h>
#endif

#ifdef HAVE_ZSTD
#include <zstd.h>
#endif

#ifdef HAVE_SNAPPY
#include <snappy-c.h>
#endif

PG_MODULE_MAGIC;

typedef struct lz_compress_header
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		rawsize;
} lz_compress_header;

#define LZ_COMPRESS_HDRSZ			((int32) sizeof(lz_compress_header))
#define LZ_COMPRESS_RAWDATA(ptr)	(((char *) (ptr)) + LZ_COMPRESS_HDRSZ)
#define LZ_COMPRESS_RAWSIZE(ptr)	(((lz_compress_header *) (ptr))->rawsize)
#define LZ_COMPRESS_SET_RAWSIZE(ptr, len) (LZ_COMPRESS_RAWSIZE(ptr) = (len))

typedef int32 CompressionLevel;

#define CompressionOptionsGetLevel(options) \
		((CompressionLevel)(intptr_t)(options))

#define CompressionOptionsFromLevel(level) \
		((CompressionOptions)(intptr_t)(level))

#define CLEVEL_OPTION	"clevel"

static CompressionLevel
clevelOptionsProcess(List *options)
{
	ListCell		   *cell;
	CompressionLevel	clevel = 0;

	foreach(cell, options)
	{
		DefElem    *def = lfirst(cell);

		if (!strcmp(def->defname, CLEVEL_OPTION))
			clevel = pg_atoi(defGetString(def), 4, 0);
		else
			elog(ERROR, "unrecognized compression level option '%s'",
				 def->defname);
	}

	return clevel;
}

static List *
clevelOptionsValidate(Form_pg_attribute attr, List *options)
{
	clevelOptionsProcess(options);
	return options;
}

static CompressionOptions
clevelOptionsConvert(Form_pg_attribute attr, List *options)
{
	return CompressionOptionsFromLevel(clevelOptionsProcess(options));
}

static bool
clevelOptionsEqual(CompressionOptions options1, CompressionOptions options2)
{
	return CompressionOptionsGetLevel(options1) ==
		   CompressionOptionsGetLevel(options2);
}

static CompressionMethodOptionsRoutines
CompressionLevelOptionsRoutines =
{
		clevelOptionsValidate,
		clevelOptionsConvert,
		NULL,
		NULL,
		clevelOptionsEqual,
};

typedef size_t (*LzCompressBoundFunc)(size_t len);

typedef size_t (*LzCompressFunc)(const void *data, void *cdata,
								 size_t len, size_t clen,
								 CompressionOptions options);

typedef void (*LzDecompressFunc)(const void *cdata, void *data,
								 size_t clen, size_t len,
								 CompressionOptions options);

static inline Datum
lz_compress(Datum datum, CompressionOptions options,
			LzCompressBoundFunc bound, LzCompressFunc compress)
{
	void   *val = (void *) PG_DETOAST_DATUM(datum);
	int32	valsize = VARSIZE_ANY_EXHDR(val);
	int32	cvalsize = bound(valsize);
	void   *cval = palloc(cvalsize + LZ_COMPRESS_HDRSZ);
	void   *cvaldata = LZ_COMPRESS_RAWDATA(cval);
	void   *valdata = VARDATA_ANY(val);

	cvalsize = compress(valdata, cvaldata, valsize, cvalsize, options);

	LZ_COMPRESS_SET_RAWSIZE(cval, valsize);
	SET_VARSIZE(cval, cvalsize + LZ_COMPRESS_HDRSZ);

	if (val != DatumGetPointer(datum))
		pfree(val);

	return PointerGetDatum(cval);
}

static inline Datum
lz_decompress(Datum cdatum, CompressionOptions options,
			  LzDecompressFunc decompress)
{
	void   *cval     = (void *) PG_DETOAST_DATUM(cdatum);
	size_t	cvalsize = VARSIZE(cval) - LZ_COMPRESS_HDRSZ;
	size_t	valsize  = LZ_COMPRESS_RAWSIZE(cval);
	void   *cvaldata = LZ_COMPRESS_RAWDATA(cval);
	void   *val      = palloc(valsize + VARHDRSZ);
	void   *valdata  = VARDATA(val);

	SET_VARSIZE(val, valsize + VARHDRSZ);

	decompress(cvaldata, valdata, cvalsize, valsize, options);

	if (cval != DatumGetPointer(cdatum))
		pfree(cval);

	return PointerGetDatum(val);
}

static void
lzAddAttr(Form_pg_attribute att, List *options)
{
	int16	typlen;
	bool	typbyval;

	get_typlenbyval(att->atttypid, &typlen, &typbyval);

	if (typbyval || typlen != -1)
		elog(ERROR, "lz compression method is only applicable to "
					"variable-length types");
}

static CompressionMethodRoutine *
lz_handler(CompressionRoutine compress,
		   DecompressionRoutine decompress,
		   CompressionMethodOptionsRoutines *options)
{
	CompressionMethodRoutine *cmr = makeNode(CompressionMethodRoutine);

	cmr->flags = 0;
	cmr->options =  options;
	cmr->addAttr = lzAddAttr;
	cmr->dropAttr = NULL;
	cmr->compress = compress;
	cmr->decompress = decompress;

	return cmr;
}

#ifdef HAVE_LZ4

#define LZ4_DICT_MAX_SIZE	0x10000

typedef Oid Lz4dDictId;

typedef struct Lz4dOptions
{
	CompressionLevel	clevel;
	Lz4dDictId			dict;
} Lz4dOptions;

typedef struct Lz4dDictionary
{
	char   *data;
	int		size;
} Lz4dDictionary;

typedef struct Lz4dOptionsExt
{
	Lz4dOptions		opts;
	Lz4dDictionary	dictionary;
	LZ4_stream_t   *streamTemplate;
} Lz4dOptionsExt;


static SPIPlanPtr savedPlanFindDict = NULL;

static Oid
lz4DictFindByName(const char *name)
{
	Datum			args[1];
	Oid				argTypes[1] = { NAMEOID };
	Oid				id;
	bool			isnull;

	SPI_connect();

	if (!savedPlanFindDict)
	{
		savedPlanFindDict = SPI_prepare(
			"SELECT oid FROM lz4_dictionary WHERE dict_name = $1",
			lengthof(argTypes), argTypes);
		if (!savedPlanFindDict)
			elog(ERROR, "Error preparing query");
		if (SPI_keepplan(savedPlanFindDict))
			elog(ERROR, "Error keeping plan");
	}

	args[0] = NameGetDatum(name);

	if (SPI_execute_plan(savedPlanFindDict, args, NULL, false, 1) < 0 ||
		SPI_processed != 1)
		elog(ERROR, "Failed to find lz4 dictionary '%s'", name);

	id = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc, 1, &isnull));

	SPI_finish();

	return id;
}

#define LZ4D_DICT_OPTION	"dict"

static Lz4dOptions
lz4dOptionsProcess(List *optionList)
{
	ListCell	   *cell;
	Lz4dOptions		options = { 0, 0 };

	foreach(cell, optionList)
	{
		DefElem    *def = lfirst(cell);

		if (!strcmp(def->defname, CLEVEL_OPTION))
			options.clevel = pg_atoi(defGetString(def), 4, 0);
		else if (!strcmp(def->defname, LZ4D_DICT_OPTION))
		{
			const char *dict_name_or_oid = defGetString(def);

			if (dict_name_or_oid[0] >= '0' &&
				dict_name_or_oid[0] <= '9' &&
				strspn(dict_name_or_oid, "0123456789") ==
				strlen(dict_name_or_oid))
				options.dict = DatumGetObjectId(DirectFunctionCall1(oidin,
											CStringGetDatum(dict_name_or_oid)));
			else
			{
				char dictIdStr[20];

				options.dict = lz4DictFindByName(dict_name_or_oid);

				snprintf(dictIdStr, sizeof(dictIdStr), "%d", options.dict);
				def->arg = (Node *) makeString(pstrdup(dictIdStr));
			}
		}
		else
			elog(ERROR, "unrecognized lz4d compression option '%s'",
				 def->defname);
	}

	if (!options.dict)
		elog(ERROR, "lz4d compression method: option '%s' is required",
			 LZ4D_DICT_OPTION);

	return options;
}

static List *
lz4dOptionsValidate(Form_pg_attribute attr, List *options)
{
	lz4dOptionsProcess(options);
	return options;
}

static SPIPlanPtr savedPlanLoadDict = NULL;

static Lz4dDictionary
lz4dDictLoad(Oid dictId, MemoryContext mcxt)
{
	Lz4dDictionary	dict;
	struct varlena *dictVal;
	Datum			dictDatum;
	Datum			args[1];
	Oid				argTypes[1] = { OIDOID };
	bool			isnull;

	SPI_connect();

	if (!savedPlanLoadDict)
	{
		savedPlanLoadDict = SPI_prepare(
			"SELECT dict_data FROM lz4_dictionary WHERE oid = $1",
			lengthof(argTypes), argTypes);
		if (!savedPlanLoadDict)
			elog(ERROR, "Error preparing query");
		if (SPI_keepplan(savedPlanLoadDict))
			elog(ERROR, "Error keeping plan");
	}

	args[0] = ObjectIdGetDatum(dictId);

	if (SPI_execute_plan(savedPlanLoadDict, args, NULL, false, 1) < 0 ||
		SPI_processed != 1)
		elog(ERROR, "Failed to load lz4 dictionary");

	dictDatum = SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc, 1, &isnull);

	dictVal = PG_DETOAST_DATUM(dictDatum);

	dict.size = VARSIZE_ANY(dictVal);

	if (dict.size > LZ4_DICT_MAX_SIZE)
		dict.size = LZ4_DICT_MAX_SIZE;

	dict.data = memcpy(MemoryContextAlloc(mcxt, dict.size),
					   (char *) VARDATA_ANY(dictVal) +
						(dict.size < LZ4_DICT_MAX_SIZE ? 0 :
						 VARSIZE_ANY(dictVal) - LZ4_DICT_MAX_SIZE),
					   dict.size);

	SPI_finish();

	return dict;
}

static SPIPlanPtr savedPlanAddSample = NULL;

static void
lz4dDictAddSample(Lz4dDictId dictId, void *sample)
{
	Datum	args[2];
	Oid		argTypes[] = { BYTEAOID, OIDOID };

	SPI_connect();

	if (!savedPlanAddSample)
	{
		savedPlanAddSample = SPI_prepare(
			"UPDATE lz4_dictionary "
			"SET dict_data = $1 || dict_data "
			"WHERE oid = $2",
			lengthof(argTypes), argTypes);
		if (!savedPlanAddSample)
			elog(ERROR, "Error preparing query");
		if (SPI_keepplan(savedPlanAddSample))
			elog(ERROR, "Error keeping plan");
	}

	args[0] = PointerGetDatum(sample);
	args[1] = ObjectIdGetDatum(dictId);

	if (SPI_execute_plan(savedPlanAddSample, args, NULL, false, 0) < 0 ||
		SPI_processed != 1)
		elog(ERROR, "Failed to insert sample into lz4_dictionary");

	SPI_finish();
}

static void
lz4dDictInvalidate(Lz4dDictId dictId)
{
	Relation	depRel;
	ScanKeyData	keys[2];
	SysScanDesc scan;
	HeapTuple	tup;
	Oid			cmoid = get_compression_method_oid("lz4d", true);

	if (!OidIsValid(cmoid))
		return;

	depRel = heap_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&keys[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(CompressionMethodRelationId));
	ScanKeyInit(&keys[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(cmoid));

	scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL,
							  2, keys);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend depend = (Form_pg_depend) GETSTRUCT(tup);
		if (depend->classid == RelationRelationId) /* TODO check dict id */
			CacheInvalidateRelcacheByRelid(depend->objid);
	}

	systable_endscan(scan);

	heap_close(depRel, AccessShareLock);
}

static CompressionOptions
lz4dOptionsConvert(Form_pg_attribute attr, List *options)
{
	Lz4dOptions		opts = lz4dOptionsProcess(options);
	Lz4dOptionsExt *extopts = palloc(sizeof(*extopts));

	extopts->opts = opts;
	extopts->dictionary.data = NULL;
	extopts->dictionary.size = 0;
	extopts->streamTemplate = NULL;

	return (CompressionOptions) extopts;
}

static void
lz4dOptionsFree(CompressionOptions options)
{
	Lz4dOptionsExt *extopts = (Lz4dOptionsExt *) options;

	if (extopts->dictionary.data)
		pfree(extopts->dictionary.data);

	if (extopts->streamTemplate)
		LZ4_freeStream(extopts->streamTemplate);

	pfree(extopts);
}

static CompressionOptions
lz4dOptionsCopy(CompressionOptions options)
{
	Lz4dOptionsExt *copy = palloc(sizeof(*copy));

	copy->opts = ((Lz4dOptionsExt *) options)->opts;
	copy->streamTemplate = NULL;
	copy->dictionary.data = NULL;
	copy->dictionary.size = 0;

	return copy;
}

static bool
lz4dOptionsEqual(CompressionOptions options1, CompressionOptions options2)
{
	Lz4dOptionsExt *o1 = (Lz4dOptionsExt *) options1;
	Lz4dOptionsExt *o2 = (Lz4dOptionsExt *) options2;

	return o1->opts.clevel == o2->opts.clevel &&
		   o1->opts.dict == o2->opts.dict;
}

static CompressionMethodOptionsRoutines
Lz4dOptionsRoutines =
{
		lz4dOptionsValidate,
		lz4dOptionsConvert,
		lz4dOptionsFree,
		lz4dOptionsCopy,
		lz4dOptionsEqual,
};

static inline size_t
lz4CompressBound(size_t len)
{
	return LZ4_compressBound((int) len);
}

static inline size_t
lz4Compress(const void *data, void *cdata, size_t len, size_t maxclen,
			CompressionOptions options)
{
	CompressionLevel clevel = CompressionOptionsGetLevel(options);
	int clen = LZ4_compress_fast(data, cdata, (int) len, (int) maxclen, clevel);

	if (clen <= 0)
		elog(ERROR, "LZ4_compress_fast() error: %d\n", -clen);

	return clen;
}

static inline size_t
lz4hcCompress(const void *data, void *cdata, size_t len, size_t clen,
			  CompressionOptions options)
{
	CompressionLevel clevel = CompressionOptionsGetLevel(options);

	clen = LZ4_compress_HC(data, cdata, len, clen, clevel);

	if (clen <= 0)
		elog(ERROR, "LZ4_compress_HC() error %d\n", (int) clen);

	return clen;
}

static inline size_t
lz4dCompress(const void *data, void *cdata, size_t len, size_t maxclen,
			 CompressionOptions coptions)
{
	Lz4dOptionsExt *options = (Lz4dOptionsExt *) coptions;
	LZ4_stream_t	stream;
	int				clen;

	if (!options->dictionary.data)
		options->dictionary = lz4dDictLoad(options->opts.dict,
										   CacheMemoryContext);

	if (!options->streamTemplate)
	{
		options->streamTemplate = LZ4_createStream();
		LZ4_resetStream(options->streamTemplate);
		LZ4_loadDict(options->streamTemplate,
					 options->dictionary.data,
					 options->dictionary.size);
	}

	stream = *options->streamTemplate;

	clen = LZ4_compress_fast_continue(&stream, data, cdata,
									  (int) len, (int) maxclen,
									  options->opts.clevel);
	if (clen <= 0)
		elog(ERROR, "LZ4_compress_fast_continue() error %d\n", clen);

	return clen;
}

static inline void
lz4Decompress(const void *cdata, void *data, size_t clen, size_t len,
			  CompressionOptions options)
{
	int rlen = LZ4_decompress_fast(cdata, data, len);

	if (rlen != clen)
		elog(ERROR, "LZ4_decompress_fast() error: %d", (int) rlen);
}

static inline void
lz4dDecompress(const void *cdata, void *data, size_t clen, size_t len,
			   CompressionOptions coptions)
{
	Lz4dOptionsExt *opts = (Lz4dOptionsExt *) coptions;
	int				rlen;

	if (!opts->dictionary.data)
		opts->dictionary = lz4dDictLoad(opts->opts.dict, CacheMemoryContext);

	rlen = LZ4_decompress_fast_usingDict(cdata, data, len,
										 opts->dictionary.data,
										 opts->dictionary.size);

	if (rlen != clen)
		elog(ERROR, "LZ4_decompress_fast() error: %d", (int) rlen);
}

static Datum
lz4_compress(Datum val, CompressionOptions options)
{
	return lz_compress(val, options, lz4CompressBound, lz4Compress);
}

static Datum
lz4hc_compress(Datum val, CompressionOptions options)
{
	return lz_compress(val, options, lz4CompressBound, lz4hcCompress);
}

static Datum
lz4_decompress(Datum cval, CompressionOptions options)
{
	return lz_decompress(cval, options, lz4Decompress);
}

static Datum
lz4d_compress(Datum val, CompressionOptions options)
{
	return lz_compress(val, options, lz4CompressBound, lz4dCompress);
}

static Datum
lz4d_decompress(Datum cval, CompressionOptions options)
{
	return lz_decompress(cval, options, lz4dDecompress);
}

PG_FUNCTION_INFO_V1(lz4_handler);

Datum
lz4_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(lz_handler(lz4_compress, lz4_decompress,
								 &CompressionLevelOptionsRoutines));
}

PG_FUNCTION_INFO_V1(lz4hc_handler);

Datum
lz4hc_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(lz_handler(lz4hc_compress, lz4_decompress,
								 &CompressionLevelOptionsRoutines));
}

PG_FUNCTION_INFO_V1(lz4d_handler);

Datum
lz4d_handler(PG_FUNCTION_ARGS)
{
	CompressionMethodRoutine *cmr = makeNode(CompressionMethodRoutine);

	cmr->flags = 0;
	cmr->options =  &Lz4dOptionsRoutines;
	cmr->addAttr = NULL; /* FIXME check varlena */
	cmr->dropAttr = NULL;
	cmr->compress = lz4d_compress;
	cmr->decompress = lz4d_decompress;

	return PointerGetDatum(cmr);
}

PG_FUNCTION_INFO_V1(lz4_dictionary_add_sample);

Datum
lz4_dictionary_add_sample(PG_FUNCTION_ARGS)
{
	Oid		typid0 = get_fn_expr_argtype(fcinfo->flinfo, 0);
	Oid		typid1 = get_fn_expr_argtype(fcinfo->flinfo, 1);
	Oid		dictId;
	int16	typlen;
	bool	typbyval;
	struct varlena *sample;

	if (typid0 == NAMEOID)
		dictId = lz4DictFindByName(NameStr(*PG_GETARG_NAME(0)));
	else
		dictId = PG_GETARG_OID(0);

	get_typlenbyval(typid1, &typlen, &typbyval);

	if (typbyval || typlen != -1)
		elog(ERROR, "lz4_dictionary_add_sample(): "
					"sample argument must be of variable-length type");

	sample = PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

	lz4dDictAddSample(dictId, sample);
	lz4dDictInvalidate(dictId);

	PG_FREE_IF_COPY(sample, 1);

	PG_RETURN_VOID();
}
#endif

#ifdef HAVE_ZSTD
static inline size_t
zstdCompressBound(size_t len)
{
	return ZSTD_compressBound(len);
}

static inline size_t
zstdCompress(const void *data, void *cdata, size_t len, size_t clen,
			 CompressionOptions options)
{
	CompressionLevel clevel = CompressionOptionsGetLevel(options);

	clen = ZSTD_compress(cdata, clen, data, len, clevel);

	if (ZSTD_isError(clen))
		elog(ERROR, "ZSTD_compress() error: %s", ZSTD_getErrorName(clen));

	return clen;
}

static inline void
zstdDecompress(const void *cdata, void *data, size_t clen, size_t len,
			   CompressionOptions options)
{
#if 0
	unsigned long long rsize = ZSTD_getDecompressedSize(cdata, clen);

	if (!rsize)
		elog(ERROR, "zstd: original size unknown");

	if (rsize != len)
		elog(ERROR, "zstd: decompressed size %d != raw size %d",
			(int) rsize, (int) len);
#endif
	size_t rlen = ZSTD_decompress(data, len, cdata, clen);

	if (len != rlen)
		elog(ERROR, "ZSTD_decompress() error %s",
			 ZSTD_getErrorName(rlen));
}

static Datum
zstd_compress(Datum val, CompressionOptions options)
{
	return lz_compress(val, options, zstdCompressBound, zstdCompress);
}

static Datum
zstd_decompress(Datum val, CompressionOptions options)
{
	return lz_decompress(val, options, zstdDecompress);
}

PG_FUNCTION_INFO_V1(zstd_handler);

Datum
zstd_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(lz_handler(zstd_compress, zstd_decompress,
								 &CompressionLevelOptionsRoutines));
}
#endif

#ifdef HAVE_SNAPPY
static inline size_t
snappyCompressBound(size_t len)
{
	return snappy_max_compressed_length(len);
}

static inline size_t
snappyCompress(const void *data, void *cdata, size_t len, size_t clen,
			   CompressionOptions options)
{
	snappy_status status = snappy_compress(data, len, cdata, &clen);

	if (status != SNAPPY_OK)
		elog(ERROR, "snappy_compress() error %d", status);

	return clen;
}

static inline void
snappyDecompress(const void *cdata, void *data, size_t clen, size_t len,
				 CompressionOptions options)
{
	size_t			dlen = len;
	snappy_status	status = snappy_uncompress(cdata, clen, data, &dlen);

	if (status != SNAPPY_OK)
		elog(ERROR, "snappy_uncompressed_length(): error %d", status);

	if (dlen != len)
		elog(ERROR, "snappy_uncompressed_length(): "
					"invalid decompressed data length");
}

static Datum
snappy_cm_compress(Datum val, CompressionOptions options)
{
	return lz_compress(val, options, snappyCompressBound, snappyCompress);
}

static Datum
snappy_cm_decompress(Datum val, CompressionOptions options)
{
	return lz_decompress(val, options, snappyDecompress);
}

PG_FUNCTION_INFO_V1(snappy_handler);

Datum
snappy_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(lz_handler(snappy_cm_compress, snappy_cm_decompress,
								 NULL));
}
#endif
