#ifndef JSONBC_DICT_H
#define JSONBC_DICT_H

#include "c.h"
#include "catalog/pg_attribute.h"

typedef int32 JsonbcKeyId;
#define JsonbcKeyIdTypeOid	INT4OID
#define JsonbcKeyIdGetDatum(datum)	Int32GetDatum(datum)
#define DatumGetJsonbcKeyId(datum)	DatumGetInt32(datum)
#define JsonbcInvalidKeyId	(-1)

typedef Oid JsonbcDictId;
#define JsonbcDictIdEnumFlag           0x80000000
#define JsonbcDictIdIsEnum(dictId)     (((dictId) &  JsonbcDictIdEnumFlag) != 0)
#define JsonbcDictIdGetEnumOid(dictId)  ((dictId) & ~JsonbcDictIdEnumFlag)
#define JsonbcDictIdTypeOid	OIDOID
#define JsonbcDictIdGetDatum(datum)	ObjectIdGetDatum(datum)
#define DatumGetJsonbcDictId(datum)	DatumGetObjectId(datum)

typedef struct
{
	const char *s;
	int			len;
} JsonbcKeyName;

extern JsonbcDictId		jsonbcDictCreate(Form_pg_attribute attr);
extern void				jsonbcDictAddRef(Form_pg_attribute attr,
										JsonbcDictId dict);
extern void				jsonbcDictRemoveRef(Form_pg_attribute attr,
											JsonbcDictId dict);
extern JsonbcKeyId		jsonbcDictGetIdByName(JsonbcDictId dict,
											  JsonbcKeyName name, bool insert);
extern JsonbcKeyName	jsonbcDictGetNameById(JsonbcDictId dict, JsonbcKeyId id);
extern void				jsonbcDictRemoveEntryById(JsonbcDictId dict,
												  JsonbcKeyId key);

#endif /* JSONBC_DICT_H */
