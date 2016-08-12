/*-------------------------------------------------------------------------
 *
 * pg_jsonbc_dict.h
 *	definition of dictionaries for tsearch
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_jsonbc_dict.h
 *
 * NOTES
 *		the genbki.pl script reads this file and generates .bki
 *		information from the DATA() statements.
 *
 *		XXX do NOT break up DATA() statements into multiple lines!
 *			the scripts are not as smart as you might think...
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_JSONBC_DICT_H
#define PG_JSONBC_DICT_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_jsonbc_dict definition.  cpp turns this into
 *		typedef struct FormData_pg_jsonbc_dict
 * ----------------
 */
#define JsonbcDictionaryRelationId	3418

CATALOG(pg_jsonbc_dict,3418) BKI_WITHOUT_OIDS
{
	Oid		dict	BKI_FORCE_NOT_NULL;		/* dictionary sequence id */
	int32	id		BKI_FORCE_NOT_NULL;		/* name id */
	text	name	BKI_FORCE_NOT_NULL;
} FormData_pg_jsonbc_dict;

typedef FormData_pg_jsonbc_dict *Form_pg_jsonbc_dict;

/* ----------------
 *		compiler constants for pg_jsonbc_dict
 * ----------------
 */
#define Natts_pg_jsonbc_dict			3
#define Anum_pg_jsonbc_dict_dict		1
#define Anum_pg_jsonbc_dict_id			2
#define Anum_pg_jsonbc_dict_name		3

#endif /* PG_JSONBC_DICT_H */
