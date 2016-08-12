/*-------------------------------------------------------------------------
 *
 * pg_compression.h
 *	  definition of the system "compression method" relation (pg_compression)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_compression.h
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
#ifndef PG_COMPRESSION_H
#define PG_COMPRESSION_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_compression definition.  cpp turns this into
 *		typedef struct FormData_pg_compression
 * ----------------
 */
#define CompressionMethodRelationId	3408

CATALOG(pg_compression,3408)
{
	NameData	cmname;			/* compression method name */
	regproc		cmhandler;		/* handler function */
	char		cmtype;			/* see CMTYPE_xxx constants below */
} FormData_pg_compression;

/* ----------------
 *		Form_pg_compression corresponds to a pointer to a tuple with
 *		the format of pg_compression relation.
 * ----------------
 */
typedef FormData_pg_compression *Form_pg_compression;

/* ----------------
 *		compiler constants for pg_compression
 * ----------------
 */
#define Natts_pg_compression			3
#define Anum_pg_compression_cmname		1
#define Anum_pg_compression_cmhandler	2
#define Anum_pg_compression_cmtype		3

/* ----------------
 *		initial contents of pg_compression
 * ----------------
 */

DATA(insert OID = 3451 (  json_null		json_null_cm_handler	0 ));
DESCR("json null compression method");
#define JSON_NULL_CM_OID 3451
DATA(insert OID = 3452 (  jsonb_null	jsonb_null_cm_handler	0 ));
DESCR("jsonb null compression method");
#define JSONB_NULL_CM_OID 3452
DATA(insert OID = 3403 (  jsonb			jsonb_handler			0 ));
DESCR("jsonb compression method");
#define JSONB_CM_OID 3403
DATA(insert OID = 3417 (  jsonbc		jsonbc_handler			0 ));
DESCR("jsonbc compression method");
#define JSONBC_CM_OID 3417

#endif   /* PG_COMPRESSION_H */
