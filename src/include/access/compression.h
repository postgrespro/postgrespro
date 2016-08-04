/*-------------------------------------------------------------------------
 *
 * compression.h
 *	  API for Postgres compression methods.
 *
 * Copyright (c) 2015-2016, PostgreSQL Global Development Group
 *
 * src/include/access/compression.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMPRESSION_H
#define COMPRESSION_H

#include "postgres.h"
#include "catalog/pg_attribute.h"
#include "nodes/nodes.h"

/*
 * API struct for an compression method.
 * Note this must be stored in a single palloc'd chunk of memory.
 */
typedef struct CompressionMethodRoutine
{
	NodeTag		type;
	void	  (*addAttr)(Form_pg_attribute attr);
	void	  (*dropAttr)(Form_pg_attribute attr);
	Datum	  (*compress)(Datum value, void *options);
	Datum	  (*decompress)(Datum value, void *options);
} CompressionMethodRoutine;

extern CompressionMethodRoutine *GetCompressionMethodRoutine(Oid cmhandler);
extern CompressionMethodRoutine *GetCompressionMethodRoutineByCmId(Oid cmoid);

#endif /* COMPRESSION_H */
