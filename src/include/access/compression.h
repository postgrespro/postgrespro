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
#include "nodes/pg_list.h"

typedef const void *CompressionOptions;

typedef struct CompressionMethodOptionsRoutines
{
	List 			   *(*validate)(Form_pg_attribute, List *);
	CompressionOptions	(*convert)(Form_pg_attribute, List *);
	void				(*free)(CompressionOptions options);
	CompressionOptions	(*copy)(CompressionOptions options);
	bool				(*equal)(CompressionOptions o1, CompressionOptions o2);
} CompressionMethodOptionsRoutines;

typedef Datum (*CompressionRoutine)  (Datum value, CompressionOptions options);
typedef Datum (*DecompressionRoutine)(Datum value, CompressionOptions options);

/* CompressionMethodRoutine.flags */
#define CM_EXTENDED_REPRESENTATION	0x01

/*
 * API struct for an compression method.
 * Note this must be stored in a single palloc'd chunk of memory.
 */
typedef struct CompressionMethodRoutine
{
	NodeTag		type;
	int			flags;
	CompressionMethodOptionsRoutines *options;
	void	  (*addAttr)(Form_pg_attribute attr, List *options);
	void	  (*dropAttr)(Form_pg_attribute attr, List *options);
	CompressionRoutine		compress;
	DecompressionRoutine	decompress;
} CompressionMethodRoutine;

typedef enum { CMOP_GET_ROUTINE } CompressionMethodOp;

typedef struct CompressionMethodOpArgs
{
	CompressionMethodOp op;
	union
	{
		struct
		{
			Oid	typeid;
		} getRoutine;
	} args;
} CompressionMethodOpArgs;

extern CompressionMethodRoutine *GetCompressionMethodRoutine(Oid cmhandler,
															 Oid typeid);
extern CompressionMethodRoutine *GetCompressionMethodRoutineByCmId(Oid cmoid,
																   Oid typeid);

#endif /* COMPRESSION_H */
