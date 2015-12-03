#include "postgres.h"
#include "utils/date.h"
#include "utils/hsearch.h"


#define ALL NIL
#define MAX_PARTITIONS 1024

#define OP_STRATEGY_LT 1
#define OP_STRATEGY_LE 2
#define OP_STRATEGY_EQ 3
#define OP_STRATEGY_GE 4
#define OP_STRATEGY_GT 5

/*
 * Partitioning type
 */
typedef enum PartType
{
	PT_HASH = 1,
	PT_RANGE,
	PT_LIST
} PartType;

/*
 * Attribute type
 */
typedef enum AttType
{
	AT_INT = 1,
	AT_DATE,
} AttType;

typedef union rng_type
{
	int		integer;
	DateADT	date;
} rng_type;

/*
 * PartRelationInfo
 *		Per-relation partitioning information
 *
 *		oid - parent table oid
 *		children - list of children oids
 *		parttype - partitioning type (HASH, LIST or RANGE)
 *		attnum - attribute number of parent relation
 */
typedef struct PartRelationInfo
{
	Oid			oid;
	// List	   *children;
	/* TODO: is there any better solution to store children in shared memory? */
	Oid			children[MAX_PARTITIONS];
	int			children_count;
	PartType	parttype;
	Index		attnum;
	AttType		atttype;

} PartRelationInfo;

/*
 * Child relation for HASH partitioning
 */
typedef struct HashRelationKey
{
	int		hash;
	Oid		parent_oid;
} HashRelationKey;

typedef struct HashRelation
{
	HashRelationKey key;
	Oid		child_oid;
} HashRelation;

/*
 * Child relation for RANGE partitioning
 */
// typedef struct RangeEntry
// {
// 	Oid			child_oid;
// 	rng_type	min;
// 	rng_type	max;
// } RangeEntry;

typedef struct RangeEntry
{
	Oid		child_oid;
	Datum	min;
	Datum	max;
} RangeEntry;

typedef struct RangeRelation
{
	Oid			parent_oid;
	int			nranges;
	RangeEntry	ranges[64];
} RangeRelation;

HTAB *relations;
HTAB *hash_restrictions;
HTAB *range_restrictions;
bool initialization_needed;

/* initialization functions */
void init(void);
void create_part_relations_hashtable(void);
void create_hash_restrictions_hashtable(void);
void create_range_restrictions_hashtable(void);
void load_part_relations_hashtable(void);
void load_hash_restrictions(Oid relid);
void load_range_restrictions(Oid relid);
