#include "postgres.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "nodes/pg_list.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"


#define ALL NIL
#define MAX_PARTITIONS 1024

#define OP_STRATEGY_LT 1
#define OP_STRATEGY_LE 2
#define OP_STRATEGY_EQ 3
#define OP_STRATEGY_GE 4
#define OP_STRATEGY_GT 5

#define BLOCKS_COUNT 10240

/*
 * Partitioning type
 */
typedef enum PartType
{
	PT_HASH = 1,
	PT_RANGE
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
 * Dynamic shared memory array
 */
typedef struct DsmArray
{
	dsm_handle	segment;
	size_t		offset;
	size_t		length;
} DsmArray;

typedef struct Block
{
	dsm_handle	segment;
	size_t		offset;
	bool		is_free;
} Block;

typedef struct Table
{
	dsm_handle	segment;
	Block	blocks[BLOCKS_COUNT];
	size_t	block_size;
	size_t	first_free;
} Table;


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
	// Oid			children[MAX_PARTITIONS];
	DsmArray    children;
	int			children_count;
	PartType	parttype;
	Index		attnum;
	Oid			atttype;

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
	// RangeEntry	ranges[64];
	DsmArray    ranges;
} RangeRelation;


typedef int IndexRange;
#define RANGE_INFINITY 0xFFFF

#define make_range(min, max) \
	((min) << 16 | ((max) & 0x0000FFFF))

#define range_min(range) \
	((range) >> 16)

#define range_max(range) \
	((range) & 0x0000FFFF)

// Range make_range(int min, int max);
// int range_min(Range range);
// int range_max(Range range);
List *append_range(List *a_lst, IndexRange b);
List *intersect_ranges(List *a, List *b);
List *unite_ranges(List *a, List *b);

LWLock *load_config_lock;
LWLock *dsm_init_lock;


// Table *init_dsm_table(size_t block_size);
// DsmArray *alloc_dsm_array(size_t entry_size, size_t length);
// void free_dsm_array(DsmArray *array);
// void *get_dsm_array(const ArrayPtr* ptr);

void alloc_dsm_table();
void create_dsm_segment(size_t block_size);
void init_dsm_table(Table *tbl, dsm_handle h, size_t block_size);
void alloc_dsm_array(DsmArray *arr, size_t entry_size, size_t length);
void free_dsm_array(DsmArray *arr);
void *dsm_array_get_pointer(const DsmArray* arr);


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
void remove_relation_info(Oid relid);
