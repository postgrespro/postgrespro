#ifndef PTRACK_H
#define PTRACK_H

#include "access/xlogdefs.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"

extern unsigned int blocks_track_count;

extern bool ptrack_enable;

extern void ptrack_save(void);

extern void 
ptrack_add_block(BlockNumber block_number, RelFileNode rel);

extern void ptrack_clear(void);

#endif   /* VISIBILITYMAP_H */
