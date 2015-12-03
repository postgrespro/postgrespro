# contrib/pathman/Makefile

MODULE_big = pathman
OBJS = init.o pathman.o $(WIN32RES)

EXTENSION = pathman
EXTVERSION = 0.1
DATA = pathman--0.1.sql
PGFILEDESC = "pathman - partitioning tool"

REGRESS = pathman

$(EXTENSION)--$(EXTVERSION).sql: sql/init.sql sql/hash.sql sql/range.sql
	cat $^ > $@

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pathman
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif