# contrib/lz/Makefile

MODULE_big = lz
OBJS = lz.o $(WIN32RES)

EXTENSION = lz
DATA = lz--1.0.sql
PGFILEDESC = "LZ compression methods"

REGRESS = lz
SHLIB_LINK += -lzstd -llz4 -lsnappy

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/lz
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
