# contrib/bson/Makefile

MODULE_big = bson
OBJS = bson.o $(WIN32RES)

EXTENSION = bson
DATA = bson--1.0.sql
PGFILEDESC = "BSON compression method for json/jsonb types"

REGRESS = bson

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/bson
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
