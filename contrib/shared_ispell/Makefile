# contrib/shared_ispell/Makefile

MODULE_big = shared_ispell
OBJS = src/shared_ispell.o

EXTENSION = shared_ispell
DATA = sql/shared_ispell--1.1.0.sql

REGRESS = shared_ispell

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/shared_ispell
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif