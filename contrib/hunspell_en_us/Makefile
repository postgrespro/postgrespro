EXTENSION = hunspell_en_us
DATA = sql/hunspell_en_us--1.0.sql

DATA_TSEARCH = dict/en_us.affix dict/en_us.dict

REGRESS = hunspell_en_us

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hunspell_en_us
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
