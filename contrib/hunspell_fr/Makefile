EXTENSION = hunspell_fr
DATA = sql/hunspell_fr--1.0.sql

DATA_TSEARCH = dict/fr.affix dict/fr.dict

REGRESS = hunspell_fr

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hunspell_fr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
