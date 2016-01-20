EXTENSION = hunspell_tr_tr
DATA = hunspell_tr_tr--1.0.sql

DATA_TSEARCH = tr_tr.affix tr_tr.dict

REGRESS = hunspell_tr_tr

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hunspell_tr_tr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
