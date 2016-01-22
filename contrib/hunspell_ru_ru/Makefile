EXTENSION = hunspell_ru_ru
DATA = sql/hunspell_ru_ru--1.0.sql

DATA_TSEARCH = dict/ru_ru.affix dict/ru_ru.dict

REGRESS = hunspell_ru_ru

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hunspell_ru_ru
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
