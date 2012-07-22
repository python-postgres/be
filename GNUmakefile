all: src/project.py.cfrag src/module.py.cfrag $(MODULE_big)

# built by configure by build/tools/config.py
include build/cache/python.mk

EXTENSION := python
MODULE_big := python
DATA := python--$(project_version).sql

OBJS = src/extension.o \
src/pl.o src/do.o src/stateful.o \
src/python.o src/postgres.o \
src/module.o \
src/errordata.o src/triggerdata.o \
src/ist.o src/xact.o \
src/error.o \
src/tupledesc.o src/function.o src/statement.o src/cursor.o \
src/type/type.o \
src/type/object.o \
src/type/pseudo.o \
src/type/record.o \
src/type/array.o \
src/type/system.o \
src/type/string.o \
src/type/numeric.o \
src/type/timewise.o \
src/type/bitwise.o

REGRESS = init io srf function trigger xact error domain enum ifmod array composite spi polymorphic materialize_cursor lo bytea pytypes timewise stateful type typmod do preload tupledesc corners environment oid cache

# PGXS built by the configure script
include build/cache/postgres.mk

# This is a developer option that enables some unexpected elog()'s
ifeq ($(PLPY_STRANGE_THINGS), 1)
override CPPFLAGS := -DPLPY_STRANGE_THINGS $(CPPFLAGS)
endif

override CPPFLAGS := -g $(python_cflags) $(CPPFLAGS) -I./src/include
override SHLIB_LINK := $(python_ldflags) $(CPPFLAGS) $(SHLIB_LINK)

# Convert the characters in the file into a comma separated list of
# ASCII character codes. (See src/module.c for where it's included)
src/module.py.cfrag: src/module.py
	$(python) build/tools/mkdigits.py <$? >$@

src/project.py.cfrag: src/project.py
	$(python) build/tools/mkdigits.py <$? >$@
