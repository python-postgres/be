-- make sure subsequent calls actually are from the cache.
-- the next check is not complete without this.
CREATE OR REPLACE FUNCTION check_for_func_persists() RETURNS text LANGUAGE python AS
$python$
import Postgres

if not hasattr(Postgres, 'CACHED'):
	Postgres.CACHED = __func__
	Postgres.CACHEDD = globals()

def main():
	assert Postgres.CACHED is __func__
	assert Postgres.CACHEDD is globals()
	return 'success'
$python$;
SELECT check_for_func_persists();
SELECT check_for_func_persists();


-- can't use proc() for the func cache check as the function cache
-- is sys.modules, which proc()/Postgres.Function doesn't use.
CREATE OR REPLACE FUNCTION check_for_func_cache_clear() RETURNS text LANGUAGE python AS
$python$
import Postgres
foo = 'init'
def main():
	global foo
	assert foo == 'init'
	foo = 'override'
	Postgres.clearcache()
	# it did change, but the next time this
	# function is called, it will be a new module.
	assert globals()['foo'] == 'override'
	return 'success'
$python$;
SELECT check_for_func_cache_clear();
-- the first one is the setup
SELECT check_for_func_cache_clear();

DROP TYPE IF EXISTS cached_or_not;
CREATE TYPE cached_or_not AS (i int, t text);
CREATE OR REPLACE FUNCTION check_for_type_cache() RETURNS text LANGUAGE python AS
$python$
from Postgres.types import regtype
import Postgres

oid = regtype('cached_or_not')
T = Postgres.Type(oid)

def main():
	cur = Postgres.Type(oid)
	assert cur is T
	Postgres.clearcache()
	newtyp = Postgres.Type(oid)
	assert cur is not newtyp
	return 'success'
$python$;
SELECT check_for_type_cache();
