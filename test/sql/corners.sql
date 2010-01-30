--
-- Test various corner cases.
--

--- COERCION ---

-- This attempts to validate that _handler is properly handling relayed exceptions
CREATE OR REPLACE FUNCTION check_failed_srf_coercion() RETURNS setof int LANGUAGE python AS
$python$
def main():
	return ((),(),())
$python$;

SELECT check_failed_srf_coercion();
SELECT * FROM check_failed_srf_coercion();


-- This will trigger a failure from int's typinput [due to str()]
CREATE OR REPLACE FUNCTION check_failed_srf_coercion2() RETURNS setof int LANGUAGE python AS
$python$
def main():
	return "foobar"
$python$;

SELECT check_failed_srf_coercion2();
SELECT * FROM check_failed_srf_coercion2();



--- EXCEPTIONS ---

-- The 'selekt 2' exception is the one represented on exit.
-- However, the 'selekt 1' exception *must* be printed in the context.
CREATE OR REPLACE FUNCTION check_proper_pg_exc_printing() RETURNS setof int LANGUAGE python AS
$python$
def main():
	try:
		with xact():
			prepare('selekt 1')
	except:
		prepare('selekt 2')
$python$;

SELECT check_proper_pg_exc_printing();

--- numeric boundaries ---

CREATE OR REPLACE FUNCTION check_numeric_edges() RETURNS text LANGUAGE python AS
$python$
import Postgres

int2 = Postgres.types.int2
int4 = Postgres.types.int4
int8 = Postgres.types.int8

int2_min = -(2**15)
int2_max = (2**15)-1

int4_min = -(2**31)
int4_max = (2**31)-1

int8_min = -(2**63)
int8_max = (2**63)-1

def main():
	assert int(int2(int2_min)) == int2_min
	assert int(int2(int2_max)) == int2_max

	assert int(int4(int4_min)) == int4_min
	assert int(int4(int4_max)) == int4_max

	assert int(int8(int8_min)) == int8_min
	assert int(int8(int8_max)) == int8_max
	return 'success'
$python$;

SELECT check_numeric_edges();


CREATE OR REPLACE FUNCTION check_numeric_outside_edges() RETURNS text LANGUAGE python AS
$python$
import Postgres

int2 = Postgres.types.int2
int4 = Postgres.types.int4
int8 = Postgres.types.int8

int2_min = -(2**15)
int2_max = (2**15)-1

int4_min = -(2**31)
int4_max = (2**31)-1

int8_min = -(2**63)
int8_max = (2**63)-1

trials = [
	(int2, int2_min - 1, int2_max + 1),
	(int4, int4_min - 1, int4_max + 1),
	(int8, int8_min - 1, int8_max + 1)
]

def main():
	for x in trials:
		typ, min, max = x
		try:
			with xact():
				typ(min)
				raise RuntimeError("%s failed to throw exception for (min) %d" %(typ.typname, min))
		except (Postgres.Exception, OverflowError):
			pass
		try:
			with xact():
				typ(max)
				raise RuntimeError("%s failed to throw exception for (max) %d" %(typ.typname, max))
		except (Postgres.Exception, OverflowError):
			pass
	return 'success'
$python$;

SELECT check_numeric_outside_edges();

-- CURSORS --

CREATE OR REPLACE FUNCTION whence_overflow() RETURNS text LANGUAGE python AS
$python$
def main():
	ps = prepare('select 1')
	c = ps.declare()
	try:
		# might need to increase this number on some platforms? =)
		c.seek(0, 2**128 + 1)
	except OverflowError:
		pass
	return 'success'
$python$;

SELECT whence_overflow();


-- SRFs --

-- validate key errors
CREATE OR REPLACE FUNCTION cause_key_error_srf(OUT i int, OUT t text) RETURNS SETOF record LANGUAGE python AS
$python$
def main():
	return (
		{'f': 'NOOOOOO'},
	)
$python$;

SELECT cause_key_error_srf();
SELECT * FROM cause_key_error_srf();

--- between transaction deallocation ---

CREATE OR REPLACE FUNCTION check_between_xact_deallocs() RETURNS text LANGUAGE python AS
$python$
# we want the state to be cleared when
# transaction scope is dumped.
from Postgres import WARNING, Stateful

@Stateful
def main():
	p = prepare('select 1')
	c = p.rows()
	yield 'setup refs'
$python$;

-- can't validate deallocation without weakref support
-- I manually validated that dealloc (via elog) was getting called -jwp@2009
SELECT check_between_xact_deallocs();
SELECT check_between_xact_deallocs();


--- Direct Function Calls ---

-- This varchar_cast will return it's *exact* argument Datum.
-- This is validating that Postgres.Function is operating properly in these conditions.
CREATE OR REPLACE FUNCTION check_dont_free_params() RETURNS text LANGUAGE python AS
$python$
import Postgres
import gc

# yeah, hardcoded Oid is lame. Fix if you like.
varchar_cast = Postgres.Function(669)

def main():
	param = Postgres.types.varchar("foo")
	r = varchar_cast(param, -1, False)
	assert r is not param
	del r
	del param
	# just to be sure...
	gc.collect()
	return 'success'
$python$;

SELECT check_dont_free_params();


--- Array Type Invalidation ---

DROP TABLE IF EXISTS array_element_t;
CREATE TABLE array_element_t (i int, t text);

CREATE OR REPLACE FUNCTION check_new_array_type() RETURNS text LANGUAGE python AS
$python$
import Postgres
from Postgres.types import regtype

array_element_t = Postgres.Type(regtype('array_element_t'))
array = array_element_t.Array

def main():
	assert Postgres.Type(array.oid) is array
	sqlexec('ALTER TABLE array_element_t ADD COLUMN x float')
	new_array = Postgres.Type(array.oid)
	# IsCurrent should identify that the element type has changed
	# and cause a new type to be created and used.
	assert new_array is not array
	assert new_array.Element is not array_element_t
	return 'success'
$python$;

SELECT check_new_array_type();


--- Held Cursors ---
--
-- This case is testing how function refresh works
-- in the held cursor context. Functions will be refreshed when
-- the xid has changed. This *has* to happen as the transaction scope
-- has been reset, and the objects in fn_info whose references were held
-- by the transaction scope are no longer safe to use.
--
-- XXX: ATM, the cursor is being fully materialized, so as-is this
-- doesn't test anything. However, leave the test here for documentation
-- purposes and the ability to note when a change happens. If someone
-- knows how to force PG to not materialize the result set, this test
-- would likely benefit from that..
--
CREATE OR REPLACE FUNCTION replace_while_held(i int) RETURNS text VOLATILE LANGUAGE python AS
$python$
def main(i):
	return 'foo'
$python$;

DECLARE held SCROLL CURSOR WITH HOLD FOR SELECT replace_while_held(i) FROM generate_series(1, 11) AS g(i) WHERE random() >= 0;

FETCH 5 IN held;

CREATE OR REPLACE FUNCTION replace_while_held(i int) RETURNS text VOLATILE LANGUAGE python AS
$python$
def main(i):
	return 'bar'
$python$;

-- show that it's bar.
SELECT replace_while_held(1);

-- things are "fine", it's still foo. (because it was already materialized)
FETCH ALL IN held;
CLOSE held;
