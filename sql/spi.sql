-- This is, sadly, a grab bag of SPI tests.
--

CREATE OR REPLACE FUNCTION prepare_in_module() RETURNS text LANGUAGE python AS
$python$
import Postgres
assert prepare is Postgres.Statement

ps = prepare("SELECT 1").string

def main():
	return ps
$python$;
SELECT prepare_in_module();

CREATE OR REPLACE FUNCTION prepare_and_forget(sql text) RETURNS VOID LANGUAGE python AS
$python$

def main(sql):
	prepare(sql)
$python$;

SELECT prepare_and_forget('SELECT 1');
SELECT prepare_and_forget('SELECT 1;');
SELECT prepare_and_forget('CREATE TABLE foo (i int)');
SELECT prepare_and_forget('SELECT $1::int');
-- Now some failures
SELECT prepare_and_forget('SELECT 1; SELECT 2;');

SELECT prepare_and_forget('INSERT INTO foo_does_not_exist VALUES ()');
SELECT prepare_and_forget('INSERT INTO foo_does_not_exist VALUES ($1)');
SELECT prepare_and_forget('INSERT INTO foo_does_not_exist VALUES ($1, $2)');

-- restricted commands
SELECT prepare_and_forget('BEGIN;');
SELECT prepare_and_forget('COMMIT;');
SELECT prepare_and_forget('ABORT;');
SELECT prepare_and_forget('SAVEPOINT foo;');
SELECT prepare_and_forget('ROLLBACK TO foo;');
SELECT prepare_and_forget('RELEASE foo;');

SELECT prepare_and_forget('COPY pg_type FROM STDIN');

-- Look at output.
CREATE OR REPLACE FUNCTION prepare_and_str(sql text) RETURNS text LANGUAGE python AS
$python$
def main(sql):
	return str([tuple(map(str, x)) for x in prepare(sql).rows()])
$python$;

SELECT prepare_and_str('SELECT 1');
SELECT prepare_and_str('SELECT 1 UNION ALL SELECT 2');

-- Basic I/O statement and coercion from text to target type.
CREATE OR REPLACE FUNCTION args_prepare_and_str(sql text, args text) RETURNS text LANGUAGE python AS
$python$
def main(sql, args):
	params = str(args).split(':')
	if not params:
		params = []
	ps = prepare(sql)
	return str([tuple(map(str, x)) for x in ps.rows(*params)])
$python$;

SELECT args_prepare_and_str('SELECT $1::text, $2::text', 'foo:bar');
SELECT args_prepare_and_str('SELECT $1::text, $2::text, $3::int', 'foo:bar:11');
SELECT args_prepare_and_str('SELECT $1::text, $2::text, $3::int, $4::bigint', 'foo:bar:11:123213424322');
SELECT args_prepare_and_str('SELECT $1::text, $2::text, $3::int, $4::bigint', 'foo:bar:11:FAIL');

-- SPI works with xact()? failure caused by bad statement.
CREATE OR REPLACE FUNCTION spi_trap_failure() RETURNS text LANGUAGE python AS
$python$
import Postgres

def main():
	try:
		with xact():
			ps = prepare('selekt 1')
	except:
		pass
	else:
		raise ValueError("failed")
	# double check that it's not in error.
	prepare("SELECT 1")
	return "xact rolled back, and exception caught from prepare() failure"
$python$;

-- Check for appropriate teardown with repeats
SELECT spi_trap_failure();
SELECT spi_trap_failure();
BEGIN;
SELECT spi_trap_failure();
SELECT spi_trap_failure();
ABORT;


-- SPI works with xact()? failure caused at execution time.
--
CREATE OR REPLACE FUNCTION spi_raise_exc() RETURNS int LANGUAGE python AS
$python$
def main():
	raise ValueError("fail pls")
$python$;

CREATE OR REPLACE FUNCTION cursor_spi_trap_failure() RETURNS text LANGUAGE python AS
$python$
def main():
	try:
		with xact():
			ps = prepare('select spi_raise_exc()')
			ps()
	except:
		pass
	else:
		raise ValueError("failed")
	# double check that it's not in error.
	prepare("SELECT 1")
	return "xact rolled back, and exception caught from execution"
$python$;

SELECT cursor_spi_trap_failure();

-- Test a merge case
DROP TABLE IF EXISTS kvpair;
CREATE TABLE kvpair (k int PRIMARY KEY, v text);
CREATE OR REPLACE FUNCTION merge_kv(k int, v text) RETURNS text LANGUAGE python AS
$python$

exists = prepare("SELECT TRUE FROM kvpair WHERE k = $1")
dels = prepare("DELETE FROM kvpair WHERE k = $1")
ins = prepare("INSERT INTO kvpair (k, v) VALUES ($1, $2)")
ups = prepare("UPDATE kvpair SET v = $2 WHERE k = $1")

def main(k, v):
	if exists(k):
		if v is None:
			r = dels(k)
			op = 'deleted'
		else:
			r = ups(k, v)
			op = 'updated'
	else:
		r = ins(k, v)
		op = 'inserted'
	return ':'.join((op, str(k), str(v), str(r)))
$python$;

SELECT merge_kv(1, 'one');
SELECT merge_kv(1, 'two');
SELECT merge_kv(1, 'five');
SELECT merge_kv(2, 'two');
SELECT merge_kv(4, 'f');
SELECT merge_kv(0, 'zero');
SELECT merge_kv(4, 'fours');
SELECT merge_kv(1, 'one');
SELECT merge_kv(1, NULL);

SELECT * FROM kvpair ORDER BY k ASC;


-- Cache invalidating? Recovery?
CREATE OR REPLACE FUNCTION return_count() RETURNS bigint LANGUAGE python AS
$python$

countit = prepare("SELECT count(*) FROM kvpair")

def main():
	return countit.first()
$python$;

SELECT return_count();
DROP TABLE kvpair;
SELECT return_count();
CREATE TABLE kvpair (k int PRIMARY KEY, v text);
SELECT return_count();


-- Hold a reference to a cursor in the module.
-- This function provides the basis for cursor lifetime tests.

CREATE OR REPLACE FUNCTION cached_cursor(dump_cur bool) RETURNS bigint LANGUAGE python AS
$python$
cur = prepare("SELECT 1::bigint").rows()

def main(dump_cursor):
	global cur
	if dump_cursor:
		cur = prepare("SELECT 1::bigint").rows()
		return 0
	r = next(cur)[0]
	try:
		next(cur)
	except StopIteration:
		pass
	else:
		raise ValueError("next() on exhausted cursor did not raise StopIteration")
	return r
$python$;

SELECT cached_cursor(false);
-- should fail with cursor is closed
SELECT cached_cursor(false);
BEGIN;
-- "reset" has to be inside the xact.
SELECT cached_cursor(true);

SELECT cached_cursor(false);

-- should fail with StopIteration
SELECT cached_cursor(false);
ABORT;

-- Rolled back savepoint; current this shouldn't work.
BEGIN;
SAVEPOINT a;
SELECT cached_cursor(true);
ROLLBACK TO a;
SELECT cached_cursor(false);
ABORT;

-- doesn't last across savepoints..
BEGIN;
SAVEPOINT a;
--- XXX: cursor not living beyond RELEASE'd savepoint?
SELECT cached_cursor(true);
RELEASE a;
SELECT cached_cursor(false);
ABORT;

-- wasn't started in the savepoint, so it should survive.
BEGIN;
SELECT cached_cursor(true);
SAVEPOINT a;
SELECT TRUE FROM kvpair LIMIT 1;
RELEASE a;
SELECT cached_cursor(false);
ABORT;


DROP SEQUENCE IF EXISTS kvpair_seq;
CREATE SEQUENCE kvpair_seq;
ALTER TABLE kvpair ALTER k SET DEFAULT nextval('kvpair_seq')::int4;


-- check DML ... RETURNING
CREATE OR REPLACE FUNCTION check_dml_ret(v text) RETURNS int LANGUAGE python AS
$python$
ins = prepare("INSERT INTO kvpair (v) VALUES ($1) RETURNING k")

def main(val):
	return ins.first(val)
$python$;

SELECT check_dml_ret('foo');
SELECT check_dml_ret('bar');
SELECT check_dml_ret('bar');
SELECT * FROM kvpair;

--
-- Postgres.Transaction functionality in conjunction with SPI.
-- In _xact.sql, the effects of subtransactions are tested using SQL functions.
-- Here, we perform many of those tests again to validate that SPI is
-- being used properly.
--
DELETE FROM kvpair;

CREATE OR REPLACE FUNCTION x_success_insert() RETURNS text LANGUAGE python AS
$python$
ins = prepare('INSERT INTO kvpair (v) VALUES ($1)')

def main():
	with xact():
		ins('SHOULD EXIST')
	return 'success'
$python$;

SELECT x_success_insert();
SELECT * FROM kvpair;

CREATE OR REPLACE FUNCTION x_fail_insert() RETURNS text LANGUAGE python AS
$python$
ins = prepare('INSERT INTO kvpair (v) VALUES ($1)')

def main():
	try:
		with xact():
			ins('SHOULD NOT EXIST')
			raise ValueError
	except ValueError:
		return 'caught valueerror'
	return 'this failed'
$python$;

SELECT x_fail_insert();
SELECT * FROM kvpair;

CREATE OR REPLACE FUNCTION x_failed_xact() RETURNS text LANGUAGE python AS
$python$
ins = prepare('INSERT INTO kvpair (k, v) VALUES ($1, $2)')
key = prepare('SELECT k FROM kvpair ORDER BY k LIMIT 1').first()

def main():
	try:
		with xact():
			ins(key, 'INSERT SHOULD FAIL')
	except:
		return 'success, caught dberror'
	return 'this failed'
$python$;

SELECT x_failed_xact();
SELECT * FROM kvpair;


CREATE OR REPLACE FUNCTION x_recover_xact() RETURNS text LANGUAGE python AS
$python$
ins = prepare('INSERT INTO kvpair (k, v) VALUES ($1, $2)')
key = prepare('SELECT k FROM kvpair ORDER BY k LIMIT 1').first()
dins = prepare('INSERT INTO kvpair (v) VALUES ($1)')

def main():
	try:
		with xact():
			ins(key, 'INSERT SHOULD FAIL')
	except:
		dins('THIS SHOULD ALSO EXIST')
		return 'success'
	return 'this failed'
$python$;

SELECT x_recover_xact();
SELECT * FROM kvpair;

CREATE OR REPLACE FUNCTION x_dont_recover_xact() RETURNS text LANGUAGE python AS
$python$
ins = prepare('INSERT INTO kvpair (k, v) VALUES ($1, $2)')
key = prepare('SELECT k FROM kvpair ORDER BY k LIMIT 1').first()
dins = prepare('INSERT INTO kvpair (v) VALUES ($1)')

def main():
	try:
		with xact():
			dins('THIS SHOULD **NOT** EXIST')
			ins(key, 'INSERT SHOULD FAIL')
	except:
		dins('this succeeds, but will fail when the following error is raised')
		raise
$python$;

SELECT x_dont_recover_xact();
-- No change expected.
SELECT * FROM kvpair;

--
-- Very basic load_rows & load_chunks tests.
--

CREATE OR REPLACE FUNCTION load_rows() RETURNS text LANGUAGE python AS
$python$
ins = prepare('INSERT INTO kvpair (v) VALUES ($1)')

def main():
	return ins.load_rows((str(x),) for x in range(200))
$python$;

SELECT load_rows();
SELECT count(*) FROM kvpair;


CREATE OR REPLACE FUNCTION load_chunks() RETURNS text LANGUAGE python AS
$python$
ins = prepare('INSERT INTO kvpair (v) VALUES ($1)')

def main():
	return ins.load_chunks([
		((str(x),) for x in range(25)),
		((str(x),) for x in range(25,50)),
		((str(x),) for x in range(50,75)),
		((str(x),) for x in range(75,100)),
	])
$python$;

SELECT load_chunks();
SELECT count(*) FROM kvpair;


-- make sure first() works with DML.
CREATE OR REPLACE FUNCTION dml_first() RETURNS int LANGUAGE python AS
$python$
ins = prepare('INSERT INTO kvpair (v) VALUES ($1)')

def main():
	return ins.first('does first work')
$python$;

-- expect 1
SELECT dml_first() = 1;


--
-- Test cursor configuration restrictions
--
-- Validate that NO SCROLL cursors cannot use read() and seek()
-- read() and seek() are SCROLL interfaces.
--
CREATE OR REPLACE FUNCTION rows_cur_cant_read() RETURNS VOID LANGUAGE python AS
$python$

def main():
	cur = prepare("SELECT 1").rows()
	cur.read(10)
$python$;
SELECT rows_cur_cant_read();

CREATE OR REPLACE FUNCTION rows_cur_cant_seek() RETURNS VOID LANGUAGE python AS
$python$

def main():
	cur = prepare("SELECT 1").rows()
	cur.seek(10)
$python$;
SELECT rows_cur_cant_seek();

CREATE OR REPLACE FUNCTION chunks_cur_cant_read() RETURNS VOID LANGUAGE python AS
$python$

def main():
	cur = prepare("SELECT 1").chunks()
	cur.read(10)
$python$;
SELECT chunks_cur_cant_read();

CREATE OR REPLACE FUNCTION chunks_cur_cant_seek() RETURNS VOID LANGUAGE python AS
$python$

def main():
	cur = prepare("SELECT 1").chunks()
	cur.seek(10)
$python$;
SELECT chunks_cur_cant_seek();


-- validate that column_names, parameter_types, pg_parameter_types, pg_column_types
-- provide the expected information.
CREATE OR REPLACE FUNCTION spi_metadata() RETURNS text LANGUAGE python AS
$python$
import Postgres

colnames = ('a_text_column', 'a_int4_column', 'a_bytea_column')
pg_types = (Postgres.CONST['TEXTOID'], Postgres.CONST['INT4OID'], Postgres.CONST['BYTEAOID'])
types = tuple(map(Postgres.Type, pg_types))

stmt = prepare('SELECT $1::text AS a_text_column, $2::int4 AS a_int4_column, $3::bytea AS a_bytea_column')

args = ('text', 0, b'bytea')

def main():
	cur = stmt.rows(*args)

	assert cur.column_names == colnames
	assert stmt.column_names == colnames

	assert cur.column_types == types
	assert stmt.column_types == types
	assert stmt.parameter_types == types

	assert stmt.pg_parameter_types == pg_types
	assert stmt.pg_column_types == pg_types
	assert cur.pg_column_types == pg_types

	return 'success'
$python$;

SELECT spi_metadata();

--
-- Generate a series of positive integers and validate that seek and read work
-- as expected for both possible directions.
CREATE OR REPLACE FUNCTION scrollable() RETURNS text LANGUAGE python AS
$python$

def testScroll(direction = True):
	imin = 0
	imax = 2**16
	if direction:
		ps = prepare("SELECT i FROM generate_series(0, (2^16)::int) AS g(i)")
	else:
		ps = prepare("SELECT i FROM generate_series((2^16)::int, 0, -1) AS g(i)")

	c = ps.declare()
	c.direction = direction
	if not direction:
		c.seek(0)

	assert [x for x, in c.read(10)] == list(range(10))
	assert [x for x, in c.read(10, 'BACKWARD')] == list(range(8, -1, -1))
	c.seek(0, 2)
	# try a non-text direction parameter
	assert [x for x, in c.read(10, False)] == list(range(imax, imax-10, -1))

	# move to end
	c.seek(0, 2)
	assert [x for x, in c.read(100, 'BACKWARD')] == list(range(imax, imax-100, -1))
	# move backwards, relative
	c.seek(-100, 1)
	assert [x for x, in c.read(100, 'BACKWARD')] == list(range(imax-200, imax-300, -1))

	# move abs, again
	c.seek(14000)
	assert [x for x, in c.read(100)] == list(range(14000, 14100))
	# move forwards, relative
	c.seek(100, 1)
	assert [x for x, in c.read(100)] == list(range(14200, 14300))
	# move abs, again
	c.seek(24000)
	assert [x for x, in c.read(200)] == list(range(24000, 24200))
	# move to end and then back some
	c.seek(20, 2)
	assert [x for x, in c.read(200, 'BACKWARD')] == list(range(imax-20, imax-20-200, -1))

	c.seek(0, 2)
	c.seek(-10, 1)
	r1 = c.read(10)
	c.seek(10, 2)
	assert [x for x, in r1] == [x for x, in c.read(10)]

def main():
	testScroll()
	testScroll(direction = False)
	return 'success'
$python$;	

SELECT scrollable();

-- check constant parameter use and their effect on the interfaces
CREATE OR REPLACE FUNCTION check_const_params() RETURNS text LANGUAGE python AS
$python$
ps = prepare("SELECT $1::int, $2::text, $3::numeric", 123, "text", 1.23)

meths = [
	ps,
	ps.rows,
	ps.chunks,
	ps.declare,
]

def main():
	r = [
		ps.first(),
		ps()[0],
		list(ps.rows())[0],
		list(ps.chunks())[0][0],
		ps.declare().read(1)[0],
	]
	for x in r:
		assert tuple(x) == (123, "text", "1.23")

	for x in meths:
		try:
			# params have already been specified.
			x(321, "wrong", "1.23")
		except TypeError:
			pass
	##
	# both load_rows and load_chunks should throw a TypeError
	try:
		ps.load_rows([(321,"wrong","3.12")])
	except TypeError:
		pass
	try:
		ps.load_chunks([[(321,"wrong","3.12")]])
	except TypeError:
		pass

	return 'success'
$python$;

SELECT check_const_params();

-- Check Postgres.eval()
CREATE OR REPLACE FUNCTION pg_eval(text) RETURNS text LANGUAGE python AS
$python$
from Postgres import eval as main
$python$;

SELECT pg_eval('''some text''::text');
SELECT pg_eval('1');
SELECT pg_eval('1::int4');
SELECT pg_eval('-123::int4');
SELECT pg_eval('-123::int4 + 1000');

--
-- Check read-only restriction. Make sure to exercise a few combinations..
-- Notably, calling DML is different then getting rows, so be sure to
-- exercise the cursor_open paths with INSERT ... RETURNING

-- first, make sure that the flag is properly present in the module execution context
CREATE OR REPLACE FUNCTION do_mod_in_mod_stable() RETURNS text STABLE LANGUAGE python AS
$python$
ps = prepare("insert into kvpair (v) VALUES ('fail')")
ps()
$python$;
SELECT do_mod_in_mod_stable();

CREATE OR REPLACE FUNCTION do_mod_in_mod_stable_rows() RETURNS text STABLE LANGUAGE python AS
$python$
ps = prepare("insert into kvpair (v) VALUES ('fail') RETURNING k, v")
r = list(ps.rows())
$python$;
SELECT do_mod_in_mod_stable_rows();

-- and finally, in main.
CREATE OR REPLACE FUNCTION do_mod_stable() RETURNS text STABLE LANGUAGE python AS
$python$
ps = prepare("insert into kvpair (v) VALUES ('fail')")
def main():
	ps()
	return 'fail'
$python$;
SELECT do_mod_stable();

CREATE OR REPLACE FUNCTION do_mod_stable_rows() RETURNS text STABLE LANGUAGE python AS
$python$
ps = prepare("insert into kvpair (v) VALUES ('fail') RETURNING k, v")
def main():
	r = list(ps.rows())
	return 'fail'
$python$;
SELECT do_mod_stable_rows();

-- Just check one IMMUTABLE as we really just want to make sure
-- that the contrast from VOLATILE is being made. We already
-- checked the combinations with STABLE, and those
-- code paths don't change due to IMMUTABLE.
CREATE OR REPLACE FUNCTION do_mod_immutable_rows() RETURNS text IMMUTABLE LANGUAGE python AS
$python$
ps = prepare("insert into kvpair (v) VALUES ('fail') RETURNING k, v")
def main():
	r = list(ps.rows())
	return 'fail'
$python$;
SELECT do_mod_immutable_rows();


-- chunksize tests; the only thing we can validate is that
-- chunks() yields the appropriate size
CREATE OR REPLACE FUNCTION check_chunksize() RETURNS text LANGUAGE python AS
$python$
ps = prepare('select i FROM generate_series(0, 2000) AS g(i)')
def main():
	c = ps.chunks()
	c.chunksize = 33
	assert len(next(c)) == 33
	c.chunksize = 51
	assert len(next(c)) == 51
	c.chunksize = 1
	assert len(next(c)) == 1
	c.chunksize = 5 
	assert len(next(c)) == 5
	assert len(next(c)) == 5
	assert len(next(c)) == 5
	return 'success(' + str(next(c)[0][0]) + ')'
$python$;

SELECT check_chunksize();

-- Postgres.execute --

-- Make sure that subsequent queries can reference objects created
-- by earlier statements(Postgres.execute)
CREATE OR REPLACE FUNCTION check_pg_execute() RETURNS text LANGUAGE python AS
$python$
import Postgres
def main():
	r = Postgres.execute("""
CREATE TEMP TABLE ttt (i int);
INSERT INTO ttt VALUES (1);
""")
	r = Postgres.execute("""
""")
	assert r == None
	assert sqleval('select i from ttt') == 1
	r = Postgres.execute("DROP TABLE ttt")
	assert r == None
	return 'success'
$python$;

SELECT check_pg_execute();

DROP TABLE IF EXISTS ttmp CASCADE;
CREATE TABLE ttmp (i int);
CREATE OR REPLACE FUNCTION insert_into_t(i int) RETURNS text LANGUAGE SQL AS
'INSERT INTO ttmp VALUES($1); SELECT ''foo''::text;';

CREATE OR REPLACE FUNCTION check_pg_execute_readall() RETURNS text LANGUAGE python AS
$python$
import Postgres

def main():
	r = Postgres.execute("""
SELECT insert_into_t(i) FROM generate_series(1, 100) g(i);
""")
	assert r == None
	return 'success'
$python$;

SELECT check_pg_execute_readall();
SELECT COUNT(*) FROM ttmp;

CREATE OR REPLACE FUNCTION check_recursive_execute() RETURNS text LANGUAGE python AS
$python$
import Postgres

sql = "SELECT check_recursive_execute()"
init = None
first = True

def main():
	global init, first
	if first is True:
		first = False
		r = Postgres.execute(sql)
	else:
		init = 'success'
	return str(init)
$python$;

SELECT check_recursive_execute();


-- Test empty first
CREATE OR REPLACE FUNCTION check_empty() RETURNS text LANGUAGE python AS
$python$
stmt = prepare('select 1 limit 0')
def main():
	assert stmt.first() is None
	assert stmt() == []
	return 'success'
$python$;
SELECT check_empty();


-- test column() cursors
CREATE OR REPLACE FUNCTION check_column() RETURNS SETOF text LANGUAGE python AS
$python$
stmt = prepare('SELECT i FROM generate_series(1, 10) AS g(i)').column

def main():
	yield 'begin'
	for x in stmt():
		yield str(x.__class__.typname) + ': ' + str(x)
	yield 'end'
$python$;

SELECT check_column();

-- make sure column-configured cursors are being properly
-- respected by the PL's materialization process.

-- the internal iterator buffer holds the individual columns, not records,
-- so the existing srf_cursor_materialize function won't be able to consume
-- buffer as it would a .rows() iterator. For now, it punts it to the generic
-- materialization mechanism.

CREATE OR REPLACE FUNCTION check_column_materialize() RETURNS SETOF text LANGUAGE python AS
$python$
stmt = prepare("SELECT i FROM (VALUES ('a'::text),('b'),('c'),('d')) AS g(i)")

def main():
	return stmt.column()
$python$;

SELECT check_column_materialize();
SELECT * FROM check_column_materialize();


CREATE OR REPLACE FUNCTION check_column_materialize_record(OUT text, OUT int) RETURNS SETOF record LANGUAGE python AS
$python$
stmt = prepare("SELECT i FROM (VALUES ('a'::text),('b'),('c'),('d')) AS g(i)")

def main():
	return stmt.column()
$python$;

-- fail.
SELECT check_column_materialize_record();
SELECT * FROM check_column_materialize_record();

-- check handling of VOID

CREATE OR REPLACE FUNCTION return_void() RETURNS VOID LANGUAGE python AS
'main = lambda: None';

CREATE OR REPLACE FUNCTION select_voids() RETURNS text LANGUAGE python AS
$python$
voids = list(map(tuple, prepare("""
	SELECT return_void(), return_void() UNION ALL
	SELECT return_void(), return_void()
""")()))

def main():
	assert voids == [(None,None), (None,None)]
	return 'success'
$python$;

SELECT select_voids();
