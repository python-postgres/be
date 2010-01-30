SET standard_conforming_strings TO ON;
SET client_encoding TO 'UTF-8';

CREATE OR REPLACE FUNCTION committed_nothing() RETURNS VOID LANGUAGE python AS
$python$
def main():
	with xact():
		pass
$python$;
SELECT committed_nothing();

CREATE OR REPLACE FUNCTION aborted_nothing() RETURNS VOID LANGUAGE python AS
$python$
def main():
	try:
		with xact():
			raise ValueError
	except ValueError:
		pass
$python$;
SELECT aborted_nothing();

CREATE OR REPLACE FUNCTION cant_reuse() RETURNS VOID LANGUAGE python AS
$python$
x = xact()

def main():
	with x:
		pass
$python$;
SELECT cant_reuse();
SELECT cant_reuse();


DROP TABLE IF EXISTS pyxact_test;
CREATE TABLE pyxact_test (i int);

-- Move everything through the SQL function to avoid
-- a dependency on Postgres.Statement [That will be checked later]
CREATE OR REPLACE FUNCTION insert_val(int) RETURNS VOID LANGUAGE SQL AS
$$
INSERT INTO pyxact_test VALUES ($1);
$$;

CREATE OR REPLACE FUNCTION delete_val(int) RETURNS VOID LANGUAGE SQL AS
$$
DELETE FROM pyxact_test WHERE i = $1;
$$;

CREATE OR REPLACE FUNCTION check_commit() RETURNS VOID LANGUAGE python AS
$python$
from Postgres import CONST, Function, Type

rp = Type(CONST['REGPROCEDUREOID'])
ins = Function(rp('insert_val(int)'))

def main():
	with xact():
		ins(123)
$python$;
SELECT check_commit();
SELECT i, i = 123 AS should_be_true FROM pyxact_test;

CREATE OR REPLACE FUNCTION check_delete() RETURNS VOID LANGUAGE python AS
$python$
from Postgres import CONST, Function, Type

rp = Type(CONST['REGPROCEDUREOID'])
delv = Function(rp('delete_val(int)'))

def main():
	with xact():
		delv(123)
$python$;
SELECT check_delete();
SELECT COUNT(i) AS should_be_zero FROM pyxact_test;

CREATE OR REPLACE FUNCTION check_abort() RETURNS VOID LANGUAGE python AS
$python$
from Postgres import CONST, Function, Type

rp = Type(CONST['REGPROCEDUREOID'])
ins = Function(rp('insert_val(int)'))

def main():
	try:
		with xact():
			ins(321)
			raise ValueError()
	except ValueError:
		pass
$python$;
SELECT check_abort();
SELECT COUNT(i) AS should_be_zero FROM pyxact_test;


CREATE OR REPLACE FUNCTION check_abort_from_error() RETURNS VOID LANGUAGE python AS
$python$
from Postgres import CONST, Function, Type

rp = Type(CONST['REGPROCEDUREOID'])
ins = Function(rp('insert_val(int)'))

def main():
	try:
		with xact():
			ins(1013)
			rp('foobaZZZzzz(int17)')
	except:
		pass
	else:
		raise RuntimeError("bad regprocedure did not raise exception")
$python$;
SELECT check_abort_from_error();
SELECT COUNT(i) AS should_be_zero FROM pyxact_test;


CREATE OR REPLACE FUNCTION entered_xact() RETURNS VOID LANGUAGE python AS
$python$
def main():
	x = xact()
	x.__enter__()
$python$;
SELECT entered_xact();

CREATE OR REPLACE FUNCTION entered_exc_xact() RETURNS VOID LANGUAGE python AS
$python$
def main():
	x = xact()
	x.__enter__()
	raise ValueError("xact WARNING should get thrown")
$python$;
SELECT entered_exc_xact();

CREATE OR REPLACE FUNCTION exited_xact() RETURNS VOID LANGUAGE python AS
$python$
def main():
	x = xact()
	x.__exit__(None, None, None)
$python$;
SELECT exited_xact();

CREATE OR REPLACE FUNCTION out_of_order() RETURNS VOID LANGUAGE python AS
$python$
def main():
	x1 = xact()
	x2 = xact()
	x1.__enter__()
	x2.__enter__()
	x1.__exit__(None, None, None)
$python$;
SELECT out_of_order();

-- Finally, test the pl_ist_count global's recursion safety(handler called by handler).
CREATE OR REPLACE FUNCTION xsubsub() RETURNS VOID LANGUAGE python AS
$python$
def main():
	x = xact()
	x.__enter__()
	raise ValueError("eek")
	# Should cause an error, but trap in outer and recover accordingly.
$python$;

CREATE OR REPLACE FUNCTION call_broken_but_recover() RETURNS VOID LANGUAGE python AS
$python$
from Postgres import CONST, Function, Type, Exception as pg_exc

rp = Type(CONST['REGPROCEDUREOID'])
ins = Function(rp('insert_val(int)'))
s = Function(rp('entered_exc_xact()'))

def main():
	with xact():
		ins(8080)
		try:
			with xact():
				s()
		except pg_exc:
			pass
$python$;

SELECT call_broken_but_recover();
SELECT i FROM pyxact_test WHERE i = 8080;

-- Same as above, but do the insert in a block (might be a useless test..)
CREATE OR REPLACE FUNCTION call_broken_but_recover_noxact() RETURNS VOID LANGUAGE python AS
$python$
from Postgres import CONST, Function, Type, Exception as pg_exc

rp = Type(CONST['REGPROCEDUREOID'])
s = Function(rp('entered_exc_xact()'))

def main():
	try:
		with xact():
			s()
	except pg_exc:
		pass
$python$;

BEGIN;
SELECT call_broken_but_recover_noxact();
INSERT INTO pyxact_test VALUES (2020);
COMMIT;
SELECT i FROM pyxact_test WHERE i = 2020;
