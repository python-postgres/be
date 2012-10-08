SET standard_conforming_strings TO ON;
SET client_encoding TO 'UTF-8';

CREATE OR REPLACE FUNCTION pysyntaxerror() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

# missing ':'
def main()
	pass
$python$;
SELECT pysyntaxerror();


CREATE OR REPLACE FUNCTION py_load_failure() RETURNS VOID LANGUAGE python AS
$python$
# raise an exception while loading the module
raise RuntimeError("doa")

def main():
	pass
$python$;
SELECT py_load_failure();

CREATE OR REPLACE FUNCTION py_failure() RETURNS VOID LANGUAGE python AS
$python$

def main():
	# raise an exception on execution
	raise RuntimeError("doa")
$python$;
SELECT py_failure();

-- raises a pg error on load
CREATE OR REPLACE FUNCTION pg_load_failure() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

rp = Postgres.Type(Postgres.CONST['REGPROCEDUREOID'])
fun = rp('nosuchfunc(int17,zzz)')

def main():
	pass
$python$;
SELECT pg_load_failure();

-- raises a pg error on exec
CREATE OR REPLACE FUNCTION pg_failure() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

rp = Postgres.Type(Postgres.CONST['REGPROCEDUREOID'])

def main():
	fun = rp('nosuchfunc(int17,zzz)')
$python$;
SELECT pg_failure();

-- suffocates a pg error on load; should see PL complaint
CREATE OR REPLACE FUNCTION pg_load_failure_suf() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

rp = Postgres.Type(Postgres.CONST['REGPROCEDUREOID'])
try:
	fun = rp('nosuchfunc(int17,zzz)')
except:
	pass

def main():
	raise RuntimeError("should never see this")
$python$;
SELECT pg_load_failure_suf();

-- suffocates a pg error on exec; should see PL complaint
CREATE OR REPLACE FUNCTION pg_failure_suf() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

rp = Postgres.Type(Postgres.CONST['REGPROCEDUREOID'])

def main():
	try:
		fun = rp('nosuchfunc(int17,zzz)')
	except:
		pass
$python$;
SELECT pg_failure_suf();

-- suffocates a pg error, and leaves an open xact on load
CREATE OR REPLACE FUNCTION pg_x_load_failure_suf() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

rp = Postgres.Type(Postgres.CONST['REGPROCEDUREOID'])
x=xact()
x.__enter__()
try:
	fun = rp('nosuchfunc(int17,zzz)')
except:
	pass

def main():
	raise RuntimeError("should never see this")
$python$;
SELECT pg_x_load_failure_suf();

-- suffocates a pg error, and leaves an open xact 
CREATE OR REPLACE FUNCTION pg_x_failure_suf() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

rp = Postgres.Type(Postgres.CONST['REGPROCEDUREOID'])

def main():
	x=xact()
	x.__enter__()
	try:
		fun = rp('nosuchfunc(int17,zzz)')
	except:
		pass
$python$;
SELECT pg_x_failure_suf();

-- suffocates a pg error, and attempts to enter a protected area
CREATE OR REPLACE FUNCTION pg_failure_suf_IFTE() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

rp = Postgres.Type(Postgres.CONST['REGPROCEDUREOID'])

def main():
	try:
		fun = rp('nosuchfunc(int17,zzz)')
	except:
		# Should be valid, but the protection of
		# PL_DB_IN_ERROR should keep it from getting called.
		rp('pg_x_failure_suf()')
$python$;
SELECT pg_failure_suf_IFTE();

-- suffocates a pg error, and leaves an open xact, attempts to enter a protected area
CREATE OR REPLACE FUNCTION pg_x_failure_suf_IFTE() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

rp = Postgres.Type(Postgres.CONST['REGPROCEDUREOID'])

def main():
	x=xact()
	x.__enter__()
	try:
		fun = rp('nosuchfunc(int17,zzz)')
	except:
		# Should be valid, but the protection of
		# PL_DB_IN_ERROR should keep it from getting called.
		rp('pg_x_failure_suf()')
$python$;
SELECT pg_x_failure_suf_IFTE();

CREATE OR REPLACE FUNCTION check_errordata_access() RETURNS SETOF text LANGUAGE python AS
$python$
import Postgres

rp = Postgres.Type(Postgres.CONST['REGPROCEDUREOID'])
keys = (
	'message',
	'detail',
	'detail_log',
	'context',
	'elevel',
	'sqlerrcode',
	'code',
	'severity',
)

def main():
	try:
		with xact():
			rp('nosuchfunc(int17,zzz)')
	except Postgres.Exception as e:
		errdata = e.pg_errordata
	# attribute checks
	errdata.internalpos
	errdata.cursorpos
	errdata.lineno
	errdata.funcname
	errdata.saved_errno
	errdata.domain
	return (
		x + ':' + str(getattr(errdata, x)) for x in keys
	)
$python$;
SELECT check_errordata_access();

-- check the attributes available on Postgres.Exception
CREATE OR REPLACE FUNCTION check_exc_access() RETURNS SETOF text LANGUAGE python AS
$python$
import Postgres

keys = (
	'code',
	'message',
	'errno',
	'severity',
)

def main():
	try:
		with xact():
			Postgres.ERROR(
				code = '22331',
				message = 'message',
				context = 'context',
				detail = 'detail'
			)
	except Postgres.Exception as e:
		err = e
	# attribute checks
	d = list(err.details.items())
	d.sort(key=lambda x: x[0])
	return tuple((
		x + ':' + str(getattr(err, x)) for x in keys
	)) + tuple(d)
$python$;
SELECT check_exc_access();


-- __func__ must exist
CREATE OR REPLACE FUNCTION silly_function() RETURNS VOID LANGUAGE python AS
$python$
del __func__
def main():
	raise Exception("haha")
$python$;

-- raised the 'haha' exception.. module is known
SELECT silly_function();
-- raise an Attribute error; we need __func__.
SELECT silly_function();


-- __func__ must be a Postgres.Function
CREATE OR REPLACE FUNCTION evil_function() RETURNS VOID LANGUAGE python AS
$python$
__func__ = None
def main():
	raise Exception("muahahahhaha")
$python$;

-- The Exception in main gets raised as we grabbed the reference from
-- our own call to load_module().
SELECT evil_function();
-- This time, fn_extra is NULL and there's a module in sys.modules.
-- Validate that __func__ is actually a Postgres.Function object.
-- This will fail with a TypeError
SELECT evil_function();


-- __func__ must be a Postgres.Function with the Oid of the called function
CREATE OR REPLACE FUNCTION more_evil_function() RETURNS VOID LANGUAGE python AS
$python$
__func__ = proc('evil_function()')
def main():
	raise Exception("muahahahhahaHAHAHAHA")
$python$;

-- same as before, we already have the module object
SELECT more_evil_function();
-- It's a function object, but the wrong function object.
-- Raise a value error this time...
SELECT more_evil_function();


CREATE OR REPLACE FUNCTION custom_error(bool) RETURNS VOID LANGUAGE python AS
$python$
import Postgres

def main(with_tb):
	Postgres.ERROR(
		code = 'AAAAA',
		message = 'message',
		context = 'context',
		detail = 'detail',
		hint = 'hint',
		inhibit_pl_context = not bool(with_tb),
	)
$python$;
\set VERBOSITY verbose

SELECT custom_error(true);
SELECT custom_error(false);

\set VERBOSITY default

-- Ignore the inhibit_pl_context setting when it's not a Postgres.Exception.
CREATE OR REPLACE FUNCTION inhibited_pl_context_pyerr() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

def main():
	ve = ValueError("show this traceback")
	ve.inhibit_pl_context = True
	raise ve
$python$;
SELECT inhibited_pl_context_pyerr();


-- exercise a relayed exception
-- probably better categorized relative to "bool", but there isn't a place..
CREATE OR REPLACE FUNCTION boolean_blowup() RETURNS bool LANGUAGE python AS
$python$
class foo(object):
	def __bool__(self):
		raise ValueError("bad nonzero implementation, relay this exception")

def main():
	return foo()
$python$;

SELECT boolean_blowup();


-- test direct function execution while "in" a Python exception --
-- (no database error hath occurred)

-- XXX: Not the desired effect for CONTEXT
--
-- The following function shows the effect of Python's __context__.
-- For plpython, the global __context__ needs to be stored and restored
-- every time the plhandler is entered. This is necessary to ensure that
-- the traceback print out is not redundant.
--
-- There are a couple ways this could be hacked:
--
--  1. setting an exception on entry to identify the context and
--     breaking the context chain at that point iff an exception occurs
--  2. storing and restoring the context from the thread state.
--     (currently a bit too "dangerous", so we'll have to poll the capi sig)
--
CREATE OR REPLACE FUNCTION check_dfc_in_exc(bool) RETURNS VOID LANGUAGE python AS
$python$
from Postgres import WARNING
def main(cont):
	try:
		if cont:
			raise TypeError("lame")
		else:
			raise ValueError("err")
	except ValueError as exc:
		__func__(True)
$python$;

SELECT check_dfc_in_exc(FALSE);



-- make sure the linecache is getting cleared
CREATE OR REPLACE FUNCTION check_linecache_clear() RETURNS VOID LANGUAGE python AS
$python$
def main():
	raise ValueError
$python$;
SELECT check_linecache_clear();

-- should show the "raise TypeError" in the traceback string.
-- adjust Postgres._pl_eox to *not* clear the linecache to see how it operated before
CREATE OR REPLACE FUNCTION check_linecache_clear() RETURNS VOID LANGUAGE python AS
$python$
def main():
	raise TypeError
$python$;
SELECT check_linecache_clear();
