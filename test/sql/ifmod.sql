SET client_encoding TO 'UTF-8';

CREATE OR REPLACE FUNCTION check_version_info() RETURNS BOOLEAN LANGUAGE python AS
$python$
import Postgres

def main():
	vi = Postgres.version_info
	if len(vi) != 5:
		raise ValueError("invalid Postgres.version_info: "+str(vi))
	if not isinstance(vi[-2], str):
		raise ValueError("invalid Postgres.version_info(state is not a str): "+str(vi))
$python$;
SELECT check_version_info();

CREATE OR REPLACE FUNCTION check_builtins() RETURNS text LANGUAGE python AS
$python$
import Postgres
def main():
	assert Postgres.execute is sqlexec
	assert Postgres.eval is sqleval
	assert Postgres.Statement is prepare
	proc
	assert Postgres.Transaction is xact
	return 'success'
$python$;
SELECT check_builtins();

CREATE OR REPLACE FUNCTION py_pg_version() RETURNS text LANGUAGE python AS
$python$
import Postgres

def main():
	return Postgres.version
$python$;
SELECT py_pg_version() = version();


CREATE OR REPLACE FUNCTION check_client_vars() RETURNS text LANGUAGE python AS
$python$
import Postgres

def main():
	assert Postgres.client_addr is None or isinstance(Postgres.client_addr, str)
	assert Postgres.client_port is None or isinstance(Postgres.client_port, str)
	return 'success'
$python$;
SELECT check_client_vars();


CREATE OR REPLACE FUNCTION get_backend_start() RETURNS timestamptz LANGUAGE python AS
$python$
import Postgres

def main():
	return Postgres.backend_start
$python$;
SELECT get_backend_start() = (SELECT backend_start FROM pg_stat_activity WHERE procpid = pg_backend_pid()) AS backend_start_comparison;
SELECT get_backend_start() = (SELECT backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()) AS backend_start_comparison;

CREATE OR REPLACE FUNCTION check_types() RETURNS text LANGUAGE python AS
$python$
import Postgres

def main():
	assert isinstance(Postgres.Type, type)
	assert isinstance(Postgres.Object, Postgres.Type)
	assert isinstance(Postgres.Array, Postgres.Type)
	assert isinstance(Postgres.String, Postgres.Type)
	assert isinstance(Postgres.Pseudo, Postgres.Type)
	assert isinstance(Postgres.TriggerData, type)
	assert isinstance(Postgres.ErrorData, type)
	assert isinstance(Postgres.Function, type)
	assert isinstance(Postgres.TupleDesc, type)
	assert isinstance(Postgres.Transaction, type)
	assert isinstance(Postgres.Statement, type)
	assert isinstance(Postgres.Cursor, type)
	return 'success'
$python$;
SELECT check_types();

CREATE OR REPLACE FUNCTION py_quoting() RETURNS text LANGUAGE python AS
$python$
import Postgres

def main():
	return """
literal: %s
literal with quote: %s
ident: %s
ident with quote: %s
null: %s
notnull: %s
	""" %(
		Postgres.quote_literal("foo"),
		Postgres.quote_literal("fo'o"),
		Postgres.quote_ident("bar"),
		Postgres.quote_ident('b"ar'),
		Postgres.quote_nullable(None),
		Postgres.quote_nullable("NOTNULL"),
	)
$python$;
SELECT py_quoting();

\set VERBOSITY verbose

-- Check Postgres.ereport and aliases
CREATE OR REPLACE FUNCTION py_report() RETURNS text LANGUAGE python AS
$python$
import Postgres

def main():
	Postgres.WARNING("danger", code = "01001",
		detail = "details", context = "added context", hint = "hint")
	Postgres.NOTICE("be advised", code = "00000",
		detail = "details", context = "added context", hint = "hint")
	Postgres.INFO("useless info", code = "00000",
		detail = "details", context = "added context", hint = "hint")
	Postgres.DEBUG("internal state info--probably won't see this",
		code = "00000", detail = "details", context = "added context", hint = "hint")
	Postgres.ereport(Postgres.severities['DEBUG4'],
		"more unseen internal state info", detail = "details",
		context = "added context", hint = "hint")
	Postgres.ERROR("it's okay", code = "XX002", detail = "details", context = "added context", hint = "hint")
	return "fail--should never return"
$python$;
SELECT py_report();

-- Check that the stdio replacements are operating.
CREATE OR REPLACE FUNCTION py_warnings() RETURNS VOID LANGUAGE python AS
$python$
import warnings

def main():
	warnings.warn("show this text as a pg warning")
$python$;
SELECT py_warnings();

\set VERBOSITY default

-- Check that the stdio replacements are operating.
CREATE OR REPLACE FUNCTION py_stdio() RETURNS VOID LANGUAGE python AS
$python$
import sys

def main():
	sys.stdout.write("data to stdout")
	sys.stderr.write("data to stderr")
	try:
		sys.stdin.read()
	except RuntimeError:
		pass
$python$;
SELECT py_stdio();

-- Postgres.notify
CREATE OR REPLACE FUNCTION py_notify(text) RETURNS VOID LANGUAGE python AS
$python$
import Postgres

def main(x):
	Postgres.notify(x)
$python$;
-- XXX: can't LISTEN; test would cause inconsistent results (psql prints the pid).
--LISTEN "foo";
SELECT py_notify('foo');


-- Postgres.*_timestamp()...
CREATE OR REPLACE FUNCTION py_timestamps() RETURNS text LANGUAGE python AS
$python$
import Postgres

def main():
	tsxact = Postgres.transaction_timestamp()
	tsstmt = Postgres.statement_timestamp()
	assert Postgres.sleep(0.2) is None
	tsnow = Postgres.clock_timestamp()
	assert tsstmt == tsxact
	assert tsnow >= tsxact
	return 'success'
$python$;

SELECT py_timestamps();

CREATE OR REPLACE FUNCTION py_backend_control() RETURNS text LANGUAGE python AS
$python$
import Postgres

def main():
	# validate what we can, and be done. :(
	assert isinstance(Postgres.terminate_backend, Postgres.Function)
	assert Postgres.terminate_backend.input[0]['atttypid'] == Postgres.CONST["INT4OID"]
	assert isinstance(Postgres.cancel_backend, Postgres.Function)
	assert Postgres.cancel_backend.input[0]['atttypid'] == Postgres.CONST["INT4OID"]
	return 'success'
$python$;

-- pretty weak test :(
SELECT py_backend_control();

CREATE OR REPLACE FUNCTION py_nosuchtype() RETURNS VOID LANGUAGE python AS
$python$
import Postgres

def main():
	from Postgres.types import foobar_nosuch_type__
$python$;
SELECT py_nosuchtype();


-- search_path --

DROP SCHEMA IF EXISTS testns1 CASCADE;
DROP SCHEMA IF EXISTS testns2 CASCADE;
CREATE SCHEMA testns1;
CREATE SCHEMA testns2;

CREATE OR REPLACE FUNCTION testns1.py_search_paths() RETURNS text LANGUAGE python AS
$python$
import Postgres

nspoid = prepare("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1").first

ns_names = Postgres.current_schemas()
ns_oids = Postgres.current_schemas_oid()

def main():
	assert 'testns1' in ns_names
	assert 'testns2' in ns_names
	assert nspoid('testns1') in ns_oids
	assert nspoid('testns2') in ns_oids
	sqlexec('DROP SCHEMA testns2')
	# anyway to induce None?
	assert 'testns1' in Postgres.current_schemas()
	assert 'testns2' not in Postgres.current_schemas()
	new_ns_oids = Postgres.current_schemas_oid()
	assert nspoid('testns1') in new_ns_oids
	assert nspoid('testns2') is None
	return 'success'
$python$;

BEGIN;
SET search_path = testns1,testns2;
SELECT testns1.py_search_paths();
ABORT;

--
-- more inhibit_pl_context influence checks (error cases in error.sql)
--
CREATE OR REPLACE FUNCTION inhibited_pl_context_warning(bool) RETURNS VOID LANGUAGE python AS
$python$
import Postgres

def main(with_tb):
	if with_tb is not None:
		with_tb = bool(with_tb)
	Postgres.WARNING(
		code = 'BBBBB',
		message = 'message',
		context = 'context',
		detail = 'detail',
		hint = 'hint',
		inhibit_pl_context = with_tb
	)
$python$;
\set VERBOSITY verbose
-- i concur, inhibit it. (shows no effect)
SELECT inhibited_pl_context_warning(true);
-- nay, it's wrong, don't inhibit it. (shows override)
SELECT inhibited_pl_context_warning(false);
-- and what happens if it's None (default takes effect)
SELECT inhibited_pl_context_warning(NULL);
