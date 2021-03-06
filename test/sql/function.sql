CREATE OR REPLACE FUNCTION cant_execute_srf()
RETURNS SETOF int LANGUAGE SQL AS
'SELECT 1';

CREATE OR REPLACE FUNCTION cant_execute_poly(anyelement)
RETURNS anyelement LANGUAGE SQL AS
'SELECT $1';

CREATE OR REPLACE FUNCTION cant_execute_trig()
RETURNS TRIGGER LANGUAGE python AS
$python$
pass
$python$;

CREATE OR REPLACE FUNCTION check_not_implemented() RETURNS text language python AS
$python$
funcs = [
	proc('cant_execute_srf()'),
	proc('cant_execute_poly(anyelement)'),
	proc('cant_execute_trig()'),
]
def main():
	i = 0
	for x in funcs:
		i = i + 1
		try:
			x()
			return 'fail @ ' + str(i)
		except NotImplementedError:
			pass
	return 'success'
$python$;

-- Make sure we can't directly execute SRFs, polymorphic,
-- and trigger returning functions.
SELECT check_not_implemented();


-- Check that the schema is being properly quoted
DROP SCHEMA IF EXISTS "need""quote" CASCADE;
CREATE SCHEMA "need""quote";
CREATE FUNCTION "need""quote".funcname(int, text) RETURNS VOID LANGUAGE python AS
$python$
nspname = prepare('select nspname from pg_catalog.pg_namespace WHERE oid = $1').first
def main(i, t):
	assert nspname(__func__.namespace) == __func__.nspname
	raise ValueError("look at the filename:" + __func__.filename)
$python$;
SELECT "need""quote".funcname(1,'t');
-- The fun part is where the original nspname is no longer up-to-date.
