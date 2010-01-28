SET standard_conforming_strings TO ON;

CREATE OR REPLACE FUNCTION nummod22(numeric) RETURNS numeric LANGUAGE python AS
$python$
def main(n):
	return type(n)(n, mod = (2,2))
$python$;
SELECT nummod22(0.22::numeric);
-- should throw error
SELECT nummod22(1.22::numeric);

-- using mod with typinput
CREATE OR REPLACE FUNCTION strnummod22(numeric) RETURNS numeric LANGUAGE python AS
$python$
def main(n):
	return type(n)(str(n), mod = (2,2))
$python$;

SELECT strnummod22(0.22::numeric);
-- should throw error
SELECT strnummod22(1.22::numeric);

-- truncate some varchars
CREATE OR REPLACE FUNCTION truncvc(varchar) RETURNS varchar LANGUAGE python AS
$python$
from Postgres.types import varchar
def main(n):
	return varchar(n, mod = (10,))
$python$;

SELECT truncvc('foo'::varchar);
SELECT truncvc('too-many-characters'::varchar);

-- modin and modout
-- a few consistency checks to exercise the code paths.

CREATE OR REPLACE FUNCTION py_modin(anyelement, text[]) RETURNS int4 STRICT LANGUAGE python AS
$python$

def main(ob, modrepr):
	return type(ob).typmodin(modrepr)
$python$;

SELECT py_modin(1::numeric, ARRAY['2','2']) = numerictypmodin(ARRAY['2','2']::cstring[]) as numeric_check;
SELECT py_modin(''::varchar, ARRAY['20']) = varchartypmodin(ARRAY['20']::cstring[]) as varchar_check;

CREATE OR REPLACE FUNCTION py_modout(anyelement, int4) RETURNS text STRICT LANGUAGE python AS
$python$

def main(ob, mod):
	return type(ob).typmodout(mod)
$python$;

SELECT py_modout(1::numeric, -1) = numerictypmodout(-1)::text AS numeric_check;
SELECT py_modout(1::numeric, 100) = numerictypmodout(100)::text AS numeric_check;
SELECT py_modout(''::varchar, -1) = varchartypmodout(-1)::text AS varchar_check;
SELECT py_modout(''::varchar, 20) = varchartypmodout(20)::text AS varchar_check;

CREATE OR REPLACE FUNCTION py_modout_str(anyelement, text) RETURNS text STRICT LANGUAGE python AS
$python$

def main(ob, mod):
	return type(ob).typmodout(mod)
$python$;

-- error while building datum
SELECT py_modout_str(1::numeric, 'foo');

-- some errors
SELECT py_modin(1::numeric, ARRAY['not-a-number']);
SELECT py_modin(1::numeric, ARRAY['3','2','1']);
