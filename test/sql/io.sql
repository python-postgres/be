SET standard_conforming_strings TO ON;

CREATE OR REPLACE FUNCTION nothing() RETURNS VOID LANGUAGE python AS
$python$
def main():
	pass
$python$;
SELECT nothing();

CREATE OR REPLACE FUNCTION return(int) RETURNS int LANGUAGE python AS
$python$
def main(n):
	return n
$python$;
SELECT return(-3);
SELECT return(3);
SELECT return(0);
SELECT return(NULL);

CREATE OR REPLACE FUNCTION return(bigint) RETURNS bigint
LANGUAGE python AS
$python$
def main(n):
	return n
$python$;
SELECT return(-12839128482::bigint);
SELECT return(12839128482::bigint);
SELECT return(NULL::bigint);


CREATE OR REPLACE FUNCTION return(text) RETURNS text
LANGUAGE python AS
$python$
def main(n):
	return n
$python$;
SELECT return(E'\nfoo bar feh   \t\t\n\n');
SELECT return(NULL::text);

CREATE OR REPLACE FUNCTION return(bytea) RETURNS bytea
LANGUAGE python AS
$python$
def main(d):
	return d
$python$;
SELECT return('\020foo\011\000\001'::bytea);

CREATE OR REPLACE FUNCTION return(bigint[])
RETURNS bigint[]
LANGUAGE python AS
$python$
def main(n):
	return n
$python$;
SELECT return(ARRAY[1::bigint,2,3]);
SELECT return(ARRAY[3::bigint,2,1]);
SELECT return(ARRAY[[3::bigint],[2],[1]]);

DROP TYPE IF EXISTS composite CASCADE;
CREATE TYPE composite AS (foo text, bar int);
CREATE OR REPLACE FUNCTION return(composite) RETURNS composite
LANGUAGE python AS
$python$
def main(n):
	return n
$python$;
SELECT return(CAST(ROW('foo', 321) AS composite));

-- Test pl's "explicit" cast
CREATE OR REPLACE FUNCTION outonly(OUT foo int)
LANGUAGE python AS
$python$
def main():
	return 2
$python$;
SELECT outonly();

CREATE OR REPLACE FUNCTION inoutonly(INOUT foo int)
LANGUAGE python AS
$python$
def main(n):
	return n
$python$;
SELECT inoutonly(2);
SELECT inoutonly(3);
SELECT inoutonly(-3);
SELECT inoutonly(NULL);

CREATE OR REPLACE FUNCTION multiple_outs(OUT foo int, OUT bar int)
LANGUAGE python AS
$python$
def main():
	return (1, 2)
$python$;
SELECT multiple_outs();

CREATE OR REPLACE FUNCTION
in_multiple_outs(INOUT foo int, OUT bar int)
LANGUAGE python AS
$python$
def main(n):
	return (n, 2)
$python$;
SELECT in_multiple_outs(1);
SELECT in_multiple_outs(-11);
SELECT in_multiple_outs(1982);


-- Validate that the function cache is invalidated when a new one
-- is created in the same session.
CREATE OR REPLACE FUNCTION changing() RETURNS text
LANGUAGE python AS
$python$
def main():
	return "foo"
$python$;
SELECT changing();

CREATE OR REPLACE FUNCTION changing() RETURNS text
LANGUAGE python AS
$python$
def main():
	return "foobar"
$python$;
SELECT changing();

CREATE OR REPLACE FUNCTION changing() RETURNS text
LANGUAGE python AS
$python$
def main():
	return "bar"
$python$;
SELECT changing();
