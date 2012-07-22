-- Test methods on Postgres.Type instances.

CREATE OR REPLACE FUNCTION typreceive_none_is_none() RETURNS text LANGUAGE python AS
$python$
from Postgres.types import int2, int4, int8, text, point, bytea

def main():
	# multiple types checked for no particular reason.
	assert int2.typreceive(None) is None
	assert int4.typreceive(None) is None
	assert int8.typreceive(None) is None
	assert text.typreceive(None) is None
	assert point.typreceive(None) is None
	assert bytea.typreceive(None) is None
	return 'success'
$python$;

SELECT typreceive_none_is_none();

DROP DOMAIN IF EXISTS binio_d;
-- We should *not* be applying the check.
-- That is left to the user of the untrusted language.
CREATE DOMAIN binio_d AS int2 CHECK (VALUE > 100);

DROP TYPE IF EXISTS binio_c;
CREATE TYPE binio_c AS (i int, t text);

DROP TYPE IF EXISTS binio_e;
CREATE TYPE binio_e AS ENUM ('foo', 'bar');

CREATE OR REPLACE FUNCTION binio() RETURNS text LANGUAGE python AS
$python$
from Postgres.types import int2, int4, int8, text, bytea, regtype
from Postgres import Type

domain = Type(regtype('binio_d'))
composite = Type(regtype('binio_c'))
enum = Type(regtype('binio_e'))

# simple consistency checks for some types
samples = [
	int2(321), int2(123),
	int4(321), int4(123),
	int8(321), int8(123),
	text('meh'), text('feh'),
	bytea(b'(\x00)'), bytea(b'\x00\x01'),
	domain(200), domain(10),
	enum('foo'), enum('bar'),
	composite((123, 'foo')), composite((321, 'bar')),
]

def main():
	for x in samples:
		t = type(x)
		b = t.typsend(x)
		assert type(b) is bytes
		x2 = t.typreceive(b)
		assert type(x2) is type(x) 
		assert x2 == x
	return 'success'
$python$;

SELECT binio();

CREATE OR REPLACE FUNCTION typreceive_not_a_buffer() RETURNS VOID LANGUAGE python AS
$python$
from Postgres.types import int4

def main():
	int4.typreceive(1234)
$python$;
SELECT typreceive_not_a_buffer();


CREATE OR REPLACE FUNCTION invalid_type_typsend() RETURNS VOID LANGUAGE python AS
$python$
from Postgres.types import int2, int4

def main():
	int4.typsend(int2(42))
$python$;
SELECT invalid_type_typsend();


CREATE OR REPLACE FUNCTION invalid_type_typreceive() RETURNS VOID LANGUAGE python AS
$python$
from Postgres.types import int2, int4

def main():
	int4.typreceive(int2.typsend(int2(42)))
$python$;
SELECT invalid_type_typreceive();

CREATE OR REPLACE FUNCTION check_typinput() RETURNS text LANGUAGE python AS
$python$
from Postgres.types import int2, int4, bytea, text, point

def main():
	assert str(int2.typinput('200')) == '200'
	assert str(int4.typinput('400')) == '400'
	assert bytes(bytea.typinput('\\000')) == b'\x00'
	assert str(point.typinput('(1.0,1.0)')) in ('(1.0,1.0)', '(1,1)')
	return 'success'
$python$;
SELECT check_typinput();


-- make sure shell types are getting loaded

DROP TYPE IF EXISTS shell_check;
CREATE TYPE shell_check;

CREATE OR REPLACE FUNCTION check_typisdefined(oid) RETURNS VOID LANGUAGE python AS
$python$
import Postgres

def main(oid):
	Postgres.Type(oid)
$python$;

SELECT check_typisdefined((SELECT oid FROM pg_type WHERE typname = 'shell_check' LIMIT 1));

-- Lookup the Type using the table's oid
DROP TABLE IF EXISTS explicit_lookup;
CREATE TABLE explicit_lookup (i int, t text);
CREATE OR REPLACE FUNCTION lookup_tables_type(oid) RETURNS TEXT LANGUAGE python AS
$$
import Postgres

def main(oid):
	return str(list(Postgres.Type.from_relation_id(oid).descriptor))
$$;

SELECT lookup_tables_type('explicit_lookup'::regclass);
