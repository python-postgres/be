-- ENUMs aren't very special.
-- They just need the typoid as the second argument.

DROP DOMAIN IF EXISTS dfoo;
DROP TYPE IF EXISTS foo;

CREATE TYPE foo AS ENUM (
	'zero',
	'one',
	'two',
	'three',
	'four'
);
CREATE DOMAIN dfoo AS foo;

CREATE OR REPLACE FUNCTION enummy(e foo) RETURNS foo LANGUAGE python AS
$python$
def main(e):
	return 'zero'
$python$;

SELECT enummy('two'::foo);
SELECT enummy('zero'::foo);
SELECT enummy('three'::foo);

-- Check instantiation about some comparisons.
CREATE OR REPLACE FUNCTION enum_ops(e foo) RETURNS foo LANGUAGE python AS
$python$
def main(e):
	et = type(e)
	zero = et('zero')
	one = et('one')
	two = et('two')
	assert zero == zero
	assert zero < one
	assert one < two
	assert zero < two
	assert not (zero > two)
	assert not (zero > one)
	return et('three')
$python$;

SELECT enum_ops('one'::foo);
SELECT enum_ops('two'::foo);
SELECT enum_ops('zero'::foo);


-- same code, but now the domain
CREATE OR REPLACE FUNCTION enummy(e dfoo) RETURNS foo LANGUAGE python AS
$python$
def main(e):
	return 'zero'
$python$;

SELECT enummy('two'::dfoo);
SELECT enummy('zero'::dfoo);
SELECT enummy('three'::dfoo);

-- Check instantiation about some comparisons.
CREATE OR REPLACE FUNCTION enum_ops(e dfoo) RETURNS foo LANGUAGE python AS
$python$
import Postgres

def main(e):
	et = type(e)
	zero = et('zero')
	one = et('one')
	two = et('two')
	assert zero == zero
	assert zero < one
	assert one < two
	assert zero < two
	assert not (zero > two)
	assert not (zero > one)
	return et('three')
$python$;

SELECT enum_ops('one'::dfoo);
SELECT enum_ops('two'::dfoo);
SELECT enum_ops('zero'::dfoo);
