-- Test the fact that the domain constraint is being applied
-- in multiple situations.
DROP DOMAIN IF EXISTS i5 CASCADE;
DROP DOMAIN IF EXISTS i4 CASCADE;
DROP DOMAIN IF EXISTS i3 CASCADE;
DROP DOMAIN IF EXISTS i2 CASCADE;
DROP DOMAIN IF EXISTS i1 CASCADE;

-- No constraint, simple alias.
CREATE DOMAIN i1 AS int4;

-- NOT NULL restriction
CREATE DOMAIN i2 AS i1 NOT NULL;

-- Must be greater than or equal to zero
CREATE DOMAIN i3 AS i2 CHECK (VALUE >= 0);

-- And must be less than or equal to zero(only one possible value)
CREATE DOMAIN i4 AS i3 CHECK (VALUE <= 0);

-- And must not be zero(no possible values)
CREATE DOMAIN i5 AS i4 CHECK (VALUE != 0);

-- The point of these domains is to check that the all the basetype
-- constraints are actually being tested when a check() is performed

CREATE OR REPLACE FUNCTION test_domain_instantiation() RETURNS text LANGUAGE python AS
$python$
import Postgres

# Just pack everything in here.
# It's a bit unittest-ish, but very convenient for the test we're doing.

rt = Postgres.Type(Postgres.CONST['REGTYPEOID'])

# load out of order, just in case the implementation
# has some issue wrt fully resolving supers.
i5 = Postgres.Type(rt('i5'))
i2 = Postgres.Type(rt('i2'))
i1 = Postgres.Type(rt('i1'))
i3 = Postgres.Type(rt('i3'))
i4 = Postgres.Type(rt('i4'))
int4 = Postgres.Type(rt('int4'))

# sanity check; proper inheritance is happening, yes?
domains = [
	(i5,i4),
	(i4,i3),
	(i3,i2),
	(i2,i1),
	(i1,int4),
	(int4,Postgres.Object),
]
for sub,sup in domains:
	assert(issubclass(sub,sup))

data = [
	(int4, [
		# just way too big
		234324234324234324234234324234, -123123223742348742384237832,
		-123, 0, 234,
		None
	]),
	(i1, [
		# ditto
		234324234324234324234234324234, -123123223742348742384237832,
		-123, 0, 234,
		None
	]),
	(i2, [
		234324234324234324234234324234, -123123223742348742384237832,
		-123, 0, 234,
		None,
	]),
	(i3, [
		# fail on -1
		-1, 0, 123,
		None
	]),
	(i4, [
		# fail on -1 and 1234
		-2, 0, 1234,
		None
	]),
	(i5, [
		# fail on -1, 0, and 12345
		-3, 0, 12345,
		None
	]),
]

def main():
	output = "\n"
	for typ, dl in data:
		output += "\n" + typ.__name__ + ":\n"
		for x in dl:
			try:
				with xact():
					r = typ(x)
					typ.check(r)
				output += " success for: '" + str(x) + "' -> '" + str(r) + "' (" + type(r).__name__ + ")\n"
			except Postgres.Exception as e:
				output += " fail for: '" + str(x) + "' [" + str(e.code) + ": " + str(e.message) + "]\n"
			except Exception as e:
				output += " fail for: '" + str(x) + "' [" + str(e) + "]\n"
	return output
$python$;

SELECT test_domain_instantiation();

-- This test is validating that domain casting functionality
-- is available. Specifically, no Postgres CAST is actually
-- being used. Rather, the relationship between the types should
-- be identified and used to merely perform a datumCopy
CREATE OR REPLACE FUNCTION test_domain_casts() RETURNS text LANGUAGE python AS
$python$
import Postgres

# Just pack everything in here.
# It's a bit unittest-ish, but very convenient for the test we're doing.

rt = Postgres.Type(Postgres.CONST['REGTYPEOID'])

int4 = Postgres.Type(rt('int4'))
i1 = Postgres.Type(rt('i1'))
i2 = Postgres.Type(rt('i2'))
i3 = Postgres.Type(rt('i3'))
i4 = Postgres.Type(rt('i4'))
i5 = Postgres.Type(rt('i5'))

data = [
	(int4, [
		int4(-123), int4(0), int4(234), int4(None)
	]),
	(i1, [
		i1(-123), i1(0), i1(234), i1(None)
	]),
	(i2, [
		i2(-123), i2(0), i2(234),
	]),
	(i3, [
		i3(0), i3(123),
	]),
	(i4, [i4(0),]),
	(i5, []),
]

types = (int4, i1, i2, i3, i4, i5)

def main():
	output = "\n"
	for typ, dl in data:
		output += "\n" + typ.__name__ + ":\n"
		for x in dl:
			for desttyp in types:
				try:
					with xact():
						r = desttyp(x)
						desttyp.check(r)
					assert r == x
					output += " success for: '" + str(x) + "' -> (" + desttyp.__name__ + ")\n"
				except Postgres.Exception as e:
					output += " fail for: '" + str(x) + "' -> " + desttyp.__name__ + " [" + str(e.code) + ": " + str(e.message) + "]\n"
				except Exception as e:
					output += " fail for: '" + str(x) + "' -> " + desttyp.__name__ + " [" + str(e) + "]\n"
	return output
$python$;

SELECT test_domain_casts();

CREATE OR REPLACE FUNCTION
check_on_srf(OUT a i1, OUT i i2, OUT j i3, OUT j2 i4) RETURNS SETOF record
LANGUAGE python AS
$python$
def main():
	# last one is invalid
	return ((432, 432, 0, -1),)
$python$;

-- python is an untrusted language, the user must explicitly
-- check domain values.
SELECT check_on_srf();
SELECT * FROM check_on_srf();
