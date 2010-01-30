CREATE OR REPLACE FUNCTION a_first(a anyarray) RETURNS anyelement LANGUAGE python AS
$python$
def main(a):
	return a[0]
$python$;

SELECT a_first(ARRAY[1]::int[]);
SELECT a_first(ARRAY[2]::int[]);
SELECT a_first(ARRAY[3]::text[]);
SELECT a_first(ARRAY['foo','bar']::text[]);

CREATE OR REPLACE FUNCTION concat_arrays(a anyarray, b anyarray) RETURNS anyarray LANGUAGE python AS
$python$
def main(a, b):
	return a + b
$python$;

SELECT concat_arrays(ARRAY[1]::int[], '{321,123}');
SELECT concat_arrays(ARRAY[2]::bigint[], '{2334343434,3213123,232123,23123}');
SELECT concat_arrays(ARRAY[['foo'],['bar']]::text[], '{{foo},{bar}}');

CREATE OR REPLACE FUNCTION wrap_element(a anyelement) RETURNS anyarray LANGUAGE python AS
$python$
def main(a):
	return type(a).Array([a])
$python$;

SELECT wrap_element('foo'::text);
SELECT wrap_element(2::int);
SELECT wrap_element(3::bigint);

CREATE OR REPLACE FUNCTION object_type(ob ANYNONARRAY) RETURNS text LANGUAGE python AS
$python$
def main(ob):
	return type(ob).typname
$python$;

SELECT object_type('foo'::text);
SELECT object_type('foo'::cstring);
SELECT object_type(123);
SELECT object_type(123::bigint);
SELECT object_type(123::smallint);
SELECT object_type(123::numeric);

CREATE OR REPLACE FUNCTION object_type(ob ANYARRAY) RETURNS text LANGUAGE python AS
$python$
def main(ob):
	return type(ob).Element.typname
$python$;

SELECT object_type('{foo}'::text[]);
SELECT object_type('{foo}'::cstring[]);
SELECT object_type(ARRAY[123]);
SELECT object_type('{123}'::bigint[]);
SELECT object_type('{123}'::smallint[]);
SELECT object_type('{123}'::numeric[]);


DROP TYPE IF EXISTS aa_enum;
DROP TYPE IF EXISTS bb_enum;
CREATE TYPE aa_enum AS ENUM ('ugh', 'feh', 'bleh');
CREATE TYPE bb_enum AS ENUM ('bleh', 'feh', 'ugh');

CREATE OR REPLACE FUNCTION enumpoly(ANYENUM) RETURNS BOOLEAN LANGUAGE python AS
$python$
def main(e):
	t = type(e)
	return t('ugh') > t('bleh')
$python$;

-- ugh is less than bleh in bb_enum (should be false)
SELECT enumpoly('feh'::aa_enum);
-- ugh is greater than bleh in bb_enum (should be true)
SELECT enumpoly('feh'::bb_enum);


CREATE OR REPLACE FUNCTION first_and_type(ob ANYARRAY, OUT first ANYELEMENT, OUT typname text)
RETURNS RECORD LANGUAGE python AS
$python$
def main(array):
	return (array[0], type(array).Element.typname)
$python$;

SELECT first_and_type('{foo,bar}'::text[]);
SELECT first_and_type('{foo,bar}'::cstring[]);
SELECT first_and_type('{1023,123,4232,423}'::smallint[]);

SELECT * FROM first_and_type('{foo,bar}'::text[]);
SELECT * FROM first_and_type('{foo,bar}'::cstring[]);
SELECT * FROM first_and_type('{1023,123,4232,423}'::smallint[]);


-- polymorphic srfs

CREATE OR REPLACE FUNCTION unroll_the_array(ob ANYARRAY) RETURNS SETOF ANYELEMENT
LANGUAGE python AS
$python$
def main(array):
	for x in array:
		if type(x) is type(array):
			for y in x:
				yield y
		else:
			yield x
$python$;

SELECT unroll_the_array(ARRAY[1,2,34,5,6,7]::int[]);
SELECT * FROM unroll_the_array(ARRAY[1,2,34,5,6,7]::int[]);

SELECT unroll_the_array(ARRAY[1,2,34,5,6,7]::text[]);
SELECT * FROM unroll_the_array(ARRAY[1,2,34,5,6,7]::text[]);

SELECT unroll_the_array(ARRAY[1,2,3,5,6,7]::"char"[]);
SELECT * FROM unroll_the_array(ARRAY[1,2,3,5,6,7]::"char"[]);

-- md action
SELECT unroll_the_array(ARRAY[[1::smallint,2],[3,5],[6,7]]);
SELECT * FROM unroll_the_array(ARRAY[[1::smallint,2],[3,5],[6,7]]);


-- polymorphic composite srf (same as above, but with an additional typname)

CREATE OR REPLACE FUNCTION unroll_the_array_more(ob ANYARRAY, OUT data ANYELEMENT, OUT typname text) RETURNS SETOF RECORD
LANGUAGE python AS
$python$
def main(array):
	for x in array:
		if type(x) is type(array):
			for y in x:
				yield y, type(y).typname
		else:
			yield x, type(x).typname
$python$;

SELECT unroll_the_array_more(ARRAY[1,2,34,5,6,7]::int[]);
SELECT * FROM unroll_the_array_more(ARRAY[1,2,34,5,6,7]::int[]);

SELECT unroll_the_array_more(ARRAY[1,2,34,5,6,7]::text[]);
SELECT * FROM unroll_the_array_more(ARRAY[1,2,34,5,6,7]::text[]);

SELECT unroll_the_array_more(ARRAY[1,2,3,5,6,7]::"char"[]);
SELECT * FROM unroll_the_array_more(ARRAY[1,2,3,5,6,7]::"char"[]);

-- md action
SELECT unroll_the_array_more(ARRAY[[1::smallint,2],[3,5],[6,7]]);
SELECT * FROM unroll_the_array_more(ARRAY[[1::smallint,2],[3,5],[6,7]]);
