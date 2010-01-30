-- This first test is used to validate the Array type's ability
-- to handle different typlen elements. text and bytea are redundant in that
-- fashion, but keep both to cover extra bases...
CREATE OR REPLACE FUNCTION py_check_array() RETURNS SETOF text LANGUAGE python AS
$python$
import Postgres
import operator
import itertools
import random

def get_n_samples(n, s):
	'get "n" number of random objects from "s" (dups are likely)'
	return list(itertools.islice((
		x for x in itertools.cycle(s)
		if random.random() > 0.6
	), n))

# sample types and data
#
# yes, random is bad, but we're not actually testing
# for transient failures. this is just an easy (lazy) way to
# collect N samples for any given need.
etypes = [
	(Postgres.types.int2, set([
		-1, 0, 1, 0x7FF, -0xFF, None
	])),
	(Postgres.types.int4, set([
		-1, 0, 1, 0x7FFFFF, -0xFFFFF, None
	])),
	(Postgres.types.int8, set([
		-1, 0, 1, 0x7FFFFFFFFF, -0xFFFFFFFFF, None
	])),
	(Postgres.types.text, set([
		"some text", "some more text",
		"less text", "that text", "meh",
		"element", "sample", "a"*50,
		"z"*100, None
	])),
	(Postgres.types.bytea, set([
		b'foobar\x00', r"\000foobar",
		b'FFF'*100, b'\x01'*20, None
	])),
	(Postgres.types.bpchar, set([
		'x', 'Y', 'a', 'b', 'n', None
	])),
	(Postgres.types.bool, set([
		None, 't', 'f', True, False, 1, 0
	]))
]

# Test all the array interfaces
def test_array(etyp, samples):
	at = etyp.Array
	E = at([])
	assert len(E) == 0
	assert E.ndim == 1
	assert E.dimensions[0] == 0
	assert E.lowerbounds[0] == 1
	try:
		E[0]
	except IndexError:
		pass
	else:
		assert False and "failed to raise IndexError"
	#
	one_d = get_n_samples(20, samples)
	A = at(one_d)
	assert len(one_d) == 20
	assert len(A) == 20
	assert A.Element is etyp
	assert type(A).Element is etyp
	assert type(A).Element.Array is at
	# Only works for single dimensions, ATM
	assert list(A) == A
	assert A.ndim == 1
	assert A.dimensions[0] == 20
	assert A.nelements == 20
	assert A.lowerbounds[0] == 1
	assert len(A.dimensions) == 1
	assert len(A.lowerbounds) == 1
	# Considering the way the elements are generated,
	# this could give a false positive, but it's not likely
	# for all of the types to fail.
	assert A[-1] == A[len(A)-1]
	assert A[-len(A)] == A[0]
	assert A[:] is A
	assert A[0:] is A
	# check slicing
	assert (A[:10] + A[11:]) == A

def main():
	for et, samples in etypes:
		yield 'checking ' + et.__name__
		test_array(et, samples)
	yield 'success'
$python$;

SELECT py_check_array();


CREATE OR REPLACE FUNCTION py_array_len(anyarray) RETURNS int LANGUAGE python AS
$python$
main = len
$python$;

SELECT py_array_len(ARRAY[1,2,3,4]::int[]);
SELECT py_array_len(ARRAY[[1,2],[3,4]]::int[]);
SELECT py_array_len('[2:4]={1,2,3}'::int[]);
SELECT py_array_len('[2:4][3:4]={{1,2},{3,4},{5,6}}'::int[]);

-- When the limitation of steps of 1 no longer exists,
-- this should be removed and additional checks should be added
-- to the test above.
CREATE OR REPLACE FUNCTION cant_use_not1_step(int[]) RETURNS text LANGUAGE python AS
$python$
import Postgres

def main(x):
	x[0:10:2]
$python$;

SELECT cant_use_not1_step('{1,2,3,4,5}'::int[]);

-- The Array constructor requires a string or a list.
CREATE OR REPLACE FUNCTION array_type_error() RETURNS text LANGUAGE python AS
$python$
from Postgres.types import int4
AT = int4.Array

def main():
	AT((1,2,3))
$python$;

SELECT array_type_error();


-- Build arrays from lists with varying dimensions
CREATE OR REPLACE FUNCTION try_deep_arrays() RETURNS text LANGUAGE python AS
$python$
from Postgres.types import int4
AT = int4.Array

def main():
	a0 = AT([])
	a1 = AT([0])
	a2 = AT([[0],[1]])
	a3 = AT([[0,1],[2,3]])
	a4 = AT([[[0,1],[2,3]], [[4,5],[6,7]]])
	a5 = AT([[[0,1,2],[3,4,5]], [[6,7,8],[9,10,11]]])
	a6 = AT([
		[[0,1,2],[3,4,5]], [[6,7,8],[8,9,10]],
		[[11,12,13],[14,15,16]], [[17,18,19],[20,21,22]]
	])
	a7 = AT([
		[
			[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
			[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
		]
	])
	a8 = AT([
		[
			[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
			[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
		],
		[
			[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
			[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
		],
		[
			[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
			[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
		],
	])
	a9 = AT([
		[
			[
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
			],
			[
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
			],
			[
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
			],
		],
		[
			[
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
			],
			[
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
			],
			[
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
			],
		],
	])
	a10 = AT([
		[
			[
				[
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				],
				[
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				],
				[
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				],
			],
			[
				[
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				],
				[
					[[0,1,2],[0,111,2]], [[1,2,3],[2,1333,0]],
					[[100,1,2],[0,1,2]], [[1,222,3],[2,1,0]],
				],
				[
					[[0,1212121,2],[0,1,2]], [[1,20202,3],[2,1,110]],
					[[0,1,2],[102220,1,2]], [[1,2,30303],[2,10101,0]],
				],
			],
		],
		[
			[
				[
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				],
				[
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
					[[0,15,2],[0,1,2]], [[1,2,3],[2,1,0]],
				],
				[
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				],
			],
			[
				[
					[[0,1,2],[0,1,2]], [[1,22,3],[2,1,0]],
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				],
				[
					[[0,1,2],[0,1,2]], [[1,27,3],[2,1,0]],
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				],
				[
					[[0,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
					[[19,1,2],[0,1,2]], [[1,2,3],[2,1,0]],
				],
			],
		],

	])
	return '\n'.join(str(x) for x in [
		a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10
	])
$python$;

SELECT try_deep_arrays();

CREATE OR REPLACE FUNCTION string_array_elements(anyarray) RETURNS text LANGUAGE python AS
$python$
def main(array):
	return str(list(array.elements()))
$python$;

SELECT string_array_elements(ARRAY[1::int2,5]);
SELECT string_array_elements(ARRAY[1::int4,5]);
SELECT string_array_elements(ARRAY[1::int8,5]);
SELECT string_array_elements(ARRAY[3::float8,18.0123]);
SELECT string_array_elements(ARRAY[['key','val'],['key2','val2']]::text[]);
SELECT string_array_elements(ARRAY[[NULL,'val'],['key2','val2']]::text[]);
SELECT string_array_elements(ARRAY[[[NULL,'val']],[['key2',NULL]]]::text[]);


-- test Array.from_elements constructor

CREATE OR REPLACE FUNCTION construct_array_from_elements(dims int[], lbs int[]) RETURNS text[] LANGUAGE python AS
$python$
def main(dims, lbs):
	return __func__.output.from_elements(
		["first","second","third","fourth"],
		dimensions = dims,
		lowerbounds = lbs)
$python$;

-- Working with a simple array, apply different dimensions and lowerbounds.
SELECT construct_array_from_elements(NULL, NULL);
SELECT construct_array_from_elements(ARRAY[4], ARRAY[2]);
SELECT (construct_array_from_elements(ARRAY[4], ARRAY[2]))[1] IS NULL AS out_of_bounds;
SELECT (construct_array_from_elements(ARRAY[4], ARRAY[2]))[5];
SELECT construct_array_from_elements(ARRAY[2,2], ARRAY[1,1]);
-- same, but (1,1) lowerbounds are implied
SELECT construct_array_from_elements(ARRAY[2,2], NULL);
SELECT construct_array_from_elements(ARRAY[1,1,1,4], NULL);
SELECT i[1][1][1][1] AS i1, i[1][1][1][2] AS i2, i[1][1][1][3] AS i3, i[1][1][1][4] AS i4 FROM construct_array_from_elements(ARRAY[1,1,1,4], NULL) AS A(i);
SELECT construct_array_from_elements(ARRAY[1,1,4,1], NULL);
SELECT construct_array_from_elements(ARRAY[1,2,2,1], NULL);

-- some errors
SELECT construct_array_from_elements(NULL, ARRAY[1,1]);
SELECT construct_array_from_elements(ARRAY[1,1], ARRAY[2,2,2]);
SELECT construct_array_from_elements(ARRAY[0,0,0], ARRAY[0,0,0]);

-- Create a new array using the components of the original
CREATE OR REPLACE FUNCTION reconstruct_array(anyarray) RETURNS text LANGUAGE python AS
$python$
def main(A):
	original = str(A)
	return 'original: %s --- reconstructed: %s' %(
		original, str(type(A).from_elements(
			A.elements(),
			dimensions = A.dimensions,
			lowerbounds = A.lowerbounds
		))
	)
$python$;

SELECT reconstruct_array('{}'::int[]);
SELECT reconstruct_array(ARRAY[0,1,2,3,4,5,6,7,8,9]::int[]);
SELECT reconstruct_array('[3:5]={1,2,3}'::int[]);
SELECT reconstruct_array('[3:5]={1,2,3}'::text[]);
SELECT reconstruct_array('[1:2][1:2]={{t1,t2},{t3,t4}}'::text[]);
SELECT reconstruct_array('[2:5][1:2]={{x,y},{t1,t2},{t3,t4},{a,b}}'::text[]);
-- And a NULL case.
SELECT reconstruct_array(ARRAY[NULL,'foo',NULL]::text[]);

-- And a composite case..
DROP TYPE IF EXISTS array_composite;
CREATE TYPE array_composite AS (i int, t text, c "char");
SELECT reconstruct_array(ARRAY[ROW(1,'t','c'), NULL, ROW(100,'text','z')]::array_composite[]);


-- Attempt to create an array using invalid elements
CREATE OR REPLACE FUNCTION invalid_elements(text) RETURNS text LANGUAGE python AS
$python$
from Postgres.types import int8

def main(x):
	int8.Array.from_elements([x])
	return 'failed'
$python$;

SELECT invalid_elements('foo');

-- Test sql_get_element() method
CREATE OR REPLACE FUNCTION py_array_sql_get_element(anyarray, int[]) RETURNS anyelement LANGUAGE python AS
$python$
def main(arr, ind):
	return arr.sql_get_element(ind)
$python$;

SELECT py_array_sql_get_element(ARRAY[1,2,3,4]::int[], ARRAY[1]);
SELECT py_array_sql_get_element(ARRAY[1,2,3,4]::int[], ARRAY[2]);
SELECT py_array_sql_get_element(ARRAY[[1,2],[3,4]]::int[], ARRAY[2,1]);
SELECT py_array_sql_get_element(ARRAY[[1,2],[3,4]]::int[], ARRAY[1,1]);
-- NULLs
SELECT py_array_sql_get_element(ARRAY[1,2,3,4]::int[], ARRAY[5]); -- out of bounds
SELECT py_array_sql_get_element(ARRAY[[1,2],[3,4]]::int[], ARRAY[2]); -- too few
SELECT py_array_sql_get_element(ARRAY[[1,2],[3,4]]::int[], ARRAY[1,2,3,4,5,6,7,8,9,10]); -- too many
-- Some repetition.
SELECT py_array_sql_get_element(ARRAY[[[[[['deep']]]]]]::text[], ARRAY[1,1,1,1,1,1]);
SELECT py_array_sql_get_element(ARRAY[[[[[['deep']]]]]]::text[], ARRAY[1,1,1,1,1]);
-- won't even get into plpython (too many dimensions)
SELECT py_array_sql_get_element(ARRAY[[[[[[['deep']]]]]]]::text[], ARRAY[1,1,1,1,1,1,1]);


-- Test get_element() method *zero*-based.
CREATE OR REPLACE FUNCTION py_array_get_element(anyarray, int[]) RETURNS anyelement LANGUAGE python AS
$python$
def main(arr, ind):
	return arr.get_element(ind)
$python$;

-- index error, no elements
SELECT py_array_get_element('{}'::int[], ARRAY[0]);
-- value error, empty index tuple
SELECT py_array_get_element('{}'::int[], '{}'::int[]);

-- success
SELECT py_array_get_element(ARRAY[1,2,3,4]::int[], ARRAY[1]);
SELECT py_array_get_element(ARRAY[1,2,3,4]::int[], ARRAY[3]);

-- negative indexes
SELECT py_array_get_element(ARRAY[1,2,3,4]::int[], ARRAY[-1]);
SELECT py_array_get_element(ARRAY[1,2,3,4]::int[], ARRAY[-2]);
SELECT py_array_get_element(ARRAY[1,2,3,4]::int[], ARRAY[-3]);
SELECT py_array_get_element(ARRAY[1,2,3,4]::int[], ARRAY[-4]);

-- IndexError's
SELECT py_array_get_element(ARRAY[1,2,3,4]::int[], ARRAY[-5]);
SELECT py_array_get_element(ARRAY[1,2,3,4]::int[], ARRAY[4]);

-- MD access
SELECT py_array_get_element(ARRAY[[1,2],[3,4]]::int[], ARRAY[1,0]);
SELECT py_array_get_element(ARRAY[[1,2],[3,4]]::int[], ARRAY[-1,-1]);

-- ValueError, too few
SELECT py_array_get_element(ARRAY[[1,2],[3,4]]::int[], ARRAY[2]);
-- ValueError, too many
SELECT py_array_get_element(ARRAY[[1,2],[3,4]]::int[], ARRAY[1,2,3,4,5,6,7,8,9,10]);

-- Deep access
SELECT py_array_get_element(ARRAY[[[[[['deep']]]]]]::text[], ARRAY[0,0,0,0,0,0]);
SELECT py_array_get_element(ARRAY[[[[[['deep'],['deeper']]]]]]::text[], ARRAY[0,0,0,0,1,0]);

-- and a couple more errors
SELECT py_array_get_element(ARRAY[[[[[['deep']]]]]]::text[], ARRAY[0,0,0,0,0,1]);
SELECT py_array_get_element(ARRAY[[[[[['deep']]]]]]::text[], ARRAY[0,0,0,0,0]);

