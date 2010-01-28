-- Test the composite Python interfaces.
--

DROP TYPE IF EXISTS ctyp CASCADE;
CREATE TYPE ctyp AS (
	i int,
	b bigint,
	t text,
	n numeric,
	c "char"
);

DROP TABLE IF EXISTS reltyp CASCADE;
CREATE TABLE reltyp (
	i int,
	b bigint,
	t text,
	n numeric,
	c "char"
);

-- Yes, it fails to create a domain.
-- Leave this here, so if that ever changes, we'll know more
-- tests will be needed.
DROP DOMAIN IF EXISTS dtyp CASCADE;
CREATE DOMAIN dtyp AS ctyp;

CREATE OR REPLACE FUNCTION check_composite_access(arg ctyp) RETURNS text LANGUAGE python AS
$python$
import Postgres
def main(a):
	assert [a[name] for name in type(a).descriptor.column_names] == [a[i] for i in range(len(a))]
	t = tuple(a)
	assert t == tuple([a[i] for i in range(len(a))])
	l = list(t)
	l.reverse()
	assert l == [a[i] for i in range(-1, -(len(t)+1), -1)]
	i, b, t, n, c = a
	return 'success'
$python$;

SELECT check_composite_access(ROW(1,2,'t',3.1,'x')::ctyp);

CREATE OR REPLACE FUNCTION check_composite_access(arg reltyp) RETURNS text LANGUAGE python AS
$python$

def main(a):
	assert [a[name] for name in type(a).descriptor.column_names] == [a[i] for i in range(len(a))]
	t = tuple(a)
	assert t == tuple([a[i] for i in range(len(a))])
	i, b, t, n, c = a
	return 'success'
$python$;

SELECT check_composite_access(ROW(1,2,'t',3.1,'x')::reltyp);

ALTER TABLE reltyp DROP COLUMN n;
ALTER TABLE reltyp DROP COLUMN c;
SELECT check_composite_access(ROW(1,2,'t')::reltyp);
ALTER TABLE reltyp ADD COLUMN n numeric;
ALTER TABLE reltyp ADD COLUMN c "char";

SELECT check_composite_access(ROW(1,2,'t',3.1,'x')::reltyp);


CREATE OR REPLACE FUNCTION check_composite_slicing(arg reltyp) RETURNS text LANGUAGE python AS
$python$
def main(a):
	n_index = type(a).descriptor.column_names.index("n")
	assert a[0:"n"] == a["i":n_index] == a[0:n_index]
	assert a[0:"n":2] == a["i":n_index:2] == a[0:n_index:2]
	assert a["n":0:-1] == a[n_index:"i":-1] == a[n_index:0:-1]
	return 'success'
$python$;

SELECT check_composite_slicing(ROW(1,2,'t',3.1,'x')::reltyp);


CREATE OR REPLACE FUNCTION slice_it(arg ctyp, s_start int, s_end int, s_step int) RETURNS text LANGUAGE python AS
$python$
def main(a, start, end, step):
	s = a[start:end:step]
	return str([str(x) for x in s])
$python$;

--
-- slicing is plagued with potential off-by-one errors, so go nuts here.
--

-- can't slice with zero step(throws error)
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, 2, 0);

SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, 2, 1), '1,2' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 1, 3, 1), '2,t' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 2, 0, -1), 't,2' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, -1, NULL, -1), 'x,3.1,t,2,1' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, NULL, NULL, -1), 'x,3.1,t,2,1' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, NULL, NULL, -2), 'x,t,1' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, NULL, NULL, 2), '1,t,x' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, NULL, NULL, -3), 'x,2' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, NULL, NULL, 3), '1,3.1' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, NULL, NULL, 1), '1,2,t,3.1,x' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, NULL, 1), '1,2,t,3.1,x' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, -1, 1), '1,2,t,3.1' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, -2, 1), '1,2,t' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, -3, 1), '1,2' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, -4, 1), '1' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, -5, 1), '[]' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, -6, 1), '[]' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 4, 4, -1), '[]' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 4, 5, -1), '[]' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, -1, 0, -2), 'x,t' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, -1, 2), '1,t' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, 1, 2), '1' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, 1, 234324234), '1' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, NULL, -234324234), '1' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, 4, 2), '1,t' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 0, 0, 234324234), '[]' AS answer;

SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 1203282, -213123, 234324234), '[]' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, -1203283, 213122, 234324234), '1' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, -1203284, 213121, -234324234), '[]' AS answer;
SELECT slice_it(ROW(1,2,'t',3.1,'x')::ctyp, 1203288, -213120, -234324234), 'x' AS answer;

--
-- checks that None is ignored, and keywords override
--
CREATE OR REPLACE FUNCTION check_transform(OUT i int, OUT i2 int, OUT f float) RETURNS SETOF record LANGUAGE python AS
$python$
def main():
	for x in prepare('select i::int, i::int AS i2, i::float AS f FROM generate_series(1,300) AS g(i)'):
		yield x.transform(None, lambda x: x*2, lambda y: y, f = lambda x: x / 2)
$python$;

SELECT * FROM check_transform() LIMIT 10;

-- kw only
CREATE OR REPLACE FUNCTION check_transform_kw(OUT i int, OUT i2 int, OUT f float) RETURNS SETOF record LANGUAGE python AS
$python$
def main():
	for x in prepare('select i::int, i::int AS i2, i::float AS f FROM generate_series(1,300) AS g(i)'):
		yield x.transform(i2 = lambda x: x * 2)
$python$;

SELECT * FROM check_transform_kw() LIMIT 10;

-- mixed positional and kw
CREATE OR REPLACE FUNCTION check_replace(OUT i int, OUT i2 int, OUT f float) RETURNS SETOF record LANGUAGE python AS
$python$
def main():
	for x in prepare('select i::int, i::int AS i2, i::float AS f FROM generate_series(1,300) AS g(i)'):
		yield x.replace(None, 0, f = 1.2)
$python$;

SELECT * FROM check_replace() LIMIT 10;


-- kw only
CREATE OR REPLACE FUNCTION check_replace_kw(OUT i int, OUT i2 int, OUT f float) RETURNS SETOF record LANGUAGE python AS
$python$
def main():
	for x in prepare('select i::int, i::int AS i2, i::float AS f FROM generate_series(1,300) AS g(i)'):
		yield x.replace(i2 = 0)
$python$;

SELECT * FROM check_replace_kw() LIMIT 10;

-- Other record APIs
CREATE OR REPLACE FUNCTION check_record_map_apis() RETURNS text LANGUAGE python AS
$python$
import Postgres
from Postgres.types import int4, int8, text, numeric, regtype
ctyp = Postgres.Type(regtype('ctyp'))
char = Postgres.Type(Postgres.CONST['CHAROID'])
#	i int,
#	b bigint,
#	t text,
#	n numeric,
#	c "char"

def main():
	x = ctyp((100,10000,'text',1.505,'x'))
	assert x.column_names == ('i','b','t','n','c')
	assert x.keys() is x.column_names
	assert x.values() is x
	assert list(x.items()) == list(zip(x.keys(), x.values()))
	assert x.column_types == (
		int4, int8, text, numeric, char
	)
	assert x.pg_column_types == (
		int4.oid, int8.oid, text.oid, numeric.oid, char.oid
	)
	return 'success'
$python$;

SELECT check_record_map_apis();
