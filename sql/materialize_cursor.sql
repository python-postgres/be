-- Test srf_materialize_cursor

CREATE OR REPLACE FUNCTION single_column(text) RETURNS SETOF text LANGUAGE python AS
$python$
def main(txt):
	return prepare(txt)
$python$;
SELECT * FROM single_column('SELECT ''foo''::text AS bleh');
SELECT * FROM single_column('SELECT ''foo''::text AS meh UNION ALL SELECT ''bar'' ORDER BY meh;');

CREATE OR REPLACE FUNCTION multi_column(OUT i int, OUT n numeric) RETURNS SETOF RECORD LANGUAGE python AS
$python$
def main():
	return prepare('SELECT 123::int AS i, 4321.234423::numeric AS n')
$python$;

SELECT * FROM multi_column();

-- Getting a bit side-tracked with this next test as it's not materialization.
--
-- The tuple descriptor must be *exactly* the same for the following to work.
-- That is, the iterator produces anonymous records targeting an anonymous
-- record type. In general, when creating a datum from a Postgres.Object, it's
-- preferred to cast, but in this particular case casting is not possible.
SELECT multi_column();

DROP TABLE IF EXISTS no_columns CASCADE;
CREATE TABLE no_columns ();
INSERT INTO no_columns DEFAULT VALUES;
INSERT INTO no_columns DEFAULT VALUES;
INSERT INTO no_columns DEFAULT VALUES;
INSERT INTO no_columns DEFAULT VALUES;

CREATE OR REPLACE FUNCTION no_columns_func() RETURNS SETOF no_columns LANGUAGE python AS
$python$
def main():
	return prepare('SELECT * FROM no_columns')
$python$;

SELECT count(*) FROM no_columns_func();


DROP TABLE IF EXISTS a_single_column CASCADE;
CREATE TABLE a_single_column (i int);
INSERT INTO a_single_column SELECT i FROM generate_series(1, 200) AS g(i);

CREATE OR REPLACE FUNCTION a_single_column_func() RETURNS SETOF a_single_column LANGUAGE python AS
$python$
def main():
	r = prepare('SELECT * FROM a_single_column').rows()
	next(r)
	next(r)
	return r
$python$;

-- 200 - 2
SELECT count(*) FROM a_single_column_func();



DROP TABLE IF EXISTS column_pair CASCADE;
CREATE TABLE column_pair (k int, v text);
INSERT INTO column_pair SELECT i as k, i::text as v FROM generate_series(1, 235) AS g(i);

CREATE OR REPLACE FUNCTION column_pair_func() RETURNS SETOF column_pair LANGUAGE python AS
$python$
def main():
	return prepare('SELECT * FROM column_pair').chunks()
$python$;

SELECT count(*) FROM column_pair_func();


-- and finally, a scroll

CREATE OR REPLACE FUNCTION scrolled_cursor() RETURNS SETOF int LANGUAGE python AS
$python$
def main():
	cur = prepare('SELECT i::int FROM generate_series(1, 223) AS g(i)').declare()
	cur.direction = False
	# go to the end (beginning, with backward direction)
	cur.seek(0, 2)
	# and scroll back 13 rows
	cur.seek(-13, 1)
	return cur
$python$;

SELECT * FROM scrolled_cursor();
