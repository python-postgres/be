-- Check that compensation is properly working.
CREATE OR REPLACE FUNCTION just_one() RETURNS int
LANGUAGE python AS
$python$
def main():
	return 1
$python$;

SELECT * FROM just_one() AS g(i) LIMIT 0;
SELECT * FROM just_one() AS g(i);

CREATE OR REPLACE FUNCTION one_two_three() RETURNS SETOF int
LANGUAGE python AS
$python$
def main():
	return (1,2,3)
$python$;

SELECT one_two_three();
SELECT one_two_three() LIMIT 2;
SELECT * FROM one_two_three();
SELECT * FROM one_two_three() LIMIT 2;

CREATE OR REPLACE FUNCTION one_two_three_records(OUT i int, OUT k text) RETURNS SETOF RECORD
LANGUAGE python AS
$python$
def main():
	return (
		(1,'one'),
		(2,'two'),
		(3,'three'),
	)
$python$;

SELECT one_two_three_records();
SELECT one_two_three_records() LIMIT 2;
SELECT * FROM one_two_three_records();
SELECT * FROM one_two_three_records() LIMIT 2;

CREATE OR REPLACE FUNCTION one_two_three_records_d(OUT i int, OUT k text) RETURNS SETOF RECORD
LANGUAGE python AS
$python$
def main():
	return (
		dict(i = 1, k = 'one'),
		dict(i = 2, k = 'two'),
		dict(i = 3, k = 'three'),
		dict(i = 4, k = None),
	)
$python$;

SELECT one_two_three_records_d();
SELECT one_two_three_records_d() LIMIT 2;
SELECT * FROM one_two_three_records_d();
SELECT * FROM one_two_three_records_d() LIMIT 2;
