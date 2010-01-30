CREATE OR REPLACE FUNCTION replaces_self() RETURNS text LANGUAGE python AS
$python$
from Postgres import Stateful

@Stateful
def main():
	yield "foo"
	yield "bar"
$python$;

SELECT i, replaces_self() FROM generate_series(0, 5) AS g(i);


CREATE OR REPLACE FUNCTION replaces_self(i int) RETURNS text LANGUAGE python AS
$python$
from Postgres import Stateful

@Stateful
def main(i):
	yield "bar: %d,%d" % (i, (yield "foo: " + str(i))[0])
$python$;

-- getting new args, right?
SELECT i, replaces_self(i) FROM generate_series(0, 5) AS g(i);


CREATE OR REPLACE FUNCTION scount() RETURNS int LANGUAGE python AS
$python$
from Postgres import Stateful

@Stateful
def main():
	i=0
	while True:
		yield i
		i = i + 1
$python$;

-- simple count-with-me
SELECT scount() AS sc, i FROM generate_series(0, 20-1) AS g(i);


CREATE OR REPLACE FUNCTION srcount(i int) RETURNS int LANGUAGE python AS
$python$
from Postgres import Stateful

@Stateful
def main(i):
	n = 0
	while True:
		n = i + n
		i, = (yield n)
$python$;

-- takes a parameter to work properly
SELECT srcount(i) AS sc, i FROM generate_series(0, 20-1) AS g(i);


CREATE OR REPLACE FUNCTION srcount(i int, j int) RETURNS int LANGUAGE python AS
$python$
from Postgres import Stateful

@Stateful
def main(i, j):
	k = 0
	while True:
		k = k + 1
		i, j = (yield i + j + k)
$python$;

-- takes a couple parameters
SELECT srcount(i, 0) AS sc1, srcount(i, 10) AS sc2, i FROM generate_series(0, 20-1) AS g(i);

-- non-composite stateful function used in materialization

CREATE OR REPLACE FUNCTION mat_stateful() RETURNS text LANGUAGE python AS
$python$
from Postgres import Stateful

@Stateful
def main():
	yield 'should be seen'
	yield 'should not be seen'
$python$;

-- it's not an SRF, but it's being materialized.
-- This test makes sure that something surprising doesn't happen.
SELECT mat_stateful();
SELECT * FROM mat_stateful();

CREATE OR REPLACE FUNCTION yay_stateful() RETURNS text language python as
$$
from Postgres import Stateful

@Stateful
def main():
	yield 'foo'
$$;

CREATE OR REPLACE FUNCTION load_yay_stateful() RETURNS text language python as
$$
def main():
	fn = proc('yay_stateful()')
	fn.load_module()
	return 'fail'
$$;
-- should bomb out; invalid context
SELECT load_yay_stateful();

CREATE OR REPLACE FUNCTION call_yay_stateful() RETURNS text language python as
$$
import sys
def main():
	fn = proc('yay_stateful()')
	assert fn() == 'foo'
	fn_mod = sys.modules[fn.oidstr]
	fn_mod.main()
	return 'fail'
$$;
-- should bomb out; invalid context again
SELECT call_yay_stateful();

-- CORNERS --

-- not really stateful
CREATE OR REPLACE FUNCTION not_stateful() RETURNS int LANGUAGE python AS
$python$
from Postgres import Stateful

@Stateful
def main():
	return "it lied"
$python$;

SELECT not_stateful();

-- not really stateful, but it is an iterator
CREATE OR REPLACE FUNCTION kinda_stateful() RETURNS int LANGUAGE python AS
$python$
from Postgres import Stateful

@Stateful
def main():
	return iter("it lied")
$python$;

SELECT kinda_stateful();


-- SRFs --
-- Not that I would recommend it.
CREATE OR REPLACE FUNCTION stateful_srf() RETURNS SETOF text LANGUAGE python AS
$python$
from Postgres import Stateful

@Stateful
def main():
	yield ("foo","bar")
	yield ("fluffy","bunnies")
$python$;

SELECT stateful_srf() FROM generate_series(1, 2) AS g(i);

-- TRFs --
-- Scary. I know.
CREATE OR REPLACE FUNCTION stateful_trigger() RETURNS TRIGGER LANGUAGE python AS
$python$
from Postgres import NOTICE, Stateful

@Stateful
def before_insert(td, new):
	assert new[0] == 'fluffy'
	td, new = (yield ('foo',))
	assert new[0] == 'bunnies'
	yield ('bar',)
	assert "fail" is True

@Stateful
def before_update(td, old, new):
	NOTICE('foo')
	td, old, new = yield new
	NOTICE('bar')
	yield new
$python$;

DROP TABLE IF EXISTS stateful_trigger_table CASCADE;
CREATE TABLE stateful_trigger_table(t text);
CREATE TRIGGER stateful_trigger_trigger
 BEFORE INSERT OR UPDATE ON stateful_trigger_table
 FOR EACH ROW
 EXECUTE PROCEDURE stateful_trigger();

BEGIN;
INSERT INTO stateful_trigger_table VALUES ('fluffy'), ('bunnies');
SELECT * FROM stateful_trigger_table;
UPDATE stateful_trigger_table SET t = NULL;
SELECT COUNT(t IS NULL) AS count FROM stateful_trigger_table;
COMMIT;
