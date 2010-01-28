SET standard_conforming_strings TO ON;

CREATE OR REPLACE FUNCTION check_lo_ops() RETURNS text LANGUAGE python AS
$python$
import Postgres

def test_LargeObject(lo):
	lo.write(b'data')
	size = 4
	lo.seek(0)
	assert lo.read() == b'data'
	assert lo.tell() == size
	lo.write(b'\n')
	lo.write(b'line2\n')
	size = size + 7
	lo.seek(0)
	lo.seek(0, 2)
	assert lo.read() == b''
	assert lo.tell() == size
	lo.seek(-2, 1)
	assert lo.tell() == size - 2
	lo.seek(0, 0)
	assert lo.readline() == b'data\n'
	assert lo.readline() == b'line2\n'
	assert lo.readline() == b''
	lo.seek(0)
	lo.seek(4,1)
	lo.seek(-4,1)
	assert lo.read(4) == b'data'

def main():
	with Postgres.LargeObject.tmp() as lo:
		test_LargeObject(lo)
	return 'success'
$python$;

SELECT check_lo_ops();


CREATE OR REPLACE FUNCTION write_some_lo_data() RETURNS oid LANGUAGE python AS
$python$
import Postgres

def main():
	with Postgres.LargeObject.create() as lo:
		lo.write(b'DATA')
	return lo.oid
$python$;

CREATE OR REPLACE FUNCTION read_some_lo_data(oid) RETURNS text LANGUAGE python AS
$python$
import Postgres

def main(loid):
	with Postgres.LargeObject(loid, mode = 'r') as lo:
		assert lo.read() == b'DATA'
	lo.unlink()
	return 'success'
$python$;


DROP TABLE IF EXISTS dis_lo;
CREATE TABLE dis_lo (o oid);
INSERT INTO dis_lo VALUES (write_some_lo_data());
SELECT read_some_lo_data(o) FROM dis_lo;
