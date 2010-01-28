CREATE OR REPLACE FUNCTION test_bytes_methods() RETURNS text LANGUAGE python AS
$python$
from Postgres.types import bytea

def main():
	assert bytea(b'foo').islower() is True
	assert bytea(b'FOO').isupper() is True
	assert bytea(b'FoO').isupper() is False
	assert bytea(b'FoO').islower() is False
	assert bytea(' ').isspace() is True
	assert bytea(b'\n').isspace() is True
	assert bytea(b'\t').isspace() is True
	assert bytea(b'X').isspace() is False
	assert bytea(b'X').isalpha() is True
	assert bytea(b'0').isalpha() is False
	assert bytea(b'0').isalnum() is True
	assert bytea(b'0x').isalnum() is True
	assert bytea(b'Title').istitle() is True
	assert bytea(b'itle').istitle() is False
	# manipulations
	assert bytea(b'LOWER').lower() == b'lower'
	assert bytea(b'upper').upper() == b'UPPER'
	assert bytea(b'title').title() == b'Title'
	assert bytea(b'title').capitalize() == b'Title'
	assert bytea(b'SwAp').swapcase() == b'sWaP'
	return 'success, methods'
$python$;

-- Removed these interfaces in favor of saving
-- the method namespace; if these are needed, just make the bytes()...
SELECT test_bytes_methods() WHERE FALSE;

CREATE OR REPLACE FUNCTION test_bytea_interoperability() RETURNS text LANGUAGE python AS
$python$
from Postgres.types import bytea

def main():
	# bytes.join doesn't like using other buffers.. :(
	assert b'.'.join(map(bytes, [bytea(b'foo'), bytea(b'bar'), b'bleh'])) == b'foo.bar.bleh'
	assert b'foo' + bytea(b'bar') == b'foobar'
	assert bytea(b'foo') + b'bar' == b'foobar'
	assert (bytea(b'foo') + b'bar').__class__ is bytea
	return 'success'
$python$;

SELECT test_bytea_interoperability();

CREATE OR REPLACE FUNCTION test_bytea_slicing() RETURNS text LANGUAGE python AS
$python$
from Postgres.types import bytea

py_data = b'a SIMPLE string of \x00 bytes to\x01 validate consistency'
pg_data = bytea(py_data)

# cover various cases
slices = [
	slice(0,0),
	slice(0,1),
	slice(-5,0,-1),
	slice(-5,0,-2),
	slice(10,-1,1),
	slice(10,-1,2),
	slice(0,None),
	slice(None,None),
	slice(None,None, 10),
	slice(5,20,4),
	slice(20,5,-4),
	slice(len(py_data), 0, -1),
	slice(0, len(py_data), len(py_data)//2),
	slice(0, len(py_data), len(py_data)),
	slice(0, len(py_data), len(py_data)+1),
	slice(0, len(py_data), 1000),
]

def main():
	b = bytea(b'bytea data')
	# first, some indexes and simple expectations
	assert b[0] == b'b'
	assert b[1] == b'y'
	assert b[2] == b't'
	assert b[-1] == b'a'
	assert b[-2] == b't'
	assert b[:] == b
	assert b[:] == b'bytea data'
	assert b[0:2] == b'by'
	assert b[2:4] == b'te'
	assert b[3:1:-1] == b'et'

	# now, consistencies with bytes()
	for x in slices:
		assert py_data.__getitem__(x) == pg_data.__getitem__(x), str(x)
	return 'success'
$python$;

SELECT test_bytea_slicing();
