-- test a few Postgres.types.oid interfaces and reg* relationships.

CREATE OR REPLACE FUNCTION check_oid(oid, OUT typname text, OUT valrepr text) RETURNS SETOF record LANGUAGE python AS
$python$
from Postgres.types import oid, regproc, regprocedure, regtype, regclass, regoper, regoperator

regtypes = (
	regproc, regprocedure, regtype, regclass, regoper, regoperator
)

def main(v):
	assert isinstance(v, oid)
	yield 'oid', str(v)
	for x in regtypes:
		rv = x(v)
		assert isinstance(rv, oid)
		assert isinstance(rv, x)
		# By subclassing PyPg_oid_Type, reg*'s get __int__ methods.
		assert int(rv) == int(v)
		# it can go back too, right?
		assert oid(rv) == v
		yield str(x.typname), str(x(v))
$python$;

SELECT * FROM check_oid(0);
SELECT * FROM check_oid(1);

-- Don't show the digits in these cases in order to avoid
-- updating the expected output. Already likely enough to break,
-- so try to decrease the frequency.
SELECT * FROM check_oid('check_oid'::regproc) WHERE NOT (valrepr ~* '[0-9]+') ORDER BY typname;
SELECT * FROM check_oid('=(integer,bigint)'::regoperator) WHERE NOT (valrepr ~* '[0-9]+') ORDER BY typname;
