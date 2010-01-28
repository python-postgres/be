DROP SCHEMA IF EXISTS nsp_a CASCADE;
DROP SCHEMA IF EXISTS nsp_b CASCADE;
CREATE SCHEMA nsp_a;
CREATE SCHEMA nsp_b;

CREATE OR REPLACE FUNCTION nsp_a.foo() RETURNS VOID LANGUAGE python AS
$$
def main():
	pass
$$;

CREATE OR REPLACE FUNCTION nsp_b.foo() RETURNS VOID LANGUAGE python AS
$$
def main():
	pass
$$;

CREATE OR REPLACE FUNCTION load_em() RETURNS VOID LANGUAGE python AS
$$
import sys
import Postgres

def main():
	p_a = proc('nsp_a.foo()')
	p_b = proc('nsp_b.foo()')

	assert p_a.oidstr not in sys.modules
	assert p_b.oidstr not in sys.modules

	Postgres.preload('nsp_a', 'nsp_b')

	assert p_a.oidstr in sys.modules
	assert p_b.oidstr in sys.modules

	Postgres.NOTICE('success')
$$;
SELECT load_em();
