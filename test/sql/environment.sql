CREATE OR REPLACE FUNCTION sys() RETURNS text LANGUAGE python AS
$$
import sys

def main():
	assert 'postgres' in sys.executable.lower()
	return 'success'
$$;
SELECT sys();

CREATE OR REPLACE FUNCTION sys_no_argv() RETURNS text LANGUAGE python AS
$$
import sys

def main():
	sys.argv
$$;
SELECT sys_no_argv();
