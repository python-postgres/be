BEGIN;
CREATE SCHEMA __python__;
SET search_path = __python__;

\i ./install.sql

-- Finish with an explicit check.
SAVEPOINT a;
CREATE OR REPLACE FUNCTION test_python() RETURNS text LANGUAGE 'python' AS
$$
import Postgres
import sys
def main():
	return "Python " + sys.version + "\n\nLanguage installed successfully."
$$;
SELECT test_python();
-- Don't want these changes.
ROLLBACK TO a;

COMMIT;
