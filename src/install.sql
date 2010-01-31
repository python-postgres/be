BEGIN;
CREATE SCHEMA __python__;
SET search_path = __python__;

CREATE FUNCTION
 "handler"()
RETURNS LANGUAGE_HANDLER LANGUAGE C AS 'python', 'pl_handler';

CREATE FUNCTION
 "validator"(oid)
RETURNS VOID LANGUAGE C AS 'python', 'pl_validator';
COMMIT;

BEGIN;
SET search_path = __python__;
-- It's okay if this fails on versions before 9.0.
CREATE FUNCTION
 "inline"(INTERNAL)
RETURNS VOID LANGUAGE C AS 'python', 'pl_inline';

CREATE LANGUAGE python HANDLER "handler" INLINE "inline" VALIDATOR "validator";
COMMIT;

-- This should not fail if the one above does.
CREATE LANGUAGE python HANDLER "handler" VALIDATOR "validator";

-- Finish with an explicit check.
BEGIN;
CREATE OR REPLACE FUNCTION test_python() RETURNS text LANGUAGE 'python' AS
$$
import Postgres
import sys
def main():
	return "Python " + sys.version + "\n\nLanguage installed successfully."
$$;
SELECT test_python();
-- Don't want these changes.
ABORT;
