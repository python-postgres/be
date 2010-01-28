BEGIN;
CREATE SCHEMA __python__;
SET search_path = __python__;

CREATE FUNCTION
 handler()
RETURNS LANGUAGE_HANDLER LANGUAGE C AS 'python', 'pl_handler';

CREATE FUNCTION
 validator(oid)
RETURNS VOID LANGUAGE C AS 'python', 'pl_validator';

CREATE FUNCTION
 inline(INTERNAL)
RETURNS VOID LANGUAGE C AS 'python', 'pl_inline';

CREATE LANGUAGE python HANDLER "handler" INLINE "inline" VALIDATOR "validator";
COMMIT;
