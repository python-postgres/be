CREATE FUNCTION
 "handler"()
RETURNS LANGUAGE_HANDLER LANGUAGE C AS 'python', 'pl_handler';

CREATE FUNCTION
 "validator"(oid)
RETURNS VOID LANGUAGE C AS 'python', 'pl_validator';

CREATE LANGUAGE python HANDLER "handler" VALIDATOR "validator";
CREATE SCHEMA __python__;
