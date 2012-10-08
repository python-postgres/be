-- PL Support

CREATE FUNCTION
"pl_handler" ()
RETURNS LANGUAGE_HANDLER LANGUAGE C AS 'python';

CREATE FUNCTION
"pl_validator" (OID)
RETURNS VOID LANGUAGE C AS 'python';

CREATE FUNCTION
"pl_inline" (INTERNAL)
RETURNS VOID LANGUAGE C AS 'python';

CREATE LANGUAGE python HANDLER "pl_handler" INLINE "pl_inline" VALIDATOR "pl_validator";
