CREATE FOREIGN DATA
__python__."Postgres.FDW:Dictionary"
HANDLER __python__.fdw_handler
VALIDATOR __python__.fdw_validator;

CREATE SERVER dictionaries
FOREIGN DATA WRAPPER "Postgres.FDW:Dictionary";

DO $$
import Postgres
Postgres.testd = {
	'foo': 'bar',
	'bar': 'foo',
}
$$;

CREATE FOREIGN TABLE f_table_name (
  key text,
  value text
) SERVER server
OPTIONS qname 'Postgres.testd';
