DROP TABLE IF EXISTS tupdtest;
CREATE TABLE tupdtest (i int, t text, c circle);

CREATE OR REPLACE FUNCTION describe_tupd(table_name text) RETURNS text
LANGUAGE python AS
$python$
from Postgres.types import regtype
from Postgres import Type

formatting = """
column_names: %s
column_types: %s
pg_column_types: %s
column_count: %d
len(): %d
"""

@pytypes
def main(table_name):
	td = Type(regtype(table_name)).descriptor
	return formatting %(
		td.column_names,
		td.column_types,
		td.pg_column_types,
		td.column_count,
		len(td),
	)
$python$;

SELECT describe_tupd('tupdtest');

ALTER TABLE tupdtest DROP COLUMN t;
SELECT describe_tupd('tupdtest');

ALTER TABLE tupdtest ADD COLUMN t2 text;
SELECT describe_tupd('tupdtest');
