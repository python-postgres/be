DO $$
import Postgres
sqlexec("""
	CREATE TABLE created_from_do (i int);
	INSERT INTO created_from_do VALUES (1004);
""")
$$ LANGUAGE python;
SELECT * FROM created_from_do;

DO $$
import Postgres
Postgres.ERROR("do-raised-error")
$$ LANGUAGE python;

DO $$
import Postgres
x = xact()
x.__enter__()
$$ LANGUAGE python;

-- Now for something more interesting...
DO $$
prepare("""
DO $more$
import Postgres
Postgres.WARNING("sub-do")
$more$ LANGUAGE python;
""")()
$$ LANGUAGE python;

-- Now for something even more interesting...
DO $$
prepare("""
DO $more$
import Postgres
Postgres.ERROR("how does the traceback look?")
$more$ LANGUAGE python;
""")()
$$ LANGUAGE python;
