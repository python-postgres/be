##
# project.py - project information
##
name = 'pg-python'
abstract = 'PostgreSQL procedural language extension embedding Python 3'

identity = 'http://python.projects.postgresql.org/backend'

meaculpa = 'James William Pye'
contact = 'mailto:x@jwp.io'

license = 'bsd,mit,attribution'

bugtracker = "http://github.com/jwp/pg-python/issues/"

systems = {
	'pg_8_3': 'PostgreSQL 8.3',
	'pg_8_4': 'PostgreSQL 8.4',
	'pg_9_0': 'PostgreSQL 9.0',
	'pg_9_1': 'PostgreSQL 9.1',
	'pg_9_2': 'PostgreSQL 9.2',
}
system_aliases = {
	'pg_8_5' : 'pg_9_0'
}
default_system = 'pg_9_2'

languages = {
	'py_3_1': 'Python 3.1',
	'py_3_2': 'Python 3.2',
	'py_3_3': 'Python 3.3',
}

date = None
tags = set(('features',))
version_info = (1, 1, 0)
version = '.'.join(map(str, version_info)) + (date is None and 'dev' or '')
