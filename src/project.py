##
# project.py - project information
##
name = 'pg-python'
abstract = 'PostgreSQL extension providing a Procedural Language and Foreign Data Wrapper accessing Python 3'

identity = 'http://python.projects.postgresql.org/backend'

meaculpa = 'James William Pye'
contact = 'mailto:james.pye@gmail.com'

license = 'bsd,mit,attribution'

bugtracker = "http://github.com/jwp/pg-python/issues/"

systems = {
	'pg_9_0': 'PostgreSQL 9.0',
	'pg_9_1': 'PostgreSQL 9.1',
	'pg_9_2': 'PostgreSQL 9.2',
	'pg_9_3': 'PostgreSQL 9.3',
}
system_aliases = {
	'pg_8_5' : 'pg_9_0'
}
default_system = 'pg_9_3'

languages = {
	'py_3_1': 'Python 3.1',
	'py_3_2': 'Python 3.2',
	'py_3_3': 'Python 3.3',
}

version_info = (1, 1, 1)
version = '.'.join(map(str, version_info))
