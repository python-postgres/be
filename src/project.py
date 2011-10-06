##
# project.py - project information
##
name = 'pg-python'
identity = 'http://python.projects.postgresql.org/backend'
author = 'James William Pye <x@jwp.name>'

systems = {
	'pg_8_3': 'PostgreSQL 8.3',
	'pg_8_4': 'PostgreSQL 8.4',
	'pg_9_0': 'PostgreSQL 9.0',
	'pg_9_1': 'PostgreSQL 9.1',
}
system_aliases = {
	'pg_8_5' : 'pg_9_0'
}
default_system = 'pg_9_1'

languages = {
	'py_3_1': 'Python 3.1',
	'py_3_2': 'Python 3.2',
}

date = 'Sat Sep 24 12:00:00 2011'
tags = set(('bugs','parity'))
version_info = (1, 0, 1)
version = '.'.join(map(str, version_info)) + (date is None and 'dev' or '')
