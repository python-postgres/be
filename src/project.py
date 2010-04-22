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
}
system_aliases = {
	'pg_8_5' : 'pg_9_0'
}
default_system = 'pg_9_0'

languages = {
	'py_3_1': 'Python 3.1',
}

date = 'Thu Apr 22 04:40:05 MST 2010'
tags = set(('beta',))
version_info = (1, 0, 0)
version = '.'.join(map(str, version_info)) + (date is None and 'dev' or '')
