##
# Meta data source.
#
# This is separated from module.py in order to allow 2.x to import it. (sphinx)
__author__ = 'James William Pye'
__project__ = 'pg-python'
__date__ = '2010-02-01'

__systems__ = {
	'pg_8_3': 'PostgreSQL 8.3',
	'pg_8_4': 'PostgreSQL 8.4',
	'pg_9_0': 'PostgreSQL 9.0',
}
__system_aliases__ = {
	'pg_8_5' : 'pg_9_0'
}
__default_system__ = 'pg_9_0'

__languages__ = {
	'py_3_1': 'Python 3.1',
}

__version_info__ = (1, 0, 0, 'dev', 0)
__version__ = '.'.join(map(str, __version_info__[:3])) + (
	__version_info__[3] if __version_info__[3] != 'final' else ''
)
