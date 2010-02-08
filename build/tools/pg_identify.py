# identify the [database] system that we're targeting the build for.
import sys
sys.dont_write_bytecode = True
import os
from os.path import dirname, realpath, join as joinpath

ls = os.linesep
err = sys.stderr.write
out = sys.stdout.write

err(' reading [database] system version from stdin...' + ls)
pg_version = sys.stdin.readline()

sysname, sysversion, *unknown = pg_version.split()
if unknown:
	err('WARNING: unexpected data trailing the system version' + ls)

err(' SYSTEM NAME: ' + sysname + ls)
err(' VERSION: ' + sysversion + ls)

major, minor, *insignificant = sysversion.split('.')
if not minor.isdigit():
	i=0
	while minor[i].isdigit():
		i = i + 1
	minor = minor[:i]

sysid = '_'.join(('pg', major, minor))

# read project metadata, assume we're in the configure directory
exec(open(joinpath('src', 'project.py')).read())

# system_aliases, systems, and default_system import from project.py

rsysid = system_aliases.get(sysid, sysid)
err(' IDENTITY: {0} -> {1}{2}'.format(sysid, rsysid, ls))
sysid = rsysid

if sysid not in systems:
	err('WARNING: identified system, {0}, is not in the supported list{1}'.format(sysid, ls))
	err('SUPPORTED: ' + ' '.join(systems.keys()) + ls)
	err(' trying to use default: ' + default_system)
	sysid = default_system

out(sysid + ls)
