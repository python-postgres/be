import sys
import os
import os.path
import uuid
try:
	import sysconfig
except ImportError:
	try:
		import distutils.sysconfig as sysconfig
	except ImportError:
		sys.stderr.write("ERROR: no sysconfig module available\n")
		sys.stderr.write("HINT: Try installing a Python 'dev' package.\n")
		sys.exit(1)
sys.path.insert(0, os.path.realpath('./src'))
import project

format = "{0} = {1!s}\n".format
def emit(name, value):
	sys.stdout.write(format(name, value))

if sys.platform in ('win32', 'win64'):
	emit('target_system', 'msw')
else:
	# ASSume.
	emit('target_system', 'posix')

# build identifier (unused)
emit('uuid', str(uuid.uuid1()))
emit('python', sys.executable)

pyversion = sysconfig.get_config_var('VERSION')
emit('python_version', pyversion)
pyabi = sysconfig.get_config_var('ABIFLAGS') or ''
emit('python_abi', pyabi)

pyspec = 'python' + pyversion + pyabi
emit('python_cflags', ' '.join([
		'-I' + sysconfig.get_config_var('INCLUDEPY'),
	]
))

emit('project_version', project.version)

libdir = sysconfig.get_config_var('LIBDIR')
libpy = '-l' + pyspec

emit('python_ldflags', ' '.join([
		'-L' + libdir, libpy,
	] + sysconfig.get_config_vars('SHLIBS', 'SYSLIBS', 'LDFLAGS')
))
