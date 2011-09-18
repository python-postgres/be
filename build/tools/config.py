import sys
import os
import os.path
import uuid
try:
	import distutils.sysconfig as sysconfig
except ImportError:
	try:
		import sysconfig
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
emit('python_version', str(sys.version_info[0]) + str(sys.version_info[1]))
emit('python_ref', 'python' + str(sys.version_info[0]) + str(sys.version_info[1]))

emit('python_cflags', ' '.join([
		'-I' + sysconfig.get_python_inc(),
	]
))

emit('project_version', project.version)

version = sysconfig.get_config_vars('VERSION')[0]
libdir = sysconfig.get_config_vars('LIBDIR')[0]

##
# XXX: Look for pythonX.Ym
# The OSX distribution appears to use the 'm' version by default,
# so if there is no m version in the library directory, assume the
# sans 'm' library is what we want.
libpythonm = 'python' + version + 'm'
for x in os.listdir(libdir):
	if libpythonm in x:
		# We found libpythonX.Ym.so in the LIBDIR.
		libpy = '-l' + libpythonm
		break
else:
	libpy = '-lpython' + version

emit('python_ldflags', ' '.join([
		'-L' + libdir,
		libpy,
	] + sysconfig.get_config_vars('SHLIBS', 'SYSLIBS', 'LDFLAGS')
))
