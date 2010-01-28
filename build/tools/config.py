import sys
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

format = "{0} = {1!s}\n".format
def emit(name, value):
	sys.stdout.write(format(name, value))

if sys.platform in ('win32', 'win64'):
	emit('target_system', 'msw')
else:
	# ASSume.
	emit('target_system', 'posix')

emit('uuid', str(uuid.uuid1()))
emit('python', sys.executable)
emit('python_version', str(sys.version_info[0]) + str(sys.version_info[1]))
emit('python_ref', 'python' + str(sys.version_info[0]) + str(sys.version_info[1]))

emit('python_cflags', ' '.join([
		'-I' + sysconfig.get_python_inc(),
	]
))

emit('python_ldflags', ' '.join([
		'-L' + sysconfig.get_config_vars('LIBDIR')[0],
		'-lpython' + sysconfig.get_config_vars('VERSION')[0],
	] + sysconfig.get_config_vars('SHLIBS', 'SYSLIBS', 'LDFLAGS')
))
