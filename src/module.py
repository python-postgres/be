##
# Pure-python part of the built-in Postgres module
##
__file__ = '[Postgres]'
import sys
import contextlib
import io

class StringModule(object):
	"""
	Used represent the pure-Python Postgres and Postgres.project modules.
	"""
	from types import ModuleType

	def __init__(self, name, src):
		self.name = name
		self.source = src

	def get_source(self, *args):
		return self.source()

	def get_code(self, *args, compile = __builtins__.compile):
		return compile(self.source(), '['+ self.name +']', 'exec')

	def load_module(self, *args, eval = __builtins__.eval):
		if self.name in sys.modules:
			return sys.modules[self.name]
		module = self.ModuleType('<' + self.name + '>')
		module.__builtins__ = __builtins__
		module.__name__ = self.name
		module.__file__ = '[' + self.name + ']'
		module.__loader__ = self
		sys.modules[self.name] = module
		try:
			eval(self.get_code(), module.__dict__, module.__dict__)
		finally:
			del sys.modules[self.name]
		return module
__loader__ = StringModule('Postgres', __get_Postgres_source__)
project = StringModule('Postgres.project', __get_Postgres_project_source__)
project = project.load_module()

# Don't add anything here unless you don't mind updating the expected output.
severities = dict([
	(k, CONST[k]) for k in (
		"DEBUG5",
		"DEBUG4",
		"DEBUG3",
		"DEBUG2",
		"DEBUG1",
		"LOG",
		"COMMERROR",
		"INFO",
		"NOTICE",
		"WARNING",
		"ERROR",
		"FATAL",
		"PANIC",
	)
])

def DEBUG(*args, code = None, _mkstate = make_sqlstate, _report = ereport, _sev = severities["DEBUG1"], **kw):
	if code is not None:
		kw.setdefault('sqlerrcode', _mkstate(code))
	return _report(_sev, *args, **kw)
def LOG(*args, code = None, _mkstate = make_sqlstate, _report = ereport, _sev = severities["LOG"], **kw):
	if code is not None:
		kw.setdefault('sqlerrcode', _mkstate(code))
	return _report(_sev, *args, **kw)
def INFO(*args, code = None, _mkstate = make_sqlstate, _report = ereport, _sev = severities["INFO"], **kw):
	if code is not None:
		kw.setdefault('sqlerrcode', _mkstate(code))
	return _report(_sev, *args, **kw)
def NOTICE(*args, code = None, _mkstate = make_sqlstate, _report = ereport, _sev = severities["NOTICE"], **kw):
	if code is not None:
		kw.setdefault('sqlerrcode', _mkstate(code))
	return _report(_sev, *args, **kw)
def WARNING(*args, code = None, _mkstate = make_sqlstate, _report = ereport, _sev = severities["WARNING"], **kw):
	if code is not None:
		kw.setdefault('sqlerrcode', _mkstate(code))
	return _report(_sev, *args, **kw)
def ERROR(*args, code = None, _mkstate = make_sqlstate, _report = ereport, _sev = severities["ERROR"], **kw):
	if code is not None:
		kw.setdefault('sqlerrcode', _mkstate(code))
	return _report(_sev, *args, **kw)
def FATAL(*args, code = None, _mkstate = make_sqlstate, _report = ereport, _sev = severities["FATAL"], **kw):
	if code is not None:
		kw.setdefault('sqlerrcode', _mkstate(code))
	return _report(_sev, *args, **kw)

##
# Override for warnings.showwarning
# (emit using ereport)
def show_python_warning(message, category, filename,
	lineno, file=None, line=None,
	_warn_ = WARNING, _state_ = make_sqlstate('01PPY')
):
	if category.__module__ == 'builtins':
		mod = ''
	else:
		mod = category.__module__ + '.'
	ctx = '%s:%s: %s%s' %(filename, lineno, mod, category.__name__)
	_warn_(message, context = ctx, sqlerrcode = _state_)
import warnings
warnings.showwarning = show_python_warning
del warnings

class InlineExecutor(object):
	"""
	Used to execute code from DO-statements.
	"""
	from types import ModuleType
	_current_id = 0

	def __init__(self, src):
		self.__class__._current_id = self.id = self.__class__._current_id + 1
		self.source = src

	def get_source(self, *args):
		return self.source

	def get_code(self, *args, compile = __builtins__.compile):
		return compile(self.source, '[do-block-%d]' % self.id, 'exec')

	def load_module(self, *args, eval = __builtins__.eval):
		if not hasattr(self, 'module'):
			self.module = self.ModuleType('<DO-statement-block>')
			self.module.__builtins__ = __builtins__
			self.module.__loader__ = self
			eval(self.get_code(), self.module.__dict__, self.module.__dict__)
		return self.module

	@classmethod
	def main(typ, prosrc):
		l = typ(str(prosrc))
		l.load_module()

def _return_arg(arg):
	return arg

_preload_get_procs = """
SELECT
	pg_proc.oid
FROM
	pg_catalog.pg_proc, pg_catalog.pg_namespace
WHERE
	pg_proc.pronamespace = pg_namespace.oid AND
	pg_proc.prolang = $1 AND
	pg_namespace.nspname = $2
"""
def preload(*args, pg_language_oid = None):
	"""
	Preload all the Python functions in the specified schemas.
	"""
	if pg_language_oid is None:
		# has to be done at runtime; __get_func__().language
		# results can vary.
		lanoid = __get_func__().language
	else:
		lanoid = pg_language_oid
	for x in args:
		funcs = map(lambda y: Function(y[0]),
			Statement(_preload_get_procs, lanoid, x))
		for z in funcs:
			z.load_module()

##
# Override stdio objects to give the client information
# about what is happening. Likely, stdio operations indicate
# a bug as they normally don't make sense in the backend context.
##
class StandardOuts(object):
	def __init__(self, title):
		self.title = title
		self._msg = "data written to " + self.title

	def isatty(self):
		return False

	def close(self):
		pass

	def write(self, data):
		# if it's just spaces ignore it.
		# probably a newline that isn't necessary.
		if data.isspace():
			return
		NOTICE(self._msg, detail = data)
sys.stdout = StandardOuts('sys.stdout')
sys.stderr = StandardOuts('sys.stderr')

class StandardIn(object):
	def __init__(self, title):
		self.title = title

	def read(self, *args, **kw):
		raise RuntimeError(
			"cannot read from " + self.title + " in Postgres backend context"
		)
sys.stdin = StandardIn("sys.stdin")

##
# Provides a built-in decorator for converting Postgres.Object instances
# to naturally corresponding Python instances.
#
# @pytypes
# def main(...):
#  ...
#
_pytypes_map = {
	CONST['BYTEAOID'] : bytes,
	CONST['BOOLOID'] : bool,
	CONST['INT2OID'] : int,
	CONST['INT4OID'] : int,
	CONST['INT8OID'] : int,
	CONST['FLOAT4OID'] : float,
	CONST['FLOAT8OID'] : float,
	CONST['CSTRINGOID'] : str,
	CONST['TEXTOID'] : str,
	CONST['VARCHAROID'] : str,
	CONST['CHAROID'] : str,
	CONST['BPCHAROID'] : str,
}
def convert_postgres_objects(seq, get_converter = _pytypes_map.get):
	"""
	Convert all the given Postgres objects in the sequence to
	corresponding Python objects.

	This only supports a handful of built-ins.
	"""
	return tuple([
		# If it's a Postgres.Type, use the Base.oid as the key.
		get_converter(
			(x.__class__.__class__ is Type and x.__class__.Base.oid or 0),
			_return_arg)(x)
		for x in seq
	])

from functools import partial
iterpytypes = partial(map, convert_postgres_objects)

class pytypes(tuple):
	def __new__(typ, ob):
		return super().__new__(typ, (ob,))
	def __call__(self, *args, **kw):
		return self[0](*convert_postgres_objects(args), **kw)

def eval(sql, *args):
	return Statement('SELECT (' + str(sql) + ');', *args).first()

class Types(object):
	__name__ = 'Postgres.types'
	__doc__ = 'types module emulator'
	__path__ = None
	__regtype = None

	def __init__(self):
		self.__regtype = Type(CONST["REGTYPEOID"])

	def __getattr__(self, attname):
		if attname.startswith('__'):
			return super(self).__getattr__(self, attname)
		try:
			return Type(self.__regtype('pg_catalog.' + attname))
		except:
			raise AttributeError("could not create type instance")

##
# Build version information.
version = CONST['PG_VERSION_STR']
# Version tuple, like Python's sys.version_info
vstr = CONST['PG_VERSION']
_version_state = vstr.strip('.0123456789')
_level = 0
if _version_state:
	vstr, _level = vstr.split(_version_state)
	_level = int(_level or '0')
	vi_parts = vstr.split('.')
else:
	_version_state = 'final'
	vi_parts = vstr.split('.')
version_info = tuple(map(int, vi_parts)) + (
	(0,) if len(vi_parts) == 2 else ()
) + (_version_state, _level)
del _level, vstr, _version_state, vi_parts

# No reason for this to be a Postgres.Exception subclass
class StopEvent(BaseException):
	"""
	Raised by a user when a trigger event should be suppressed.
	"""

class Exception(Exception):
	"""
	Standard Postgres exception.

	Raised with the 'pg_errordata' attribute set to a Postgres.ErrorData instance
	when a Postgres ERROR occurs.
	"""
	_pg_ed_atts = (
		'detail',
		'context',
		'hint',
		'position',
		'internal_position',
		'internal_query',
		'errno',
		'filename',
		'line',
		'function',
	)

	@property
	def details(self):
		return {
			k : v for k, v in (
				(k, getattr(self.pg_errordata, k, None))
				for k in self._pg_ed_atts
			) if v
		}

	@property
	def errno(self):
		return self.pg_errordata.errno

	@property
	def severity(self):
		return self.pg_errordata.severity

	@property
	def code(self):
		return self.pg_errordata.code

	@property
	def message(self):
		return self.pg_errordata.message

	def __init__(self, pg_errordata = None):
		self.pg_errordata = pg_errordata

	def __str__(self):
		# In situations where an instance is the __context__
		# or __cause__ of the fully raised exception, print
		# out all the fields in ErrorData.
		#
		# In the chained exception case, we will need a fully
		# print-out of the information.
		if getattr(self, 'pg_errordata', None) is not None \
		and getattr(self, '_pg_inhibit_str', False) is not True:
			ed = self.pg_errordata
			s = ed.message + '\nCODE: ' + ed.code
			if ed.detail:
				s = s + '\nDETAIL: ' + ed.detail
			if ed.context:
				s = s + '\nCONTEXT: ' + ed.context
			if ed.hint:
				s = s + '\nHINT: ' + ed.hint
			return s
		else:
			return ''

class LargeObject(io.IOBase):
	"""
	Python interface to Postgres LargeObjects.

	Depends on the the _lo_* built-ins created by the C-portion of the Postgres
	module.
	"""
	_INV_READ = CONST["INV_READ"]
	_INV_WRITE = CONST["INV_WRITE"]

	closed = None

	def fileno(self):
		raise IOError("LargeObject's do not have an underlying file descriptor")

	def isatty(self):
		return False

	def flush(self):
		pass

	def seekable(self):
		return True

	def readable(self):
		return 'r' in self.mode

	def writable(self):
		return 'w' in self.mode

	def __repr__(self):
		return '''<%s large object '%d' mode '%s' at %s>''' % (
			self.closed and 'closed' or 'open',
			int(self.oid), self.mode, hex(id(self)),
		)

	@classmethod
	def create(cls):
		oid = _lo_create()
		return cls(oid, mode = 'rw')

	@classmethod
	def tmp(cls):
		oid = _lo_create()
		return cls(oid, mode = 'rwt')

	def __init__(self, oid, mode = 'r'):
		mc = 0
		for m in mode:
			if m == 'r':
				mc |= self._INV_READ
			elif m == 'w':
				mc |= self._INV_WRITE
			elif m == 't':
				pass
			else:
				raise IOError('invalid mode request ' + repr(m))
		mode = ''.join(set(mode))

		self.lod = _lo_open(oid, mc)
		self.mode = mode
		self.oid = oid
		self.closed = False

	def read(self, quantity = None):
		if self.closed is not False:
			raise ValueError('operation on closed LargeObject')
		lod = self.lod
		if quantity is None:
			# read everything
			string = io.BytesIO()
			data = _lo_read(lod, 1024)
			while len(data) == 1024:
				string.write(data)
				data = _lo_read(lod, 1024)
			string.write(data)
			string.seek(0)
			return string.read()
		# otherwise, directly read the specified amount
		return _lo_read(lod, quantity)

	def readline(self):
		if self.closed is not False:
			raise ValueError('operation on closed LargeObject')

		lod = self.lod
		line_data = b''
		data = _lo_read(lod, 100)
		while not b'\n' in data:
			line_data = line_data + data
			data = _lo_read(lod, 100)
			if not data:
				break
		line_data = line_data + data
		nloffset = line_data.find(b'\n')
		if nloffset != -1:
			# seek relative, back to the nloffset
			self.seek(-(len(line_data) - nloffset - 1), 1)
			# include the newline
			return line_data[:nloffset+1]
		else:
			return line_data

	def __iter__(self):
		return self

	def __next__(self):
		r = self.readline()
		if r:
			return r
		raise StopIteration

	def write(self, data):
		if self.closed is not False:
			raise ValueError('operation on closed LargeObject')
		return _lo_write(self.lod, data)

	def tell(self):
		if self.closed is not False:
			raise ValueError('operation on closed LargeObject')
		return _lo_tell(self.lod)

	def seek(self, offset, whence = 0):
		if self.closed is not False:
			raise ValueError('operation on closed LargeObject')
		return _lo_seek(self.lod, offset, whence)

	def close(self):
		if not self.closed:
			_lo_close(self.lod)
			self.closed = True
			if 't' in self.mode:
				self.unlink()

	def unlink(self):
		if not self.closed:
			self.close()
		_lo_unlink(self.oid)

##
# Internal functions.
##

def _clearfunccache():
	import sys
	rm = []
	for k in sys.modules:
		if k.isdigit():
			fm = sys.modules[k]
			if getattr(fm, '__func__', False):
				if fm.__func__.__class__ is Function:
					rm.append(k)
	for k in rm:
		del sys.modules[k]

def clearcache():
	_clearfunccache()
	_cleartypecache()
	try:
		import linecache
		linecache.clearcache()
	except ImportError:
		# It's unlikely, but Python's stdlib... :(
		pass

# used by _pl_reload() to identify modules
# that should not be removed from sys.modules on reloads
_original_modules = set(sys.modules.keys())
_original_modules.add('Postgres')

# called the first time the language is invoked to finalize the module/env
def _pl_first_call():
	try:
		# Is the ServerEncoding usable?
		'1234567890'.encode(encoding)
	except:
		raise RuntimeError("server encoding not recognized by Python")

	global types
	types = Types()
	sys.modules['Postgres.types'] = types

	def proc(proid, _regproc = Type(CONST["REGPROCEDUREOID"])):
		if (proid.__class__ is not int):
			proid = _regproc(proid)
		return Function(proid)

	global sleep, cancel_backend, terminate_backend
	sleep = proc('pg_catalog.pg_sleep(double precision)')
	cancel_backend = proc('pg_catalog.pg_cancel_backend(int4)')
	if version_info[:2] >= (8,4):
		terminate_backend = proc('pg_catalog.pg_terminate_backend(int4)')

	##
	# Initialize the common built-in aliases.
	__builtins__.pytypes = pytypes
	__builtins__.xact = Transaction
	__builtins__.proc = proc
	__builtins__.prepare = Statement
	__builtins__.sqleval = eval
	__builtins__.sqlexec = execute

# execute the init.py file relative to the cluster
def _pl_local_init(initfile = "init.py", eval = __builtins__.eval):
	from types import ModuleType
	import os.path
	if os.path.exists(initfile):
		# XXX: Do permission check on init.py
		with open(initfile) as init_file:
			bc = compile(init_file.read(), initfile, 'exec')
			module = ModuleType('__pg_init__')
			module.__file__ = initfile
			module.__builtins__ = __builtins__
			eval(bc, module.__dict__, module.__dict__)
		sys.modules['__pg_init__'] = module
		LOG('loaded Python module "__pg_init__" (init.py)')

# clear non-default modules and run the init.py again
def _pl_reload():
	# XXX: NOT USED
	r = set(sys.modules.keys()) - _original_modules
	for x in r:
		del sys.modules[x]
	_pl_local_init()

# Clear the linecache in order to avoid
# situations where a stale entry exists.
# This helps ensure that the common case of repeat CREATE OR REPLACE's
# show the right lines when they blow up--incremental corrections.
def _pl_eox():
	try:
		import linecache
		linecache.clearcache()
	except (ImportError, AttributeError):
		# ignore if linecache doesn't exist
		pass
