import unittest
import time
from postgresql.exceptions import QueryCanceledError
from postgresql.temporal import pg_tmp

mklang_90 = """
CREATE SCHEMA __python__;
SET search_path = __python__;

CREATE FUNCTION
 "handler"()
RETURNS LANGUAGE_HANDLER LANGUAGE C AS 'python', 'pl_handler';

CREATE FUNCTION
 "validator"(oid)
RETURNS VOID LANGUAGE C AS 'python', 'pl_validator';

CREATE FUNCTION
 "inline"(INTERNAL)
RETURNS VOID LANGUAGE C AS 'python', 'pl_inline';

CREATE LANGUAGE python HANDLER "handler" INLINE "inline" VALIDATOR "validator";
"""

mklang_83 = """
CREATE SCHEMA __python__;
SET search_path = __python__;

CREATE FUNCTION
 "handler"()
RETURNS LANGUAGE_HANDLER LANGUAGE C AS 'python', 'pl_handler';

CREATE FUNCTION
 "validator"(oid)
RETURNS VOID LANGUAGE C AS 'python', 'pl_validator';

-- This should not fail if the one above does.
CREATE LANGUAGE python HANDLER "handler" VALIDATOR "validator";
"""

infinite_loop = """
CREATE OR REPLACE FUNCTION
public.iloop() RETURNS int LANGUAGE python AS
$$
import Postgres

def main():
	Postgres.WARNING('doint')
	while True:
		pass
	return -1
$$;
"""

infinite_loop_in_subxact = """
CREATE OR REPLACE FUNCTION
public.iloop_in_subxact()
RETURNS int LANGUAGE python AS
$$
import Postgres

def main():
	with xact():
		Postgres.WARNING('doint')
		while True:
			pass
	return -1
$$;
"""

infinite_loop_in_failed_subxact = """
CREATE OR REPLACE FUNCTION
public.iloop_in_failed_subxact()
RETURNS int LANGUAGE python AS
$$
import Postgres

def main():
	with xact():
		try:
			prepare('selekt 1')
		except Exception:
			pass
		Postgres.WARNING('doint')
		while True:
			pass
	return -1
$$;
"""

return_one = """
CREATE OR REPLACE FUNCTION
public.return_one()
RETURNS int LANGUAGE python AS
$$
def main():
	return 1
$$;
"""

call_iloops = """
CREATE OR REPLACE FUNCTION
public.call_iloops()
RETURNS int LANGUAGE python AS
$$
import Postgres
functions = [
	proc('public.iloop()'),
	proc('public.iloop_in_subxact()'),
	proc('public.iloop_in_failed_subxact()'),
]

def main():
	for x in functions:
		try:
			with xact():
				x()
		except KeyboardInterrupt:
			pass
		except Postgres.Exception as err:
			if not err.code.startswith('57'):
				raise
	return 1
$$;
"""

funcs = [
	infinite_loop,
	infinite_loop_in_subxact,
	infinite_loop_in_failed_subxact,
	return_one,
	call_iloops,
]

xfuncs = [
	"SELECT iloop();",
	"SELECT iloop_in_subxact();",
	"SELECT iloop_in_failed_subxact();",
]

class test_interrupt(unittest.TestCase):
	def hook(self, msg):
		if msg.message == 'doint':
			db.interrupt()
			return True # suppress

	@pg_tmp
	def testInterrupt(self):
		db.msghook = self.hook
		for x in xfuncs:
			# Ran inside a block.
			self.failUnlessRaises(QueryCanceledError, sqlexec, x)
			# Connection should be usable now.
			self.failUnlessEqual(prepare('select 1').first(), 1)

	@pg_tmp
	def testInterruptInBlock(self):
		db.msghook = self.hook
		for x in xfuncs:
			# Ran inside a block.
			try:
				with xact():
					sqlexec(x)
			except QueryCanceledError:
				pass
			# Connection should be usable now.
			self.failUnlessEqual(prepare('select 1').first(), 1)

	@pg_tmp
	def testInterruptInSubxact(self):
		db.msghook = self.hook
		# Ran inside a block.
		for x in xfuncs:
			with xact():
				try:
					with xact():
						sqlexec(x)
				except QueryCanceledError:
					pass
				self.failUnlessEqual(prepare('select 1').first(), 1)
		self.failUnlessEqual(prepare('select 1').first(), 1)

	@pg_tmp
	def testInterruptBeforeUse(self):
		# In order to implement interrupt support,
		# the signal handlers are overridden.
		# This means that it is possible to set an interrupt
		# while outside of the PL. Exercise that case.
		db.msghook = self.hook
		with xact():
			try:
				# not actually testing anything here;
				# rather, we need 'handler_count > 0'.
				with xact():
					sqlexec(xfuncs[0])
			except QueryCanceledError:
				pass
			db.interrupt()
			time.sleep(0.1)
			sqlexec("SELECT return_one();")

	@pg_tmp
	def testInterruptWithinUse(self):
		# In order to implement interrupt support,
		# the signal handlers are overridden.
		# This means that it is possible to set an interrupt
		# while outside of the PL. Exercise that case.
		db.msghook = self.hook
		sqlexec("SELECT call_iloops();")
		with xact():
			sqlexec("SELECT call_iloops();")

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	with pg_tmp:
		if db.version_info[:2] < (8,5):
			sqlexec(mklang_83)
		else:
			sqlexec(mklang_90)
		for x in funcs:
			sqlexec(x)
	unittest.main(this)
