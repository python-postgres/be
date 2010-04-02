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

funcs = [
	infinite_loop,
	infinite_loop_in_subxact,
	infinite_loop_in_failed_subxact,
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
