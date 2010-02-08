#!/usr/bin/env python3
# Generate the requirements.txt file.
import sys
sys.dont_write_bytecode = True
from os.path import dirname, realpath
sys.path.insert(0, dirname(dirname(realpath(__file__))))
import project

req_template = """
============
Requirements
============

This version of pg-python is known to work with the following versions of
PostgreSQL:

{postgres}

And the following versions of Python:

{python}

Python's C-APIs are fairly stable, so newer versions of Python are expected to
normally work. However, PostgreSQL's C-APIs and header files can be adjusted
from minor release to minor release. If the target version of PostgreSQL is not
listed above, it should not be expected to work.
""".strip().format

if __name__ == '__main__':
	sys.stdout.write(req_template(
		postgres = ' - ' + '\n - '.join(sorted(list(project.systems.values()))),
		python = ' - ' + '\n - '.join(sorted(list(project.languages.values())))
	))
