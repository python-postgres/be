# -*- coding: utf-8 -*-
import sys, os
sys.dont_write_bytecode = True
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# General configuration
# ---------------------

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = []

# Add any paths that contain templates here, relative to this directory.
#templates_path = []

# The suffix of source filenames.
source_suffix = '.txt'

# The master toctree document.
master_doc = 'index'

# General substitutions.
import project as proj
project = proj.name
copyright = '2010, ' + proj.meaculpa

# The default replacements for |version| and |release|, also used in various
# other places throughout the built documents.
#
# The short X.Y version.
version = '.'.join(map(str,proj.version_info[:2]))
# The full version, including alpha/beta/rc tags.
release = proj.version

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
#today = ''
# Else, today_fmt is used as the format for a strftime call.
today_fmt = '%B %d, %Y'

# List of documents that shouldn't be included in the build.
#unused_docs = []

# List of directories, relative to source directories, that shouldn't be searched
# for source files.
#exclude_dirs = []

# The reST default role (used for this markup: `text`) to use for all documents.
default_role = None
add_function_parentheses = True
pygments_style = 'sphinx'
html_style = 'default.css'
