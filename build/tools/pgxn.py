#!/usr/bin/env python3
# Render PGXN META.json file from src/project.py.
import sys
import os
sys.path.insert(0, os.path.realpath('./src'))
import project
import json

data = {
   "name": project.name,
   "abstract": project.abstract,
   "description": "",
   "version": project.version,
   "maintainer": [project.meaculpa],
   "license": "bsd",
   "provides": {
      "python": {
			"name": "python",
         "abstract": "procedural language extension",
         "file": "src/install_inline.sql",
         "version": project.version,
      }
   },
   "resources": {
      "bugtracker": {
         "web": project.bugtracker,
      },
      "repository": {
        "url":  "git://github.com/jwp/pg-python.git",
        "web":  "http://github.com/jwp/pg-python/",
        "type": "git",
      }
   },
   "generated_by": "James William Pye",
   "tags": [
      "python", "pl", "procedural language", "awesome"
   ],
   "meta-spec": {"version": "1.0.0", "url": "http://pgxn.org/meta/spec.txt"},
}

json.dump(data, sys.stdout)
