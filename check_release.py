import re
import requests
from pypi_cli import get_package
import sys
import os

pack = get_package('planchet', requests.Session())
versions = set(pack.version_downloads.keys())
text = open(os.path.join(os.path.dirname(__file__), 'setup.py')).read()
version = re.search('version *= *\'([0-9.vab]+)\'', text).group(1)

if version in versions:
    sys.exit(1)
