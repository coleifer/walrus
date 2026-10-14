from setuptools import find_packages
from setuptools import setup

import os
import re
# 2.7 shim.
with open(os.path.join(os.path.dirname(__file__),
                       'walrus/__init__.py'), 'rt') as fh:
    version, = [l for l in fh.readlines() if l.startswith('__version__ = ')]
    version = re.search(r'\'(\d+\.\d+\.\d+)\'', version).groups()[0]

setup(name='walrus',
      version=version,
      install_requires=['redis>=4.0.0'],
      packages=find_packages(),
      package_data={'walrus': ['scripts/*', 'stopwords.txt']})
