from setuptools import find_packages
from setuptools import setup

setup(name='walrus',
      install_requires=['redis>=3.0.0'],
      packages=find_packages(),
      package_data={'walrus': ['scripts/*', 'stopwords.txt']})
