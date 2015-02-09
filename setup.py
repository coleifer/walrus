import os

from setuptools import find_packages
from setuptools import setup


cur_dir = os.path.dirname(__file__)
readme = os.path.join(cur_dir, 'README.md')
if os.path.exists(readme):
    with open(readme) as fh:
        long_description = fh.read()
else:
    long_description = ''

setup(
    name='walrus',
    version=__import__('walrus').__version__,
    description='walrus',
    long_description=long_description,
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    url='http://github.com/coleifer/walrus/',
    install_requires=['redis'],
    packages=find_packages(),
    package_data={
        'walrus': [
            'scripts/*',
            'stopwords.txt',
        ],
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    test_suite='walrus.tests',
)
