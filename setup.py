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
    install_requires=['redis>=3.0.0'],
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
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    test_suite='walrus.tests',
)
