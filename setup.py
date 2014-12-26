from setuptools import setup

setup(
    name='walrus',
    version=__import__('walrus').__version__,
    description='walrus',
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    url='http://github.com/coleifer/',
    py_modules=['walrus'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    test_suite='walrus',
)
