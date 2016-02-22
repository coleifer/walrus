import os
import sys


PY3 = sys.version_info[0] == 3

if PY3:
    unicode_type = str
    basestring_type = (str, bytes)
else:
    unicode_type = unicode
    basestring_type = basestring


def encode(s):
    if PY3 and isinstance(s, unicode_type):
        return s.encode('utf-8')
    return s


def decode(s):
    if PY3 and isinstance(s, bytes):
        return s.decode('utf-8')
    return s


class memoize(dict):
    def __init__(self, fn):
        self._fn = fn

    def __call__(self, *args):
        return self[args]

    def __missing__(self, key):
        result = self[key] = self._fn(*key)
        return result


@memoize
def load_stopwords(stopwords_file):
    path, filename = os.path.split(stopwords_file)
    if not path:
        path = os.path.dirname(__file__)
    filename = os.path.join(path, filename)
    if not os.path.exists(filename):
        return

    with open(filename) as fh:
        return fh.read()
