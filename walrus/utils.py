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
    return s.encode('utf-8') if isinstance(s, unicode_type) else s


def decode(s):
    return s.decode('utf-8') if isinstance(s, bytes) else s


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
