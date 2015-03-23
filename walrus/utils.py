import itertools
import os
import sys


if sys.version_info[0] == 2:
    unicode_type = unicode
else:
    unicode_type = str


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


_sentinel = object()

def chunked(iterable, n):
    gen_expr = (list(g) for g in itertools.izip_longest(
        *[iter(iterable)] * n, fillvalue=_sentinel))
    for group in gen_expr:
        if group[-1] is _sentinel:
            del group[group.index(_sentinel):]
        yield group
