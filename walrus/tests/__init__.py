import sys
import unittest

from walrus.tests.autocomplete import *
from walrus.tests.cache import *
from walrus.tests.containers import *
from walrus.tests.counter import *
from walrus.tests.database import *
from walrus.tests.fts import *
from walrus.tests.graph import *
from walrus.tests.lock import *
from walrus.tests.models import *
from walrus.tests.rate_limit import *
from walrus.tests.streams import *


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
