"""
Lightweight Python utilities for working with Redis.
"""

__author__ = 'Charles Leifer'
__license__ = 'MIT'
__version__ = '0.2.2'

#               ___
#            .-9 9 `\
#          =(:(::)=  ;
#            ||||     \
#            ||||      `-.
#           ,\|\|         `,
#          /                \
#         ;                  `'---.,
#         |                         `\
#         ;                     /     |
#         \                    |      /
#  jgs     )           \  __,.--\    /
#       .-' \,..._\     \`   .-'  .-'
#      `-=``      `:    |   /-/-/`
#                   `.__/

from walrus.autocomplete import Autocomplete
from walrus.cache import Cache
from walrus.containers import Array
from walrus.containers import Container
from walrus.containers import Hash
from walrus.containers import HyperLogLog
from walrus.containers import List
from walrus.containers import Set
from walrus.containers import ZSet
from walrus.database import Database
from walrus.lock import Lock
from walrus.models import *

# Friendly alias.
Walrus = Database
