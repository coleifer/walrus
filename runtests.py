#!/usr/bin/env python

import optparse
import os
import sys
import unittest

def runtests(verbose=False, failfast=False, names=None):
    if names:
        suite = unittest.TestLoader().loadTestsFromNames(names, tests)
    else:
        suite = unittest.TestLoader().loadTestsFromModule(tests)
    runner = unittest.TextTestRunner(verbosity=2 if verbose else 1,
                                     failfast=failfast)
    return runner.run(suite)

if __name__ == '__main__':
    try:
        from redis import Redis
    except ImportError:
        raise RuntimeError('redis-py must be installed.')
    else:
        try:
            Redis().info()
        except:
            raise RuntimeError('redis server does not appear to be running')

    parser = optparse.OptionParser()
    parser.add_option('-v', '--verbose', action='store_true', default=False,
                      dest='verbose', help='Verbose output.')
    parser.add_option('-f', '--failfast', action='store_true', default=False,
                      help='Stop on first failure or error.')
    parser.add_option('-s', '--stream', action='store_true', dest='stream',
                      help='Run stream command tests (default if server>=5.0)')
    parser.add_option('-z', '--zpop', action='store_true', dest='zpop',
                      help='Run ZPOP* tests (default if server>=5.0)')
    options, args = parser.parse_args()
    if options.stream:
        os.environ['TEST_STREAM'] = '1'
    if options.zpop:
        os.environ['TEST_ZPOP'] = '1'

    from walrus import tests

    result = runtests(
        verbose=options.verbose,
        failfast=options.failfast,
        names=args)

    if result.failures:
        sys.exit(1)
    elif result.errors:
        sys.exit(2)
