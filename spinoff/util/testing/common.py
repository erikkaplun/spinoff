from __future__ import print_function

import types
import warnings
from contextlib import contextmanager

from gevent import idle, Timeout, sleep
from nose.tools import eq_


@contextmanager
def assert_not_raises(exc_class=Exception, message=None):
    assert issubclass(exc_class, BaseException) or isinstance(exc_class, types.ClassType)
    try:
        yield
    except exc_class as e:
        raise AssertionError(message or "No exception should have been raised but instead %s was raised" % (repr(e),))


@contextmanager
def assert_raises(exc_class=Exception, message=None):
    assert issubclass(exc_class, BaseException) or isinstance(exc_class, types.ClassType)
    basket = [None]
    try:
        yield basket
    except exc_class as e:
        basket[0] = e
    else:
        raise AssertionError(message or "An exception should have been raised")


@contextmanager
def expect_num_warnings(n, message=None, timeout=None):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        yield
        with Timeout(timeout, exception=False):
            while len(w) < n:
                idle()
        eq_(len(w), n, message or "expected %s warnings but found %s: %s" % (n, len(w), ', '.join(map(str, w))))


@contextmanager
def expect_no_warnings(during, message=None):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        yield
        sleep(during + 0.001)  # + 0.001 so that warnings occuring exactly in `during` seconds would be noticed
        eq_(len(w), 0, message or "expected no warnings but found %s: %s" % (len(w), ', '.join(map(str, w))))


def expect_one_warning(message=None):
    return expect_num_warnings(1, message)


swallow_one_warning = expect_one_warning


def timeout(timeout=None):
    def decorate(fn):
        fn.timeout = timeout
        return fn
    return decorate
