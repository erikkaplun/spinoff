from __future__ import print_function

import warnings
from contextlib import contextmanager
from functools import wraps

from twisted.internet.defer import Deferred
from twisted.internet.task import Clock

from spinoff.util.async import CancelledError


def simtime(fn):
    @wraps(fn)
    def wrap(*args, **kwargs):
        clock = Clock()
        from twisted import internet
        oldreactor = internet.reactor
        internet.reactor = clock
        try:
            fn(clock, *args, **kwargs)
        finally:
            internet.reactor = oldreactor
    return wrap


def deferred(f):
    """This is almost exactly the same as nose.twistedtools.deferred, except it allows one to have simulated time in
    their test code by passing in a `Clock` instance. That `Clock` instance will be `advance`d as long as there are
    any deferred calls bound to it.

    """
    @wraps(f)
    def ret():
        error = [None]

        clock = Clock()

        d = f(clock)

        @d.addErrback
        def on_error(f):
            error[0] = f

        while True:
            time_to_wait = max([0] + [call.getTime() - clock.seconds() for call in clock.getDelayedCalls()])
            if time_to_wait == 0:
                break
            else:
                clock.advance(time_to_wait)

        if error[0]:
            error[0].raiseException()

    return ret


def immediate(d):
    assert d.called
    return d


def deferred_result(d):
    ret = [None]
    exc = [None]
    if isinstance(d, Deferred):
        d = immediate(d)
        d.addCallback(lambda result: ret.__setitem__(0, result))
        d.addErrback(lambda f: exc.__setitem__(0, f))
        if exc[0]:
            exc[0].raiseException()
        return ret[0]
    else:
        return d


def cancel_deferred(d):
    d.addErrback(lambda f: f.trap(CancelledError))
    d.cancel()


@contextmanager
def assert_not_raises(exc_class=Exception, message=None):
    if isinstance(exc_class, basestring):
        message, exc_class = exc_class, Exception
    try:
        yield
    except exc_class as e:
        raise AssertionError(message or "No exception should have been raised but instead %s was raised" % (repr(e),))


@contextmanager
def assert_raises(exc_class=Exception, message=None):
    if isinstance(exc_class, basestring):
        message, exc_class = exc_class, Exception
    basket = [None]
    try:
        yield basket
    except exc_class as e:
        basket[0] = e
    else:
        raise AssertionError(message or "An exception should have been raised")


@contextmanager
def assert_num_warnings(n, message=None):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        yield
        assert len(w) == n, message or "expected %s warnings but found %s: %s" % (n, len(w), ', '.join(map(str, w)))


def assert_no_warnings(message=None):
    return assert_num_warnings(0, message)


def assert_one_warning(message=None):
    return assert_num_warnings(1, message)


swallow_one_warning = assert_one_warning


class MockFunction(object):

    called = property(lambda self: len(self.argses))

    def __init__(self, return_values=None):
        self.reset()
        self.return_values = return_values and list(return_values)

    def __call__(self, *args, **kwargs):
        self.argses.append(args)
        self.kwargses.append(kwargs)
        if self.return_values is not None:
            return self.return_values.pop(0)

    def assert_called(self, n=None, message=None):
        if isinstance(n, basestring):
            n, message = None, n
        if n is not None:
            assert self.called == n
        else:
            assert self.called
        self.reset()

    def assert_called_with_signature(self, *args, **kwargs):
        args_, kwargs_ = self.argses.pop(0), self.kwargses.pop(0)
        assert args == args_ and kwargs == kwargs_, "mock function called with different arguments than expected"

    def reset(self):
        self.argses = []
        self.kwargses = []


def errback_called(d):
    mock_fn = MockFunction()
    d.addErrback(mock_fn)
    return mock_fn.called


def callback_called(d):
    mock_fn = MockFunction()
    d.addCallback(mock_fn)
    return mock_fn.called


def timeout(timeout=None):
    def decorate(fn):
        fn.timeout = timeout
        return fn
    return decorate
