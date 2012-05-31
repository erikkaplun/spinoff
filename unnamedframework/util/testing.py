import sys
from contextlib import contextmanager
from functools import wraps

from twisted.internet.task import Clock

from unnamedframework.util.async import CancelledError


__all__ = ['deferred', 'assert_raises', 'assert_not_raises', 'MockFunction']


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
            error[0] = sys.exc_info()

        while True:
            time_to_wait = max([0] + [call.getTime() - clock.seconds() for call in clock.getDelayedCalls()])
            if time_to_wait == 0:
                break
            else:
                clock.advance(time_to_wait)

        if error[0]:
            exc_info = error[0]
            raise exc_info[0], exc_info[1], exc_info[2]

    return ret


def immediate(d):
    assert d.called
    return d


def deferred_result(d):
    ret = [None]
    exc = [None]
    d = immediate(d)
    d.addCallback(lambda result: ret.__setitem__(0, result))
    d.addErrback(lambda f: exc.__setitem__(0, f))
    if exc[0]:
        exc[0].raiseException()
    return ret[0]


def cancel_deferred(d):
    d.addErrback(lambda f: f.trap(CancelledError))
    d.cancel()


@contextmanager
def assert_not_raises(exc_class=Exception, message=None):
    try:
        yield
    except exc_class as e:
        raise AssertionError(message or "No exception should have been raised but instead %s was raised" % repr(e))


@contextmanager
def assert_raises(exc_class=Exception, message=None):
    try:
        yield
    except exc_class:
        pass
    else:
        raise AssertionError(message or "An exception should have been raised")


class MockFunction(object):

    def __init__(self):
        self.reset()

    def __call__(self, *args, **kwargs):
        self.called += 1
        self.args = args
        self.kwargs = kwargs

    def assert_called(self, n=None, message=None):
        if isinstance(n, basestring):
            n, message = None, n
        if n is not None:
            assert self.called == n
        else:
            assert self.called
        self.reset()

    def reset(self):
        self.called = 0
        self.args = self.kwargs = None


def errback_called(d):
    mock_fn = MockFunction()
    d.addErrback(mock_fn)
    return mock_fn.called


def callback_called(d):
    mock_fn = MockFunction()
    d.addCallback(mock_fn)
    return mock_fn.called


class DebugActor(object):

    def __init__(self, name, debug_fn=lambda x: 'a message'):
        self.name = name
        self.debug_fn = debug_fn

    def deliver(self, message, inbox):
        print "%s: received %s into %s" % (self.name, self.debug_fn(message), inbox)
