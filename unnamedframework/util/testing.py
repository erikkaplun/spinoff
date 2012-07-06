import warnings
import sys
from contextlib import contextmanager
from functools import wraps

from twisted.internet.task import Clock
from twisted.internet.defer import Deferred, succeed

from unnamedframework.util.async import CancelledError
from unnamedframework.actor.actor import BaseActor
from unnamedframework.util.pattern_matching import match, ANY, IS_INSTANCE, NOT


__all__ = ['deferred', 'assert_raises', 'assert_not_raises', 'MockFunction', 'assert_num_warnings', 'assert_no_warnings', 'assert_one_warning']


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
        raise AssertionError(message or "No exception should have been raised but instead %s was raised" % repr(e))


@contextmanager
def assert_raises(exc_class=Exception, message=None):
    if isinstance(exc_class, basestring):
        message, exc_class = exc_class, Exception
    try:
        yield
    except exc_class:
        pass
    else:
        raise AssertionError(message or "An exception should have been raised")


@contextmanager
def assert_num_warnings(n):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        yield
        assert len(w) == n


def assert_no_warnings():
    return assert_num_warnings(0)


def assert_one_warning():
    return assert_num_warnings(1)


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


class DebugActor(object):

    def __init__(self, name, debug_fn=lambda x: 'a message'):
        self.name = name
        self.debug_fn = debug_fn

    def deliver(self, message, inbox):
        print "%s: received %s into %s" % (self.name, self.debug_fn(message), inbox)


class MockActor(BaseActor):

    def __init__(self, *args, **kwargs):
        super(MockActor, self).__init__(*args, **kwargs)
        self.messages = []
        self.waiting = None

    def handle(self, message):
        if self.waiting:
            self.waiting.callback(message)
        else:
            self.messages.append(message)

    def clear(self):
        ret = self.messages
        self.messages = []
        return ret

    def wait(self):
        if self.messages:
            return succeed(self.messages.pop(0))
        self.waiting = Deferred()
        return self.waiting


class Container(MockActor):

    def __init__(self, actor_cls=None, start_automatically=True):
        super(Container, self).__init__()
        self._actor_cls = actor_cls
        self._start_automatically = start_automatically

    def __enter__(self):
        self.start()
        if self._actor_cls:
            self._actor = a = self._actor_cls() if isinstance(self._actor_cls, type) else self._actor_cls
            a._parent = self
            if self._start_automatically:
                a.start()
        return self

    def __exit__(self, exc_cls, exc, tb):
        if exc is None:
            self.raise_errors()
        else:
            raise exc_cls, exc, tb

    def consume_message(self, pattern, n=1, message=None):
        assert n in ('INF', 'ANY') or n >= 0
        consumed = 0
        while n > 0 or n in ('INF', 'ANY'):
            for msg in self.messages:
                if pattern == msg:
                    self.messages.remove(msg)
                    consumed += 1
                    break
            else:
                break
            if n not in ('INF', 'ANY'):
                n -= 1
        assert consumed or n == 'ANY', message

    def ignore_non_assertions(self):
        self.consume_message(('error', ANY, (NOT(IS_INSTANCE(AssertionError)), ANY)), 'ANY')

    def has_message(self, pattern):
        return any(match(pattern, msg) for msg in self.messages)

    def raise_errors(self, only_asserts=False):
        assert not only_asserts
        for msg in self.messages:
            if msg[0] == 'error':
                # see: http://twistedmatrix.com/trac/ticket/5178
                if not isinstance(msg[2][1], basestring):
                    raise msg[2][0], None, msg[2][1]
                else:
                    print(msg[2][1])
                    raise msg[2][0]


class RootActor(Container):
    def __new__(cls, *args, **kwargs):
        warnings.warn("RootActor is deprecated; use Container instead", DeprecationWarning)
        return super(RootActor, cls).__new__(cls, *args, **kwargs)


def run(a_cls, raise_only_asserts=False):
    if raise_only_asserts:
        warnings.warn("raise_only_asserts is deprecated; use Container instead", DeprecationWarning)
    container = contain(a_cls)
    _, actor = container.__enter__()
    if raise_only_asserts:
        container.consume_message(('error', ANY, (NOT(IS_INSTANCE(AssertionError)), ANY), ANY), n='INF')
    container.__exit__(None, None, None)
    return container, actor


class contain(Container):

    def __enter__(self):
        super(contain, self).__enter__()
        return self, self._actor
