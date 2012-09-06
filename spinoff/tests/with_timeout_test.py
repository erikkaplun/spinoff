from twisted.internet.defer import Deferred, CancelledError, inlineCallbacks
from twisted.internet.task import Clock

from spinoff.util.async import with_timeout, sleep, Timeout
from spinoff.util.testing import deferred, errback_called, MockFunction


@deferred
@inlineCallbacks
def test_timeout_is_reached(clock):
    try:
        yield with_timeout(1.0, sleep(2.0, reactor=clock), reactor=clock)
    except Timeout:
        pass
    else:
        assert False, "Timeout should have been reached"


@deferred
@inlineCallbacks
def test_timeout_is_not_reached(clock):
    try:
        yield with_timeout(2.0, sleep(1.0, clock), reactor=clock)
    except Timeout:
        assert False, "Timeout should not have been reached"


def test_deferred_is_cancelled_when_timeout_reached():
    clock = Clock()

    cancel_fn = MockFunction()
    mock = Deferred(canceller=cancel_fn)

    d = with_timeout(1.0, mock, reactor=clock)
    d.addErrback(lambda f: f.trap(CancelledError))
    d.cancel()

    clock.advance(10)

    assert cancel_fn.called


@deferred
@inlineCallbacks
def test_exceptions_are_passed_through(clock):
    class MyExc(Exception):
        pass

    @inlineCallbacks
    def dummy():
        yield sleep(1.0, reactor=clock)
        raise MyExc

    try:
        yield with_timeout(2.0, dummy(), reactor=clock)
    except MyExc:
        pass
    except Timeout:
        assert False, "Should not have raised TimeoutError"
    else:
        assert False, "Should have raised MyExc"


def test_with_no_timeout():
    clock = Clock()
    d = with_timeout(None, Deferred(), clock)
    clock.advance(9999999999999999)
    assert not errback_called(d)


@deferred
@inlineCallbacks
def test_with_no_deferred(clock):
    foo = yield with_timeout(123, 'foo')
    assert foo == 'foo'
