from twisted.internet.defer import TimeoutError, Deferred, CancelledError, inlineCallbacks
from twisted.internet.task import Clock

from unnamedframework.util.async import with_timeout, sleep
from unnamedframework.util.testing import deferred, errback_called


@deferred
@inlineCallbacks
def test_timeout_is_reached(clock):
    try:
        yield with_timeout(1.0, sleep(2.0, reactor=clock), reactor=clock)
    except TimeoutError:
        pass
    else:
        assert False, "Timeout should have been reached"


@deferred
@inlineCallbacks
def test_timeout_is_not_reached(clock):
    try:
        yield with_timeout(2.0, sleep(1.0, clock), reactor=clock)
    except TimeoutError:
        assert False, "Timeout should not have been reached"


def test_deferred_is_cancelled_when_timeout_reached():
    clock = Clock()

    t = []
    mock = Deferred(canceller=lambda _: t.append('OK'))

    d = with_timeout(1.0, mock, reactor=clock)
    d.addErrback(lambda f: f.trap(CancelledError))
    d.cancel()

    clock.advance(10)

    assert t == ['OK']


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
    except TimeoutError:
        assert False, "Should not have raised TimeoutError"
    else:
        assert False, "Should have raised MyExc"


def test_with_no_timeout():
    clock = Clock()
    d = with_timeout(None, Deferred(), clock)
    clock.advance(9999999999999999)
    assert not errback_called(d)
