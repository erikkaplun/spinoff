from twisted.internet.defer import TimeoutError, Deferred, CancelledError, inlineCallbacks
from twisted.internet.task import Clock

from spinoff.util.async import with_timeout, sleep
from spinoff.util.testing import deferred


@deferred
@inlineCallbacks
def test_timeout1(clock):
    try:
        yield with_timeout(1.0, sleep(2.0, reactor=clock), reactor=clock)
    except TimeoutError:
        pass
    else:
        assert False, "Timeout should have been reached"


@deferred
@inlineCallbacks
def test_timeout2(clock):
    try:
        yield with_timeout(2.0, sleep(1.0, clock), reactor=clock)
    except TimeoutError:
        assert False, "Timeout should not have been reached"


def test_timeout3():
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
def test_timeout_with_exc(clock):
    class MyExc(Exception): pass

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
