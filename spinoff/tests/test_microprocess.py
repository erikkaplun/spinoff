import random

from twisted.internet.defer import Deferred, returnValue, _DefGen_Return

from spinoff.util.microprocess import microprocess, CoroutineStopped, CoroutineRefusedToStop, CoroutineAlreadyRunning
from spinoff.util.testing import assert_not_raises, deferred_result, assert_raises
from spinoff.util.microprocess import CoroutineNotRunning
from spinoff.util.microprocess import CoroutineAlreadyStopped


def test_basic():
    called = [0]

    retval = random.random()

    def mock_coroutine():
        called[0] += 1
        yield
        called[0] += 1
        returnValue(retval)

    Proc = microprocess(mock_coroutine)

    proc = Proc()
    assert not called[0], "creating a microprocess should not automatically start the coroutine in it"

    assert callable(getattr(proc, 'start', None)), "microprocesses should be startable"
    assert callable(getattr(proc, 'pause', None)), "microprocesses should be pausable"
    assert callable(getattr(proc, 'resume', None)), "microprocesses should be resumable"
    assert callable(getattr(proc, 'stop', None)), "microprocesses should be stoppable"

    with assert_not_raises(_DefGen_Return):
        d = proc.start()
    assert called[0], "starting a microprocesses will start the coroutine in it"
    assert isinstance(d, Deferred), "starting a microprocesses returns a Deferred"

    assert called[0] == 2, "the coroutine in a microprocess should complete as normal"
    assert deferred_result(d) == retval, "the deferred returned by microprocess.start should contain the result of the coroutine"

    with assert_raises(CoroutineAlreadyRunning):
        proc.start()


def test_deferreds_inside_microprocesses():
    called = [0]

    mock_d = Deferred()

    def mock_async_fn():
        return mock_d

    def mock_coroutine():
        called[0] += 1
        yield mock_async_fn()
        called[0] += 1

    Proc = microprocess(mock_coroutine)
    proc = Proc()
    d = proc.start()

    assert not d.called
    mock_d.callback(None)
    assert d.called


def test_wrapped_coroutine_yielding_a_non_deferred():
    def mock_coroutine():
        tmp = random.random()
        ret = yield tmp
        assert ret == tmp
    Proc = microprocess(mock_coroutine)
    proc = Proc()
    proc.start()


def test_pausing_and_resuming():
    async_result = [None]

    stopped = [False]

    mock_d = Deferred()

    def mock_async_fn():
        return mock_d

    def mock_coroutine():
        try:
            ret = yield mock_async_fn()
            async_result[0] = ret
        except CoroutineStopped:
            stopped[0] = True
    Proc = microprocess(mock_coroutine)

    ### resuming when the async called has been fired
    proc = Proc()
    d = proc.start()

    proc.pause()

    with assert_raises(CoroutineNotRunning):
        proc.pause()

    retval = random.random()
    mock_d.callback(retval)

    assert not d.called, "a paused coroutine should not be resumed when the call it's waiting on completes"

    proc.resume()

    assert d.called

    assert async_result[0] == retval

    ### resuming when the async call has NOT been fired
    mock_d = Deferred()
    proc = Proc()
    d = proc.start()

    proc.pause()
    proc.resume()

    ### can't resume twice
    with assert_raises(CoroutineAlreadyRunning, "it should not be possible to resume a microprocess twice"):
        proc.resume()

    ### stopping
    mock_d = Deferred()
    proc = Proc()
    d = proc.start()

    proc.stop()
    with assert_raises(CoroutineAlreadyStopped):
        proc.stop()

    assert stopped[0]

    with assert_raises(CoroutineAlreadyStopped):
        proc.start()
    with assert_raises(CoroutineAlreadyStopped):
        proc.resume()

    ### stopping a paused coroutine
    mock_d = Deferred()
    proc = Proc()
    d = proc.start()

    proc.pause()
    proc.stop()

    assert stopped[0]


def test_coroutine_must_exit_after_being_stopped():
    def mock_coroutine():
        while True:
            try:
                yield Deferred()
            except CoroutineStopped:
                pass
    Proc = microprocess(mock_coroutine)
    proc = Proc()
    proc.start()
    with assert_raises(CoroutineRefusedToStop, "coroutine should not be allowed to continue working when stopped"):
        proc.stop()
