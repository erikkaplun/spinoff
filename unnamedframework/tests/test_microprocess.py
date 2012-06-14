import random

from twisted.internet.defer import Deferred, returnValue, _DefGen_Return

from unnamedframework.util.microprocess import (microprocess, CoroutineStopped, CoroutineRefusedToStop,
                                                CoroutineAlreadyRunning, CoroutineNotRunning, CoroutineAlreadyStopped, is_microprocess)
from unnamedframework.util.testing import assert_not_raises, deferred_result, assert_raises, assert_no_warnings, assert_one_warning


def test_basic():
    called = [0]

    retval = random.random()

    mock_d = Deferred()

    @microprocess
    def Proc():
        called[0] += 1
        yield mock_d
        called[0] += 1
        returnValue(retval)

    proc = Proc()
    assert not called[0], "creating a microprocess should not automatically start the coroutine in it"

    assert callable(getattr(proc, 'start', None)), "microprocesses should be startable"
    assert callable(getattr(proc, 'pause', None)), "microprocesses should be pausable"
    assert callable(getattr(proc, 'resume', None)), "microprocesses should be resumable"
    assert callable(getattr(proc, 'stop', None)), "microprocesses should be stoppable"

    with assert_not_raises(_DefGen_Return):
        d = proc.start()
    assert isinstance(d, Deferred), "starting a microprocesses returns a Deferred"

    with assert_raises(CoroutineAlreadyRunning):
        proc.start()

    mock_d.callback(None)

    assert called[0] == 2, "the coroutine in a microprocess should complete as normal"
    assert deferred_result(d) == retval, "the deferred returned by microprocess.start should contain the result of the coroutine"

    assert not proc.is_alive


def test_returnvalue():
    @microprocess
    def Proc():
        yield
        returnValue(123)
    with assert_no_warnings():
        Proc().start()

    #######################
    # This can be considered white-box testing as we know that @microprocesses uses a customized version of
    # inlineCallbacks internally that works around unwanted returnValue warnings--we need to verify if it still keeps
    # wanted returnValue warnings:

    def foo():
        returnValue(123)

    @microprocess
    def Proc2():
        yield
        foo()
    with assert_one_warning():
        Proc2().start()


def test_is_microprocess():
    def Proc():
        yield

    assert not is_microprocess(Proc)

    Proc = microprocess(Proc)

    assert is_microprocess(Proc)

    class Mock(object):
        some_method = Proc
    assert is_microprocess(Mock.some_method)
    assert is_microprocess(Mock().some_method)


def test_deferreds_inside_microprocesses():
    called = [0]

    mock_d = Deferred()

    @microprocess
    def Proc():
        called[0] += 1
        yield mock_d
        called[0] += 1

    proc = Proc()
    d = proc.start()

    assert not d.called
    mock_d.callback(None)
    assert d.called


def test_wrapped_coroutine_yielding_a_non_deferred():
    @microprocess
    def Proc():
        tmp = random.random()
        ret = yield tmp
        assert ret == tmp
    proc = Proc()
    proc.start()

    @microprocess
    def Proc2():
        ret = yield
        assert ret is None
    proc2 = Proc2()
    proc2.start()


def test_pausing_and_resuming():
    async_result = [None]

    stopped = [False]

    mock_d = Deferred()

    def mock_async_fn():
        return mock_d

    @microprocess
    def Proc():
        try:
            ret = yield mock_async_fn()
            async_result[0] = ret
        except CoroutineStopped:
            stopped[0] = True

    ### resuming when the async called has been fired
    proc = Proc()
    d = proc.start()

    proc.pause()
    assert not proc.is_running
    assert proc.is_alive
    assert proc.is_paused

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
    proc2 = Proc()
    d = proc2.start()

    proc2.pause()
    proc2.resume()

    ### can't resume twice
    with assert_raises(CoroutineAlreadyRunning, "it should not be possible to resume a microprocess twice"):
        proc2.resume()

    ### stopping
    mock_d = Deferred()
    proc3 = Proc()
    d = proc3.start()

    proc3.stop()
    with assert_raises(CoroutineAlreadyStopped):
        proc3.stop()

    assert stopped[0]

    with assert_raises(CoroutineAlreadyStopped):
        proc3.start()
    with assert_raises(CoroutineAlreadyStopped):
        proc3.resume()

    ### stopping a paused coroutine
    mock_d = Deferred()
    proc4 = Proc()
    d = proc4.start()

    proc4.pause()
    proc4.stop()

    assert stopped[0]


def test_coroutine_does_not_have_to_catch_coroutinestopped():
    @microprocess
    def Proc():
        yield Deferred()
    proc = Proc()
    proc.start()
    with assert_not_raises(CoroutineStopped):
        proc.stop()


def test_coroutine_must_exit_after_being_stopped():
    # coroutine that violates the rule
    @microprocess
    def Proc():
        while True:
            try:
                yield Deferred()
            except CoroutineStopped:
                pass
    proc = Proc()
    proc.start()
    with assert_raises(CoroutineRefusedToStop, "coroutine should not be allowed to continue working when stopped"):
        proc.stop()

    # coroutine that complies with the rule
    @microprocess
    def Proc2():
        while True:
            try:
                yield Deferred()
            except CoroutineStopped:
                break
    proc2 = Proc2()
    proc2.start()
    with assert_not_raises(CoroutineRefusedToStop):
        proc2.stop()


def test_coroutine_can_return_a_value_when_stopped():
    retval = random.random()

    @microprocess
    def Proc():
        while True:
            try:
                yield Deferred()
            except CoroutineStopped:
                returnValue(retval)
    proc = Proc()
    d = proc.start()
    with assert_not_raises(_DefGen_Return):
        proc.stop()
    assert deferred_result(d) == retval


def test_microprocess_with_args():
    passed_values = [None, None]

    @microprocess
    def Proc(a, b):
        yield
        passed_values[:] = [a, b]

    proc = Proc(1, b=2)
    proc.start()

    assert passed_values == [1, 2]


def test_microprocess_doesnt_require_generator():
    @microprocess
    def Proc():
        pass

    proc = Proc()
    with assert_not_raises():
        d = proc.start()
    deferred_result(d)
