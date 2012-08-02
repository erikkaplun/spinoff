import gc
import sys
import weakref
from contextlib import contextmanager

from twisted.internet.defer import Deferred, returnValue, fail, succeed

from spinoff.util.python import noreturn
from spinoff.util._defer import inlineCallbacks
from spinoff.util.testing import assert_raises, deferred_result, assert_not_raises


def test_main():
    d_ctrl_1, d_ctrl_2 = Deferred(), Deferred()

    wref = [None]
    never_called = [True]

    @inlineCallbacks
    def test_coroutine():
        dummy = Dummy()
        wref[0] = weakref.ref(dummy)

        yield d_ctrl_1  # pause for state inspection

        noreturn(other_coroutine())
        never_called[0] = False

    other_coroutine_called = [False]

    def other_coroutine():
        other_coroutine_called[0] = True
        yield d_ctrl_2  # pause for state inspection

    final_d = test_coroutine()

    gc.collect()  # don't rely on automatic collection
    assert wref[0]()  # just to reinforce the test logic

    d_ctrl_1.callback(None)  # resume test_coroutine
    assert other_coroutine_called[0], "the other coroutine should be executed"
    gc.collect()  # don't rely on automatic collection
    assert not wref[0](), "objects in the caller should become garbage"
    assert not final_d.called

    d_ctrl_2.callback(None)  # resume other_coroutine
    assert final_d.called, "the whole procedure should have been completed"
    assert never_called[0], "flow should never return to the noreturn-calling coroutine"


def test_deep_recursion():
    def fact(n, result=1):
        if n <= 1:
            returnValue(result)
        else:
            noreturn(fact(n - 1, n * result))
        yield

    assert deferred_result(inlineCallbacks(fact)(1)) == 1
    assert deferred_result(inlineCallbacks(fact)(10)) == safe_fact(10)

    with recursion_limit(100):
        with assert_not_raises(RuntimeError):
            # +10 is actually too high here as we probably already have some stuff on the stack, but just to be sure
            assert deferred_result(inlineCallbacks(fact)(110)) == safe_fact(110)

    # ...and now let's prove that the same (tail call optimizable) algorithm without noreturn will eat up the stack

    def normal_fact(n, result=1):
        if n <= 1:
            returnValue(result)
        else:
            return normal_fact(n - 1, n * result)

    with recursion_limit(100):
        with assert_raises(RuntimeError):
            normal_fact(110)


def test_with_previous_yield_result_not_none():
    class MockException(Exception):
        pass

    fn_called = [False]

    @inlineCallbacks
    def fn():
        fn_called[0] = True
        try:
            yield fail(MockException())
        except MockException:
            pass

        noreturn(fn2())

    def fn2():
        yield succeed(None)

    with assert_not_raises(MockException):
        fn()
    assert fn_called[0]


def test_noreturn_of_other_inlineCallbacks_wrapped_callable():
    after_noreturn_reached = [False]

    @inlineCallbacks
    def fn():
        yield
        noreturn(fn2())
        after_noreturn_reached[0] = True

    fn2_called = [False]

    @inlineCallbacks
    def fn2():
        fn2_called[0] = True
        yield
        returnValue('someretval')

    retval = deferred_result(fn())
    assert fn2_called[0]
    assert not after_noreturn_reached[0]
    assert retval == 'someretval'


def test_noreturn_with_regular_function():
    after_noreturn_reached = [False]

    @inlineCallbacks
    def fn():
        yield
        noreturn(fn2())
        after_noreturn_reached[0] = True

    def fn2():
        return 'someretval'

    retval = deferred_result(fn())
    assert not after_noreturn_reached[0]
    assert retval == 'someretval'


@contextmanager
def recursion_limit(n):
    old = sys.getrecursionlimit()
    sys.setrecursionlimit(n)
    try:
        yield
    finally:
        sys.setrecursionlimit(old)


# because object() cannot be weakly referenced
class Dummy:
    pass


def safe_fact(n):
    return reduce(lambda a, b: a * b, xrange(1, n + 1))
