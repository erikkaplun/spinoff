from __future__ import print_function

import random
import warnings

from twisted.internet.defer import QueueUnderflow, Deferred, fail
from twisted.internet.task import Clock

from spinoff.actor import Actor, actor, CoroutineStopped, CoroutineNotRunning, CoroutineAlreadyStopped, CoroutineAlreadyRunning, CoroutineRefusedToStop
from spinoff.util.async import CancelledError, sleep
from spinoff.util.testing import deferred_result, assert_raises, assert_not_raises, assert_one_warning
from spinoff.util import pattern as match


warnings.simplefilter('always')


def test_basic():
    c = Actor()
    mock = Actor()
    c.connect(mock)

    c.put('msg-1')
    assert deferred_result(mock.get()) == 'msg-1'


def test_flow():
    called = [0]

    mock_d = Deferred()

    @actor
    def Proc(self):
        called[0] += 1
        yield mock_d
        called[0] += 1

    proc = Proc()
    assert not called[0], "creating a microprocess should not automatically start the coroutine in it"

    assert callable(getattr(proc, 'start', None)), "microprocesses should be startable"
    assert callable(getattr(proc, 'pause', None)), "microprocesses should be pausable"
    assert callable(getattr(proc, 'resume', None)), "microprocesses should be resumable"
    assert callable(getattr(proc, 'stop', None)), "microprocesses should be stoppable"

    d = proc.start()
    assert isinstance(d, Deferred), "starting a microprocesses returns a Deferred"

    with assert_raises(CoroutineAlreadyRunning):
        proc.start()

    mock_d.callback(None)

    assert called[0] == 2, "the coroutine in a microprocess should complete as normal"

    assert not proc.is_alive


def test_yielding_a_non_deferred():
    @actor
    def Actor1(self):
        tmp = random.random()
        ret = yield tmp
        assert ret == tmp
    Actor1.spawn()

    @actor
    def Actor2(self):
        ret = yield
        assert ret is None
    Actor2.spawn()


def test_exception():
    @actor
    def X(self):
        raise MockException()

    proc = X.spawn()

    with assert_raises(MockException):
        deferred_result(proc.d)


def test_fail_deferred():
    @actor
    def X(self):
        yield fail(MockException())

    proc = X.spawn()

    with assert_raises(MockException):
        deferred_result(proc.d)


def test_pending_exceptions_on_pause_are_discarded_with_a_warning():
    mock_d = Deferred()

    @actor
    def X(self):
        yield mock_d

    p = X()
    p.start()
    p.pause()
    try:
        raise Exception()
    except Exception:
        mock_d.errback()

    with assert_one_warning():
        p.stop()


# def test_return_value():
#     @actor
#     def X(self):


def test_pausing_and_resuming():
    async_result = [None]

    stopped = [False]

    mock_d = Deferred()

    def mock_async_fn():
        return mock_d

    @actor
    def X(self):
        try:
            ret = yield mock_async_fn()
            async_result[0] = ret
        except CoroutineStopped:
            stopped[0] = True

    ### resuming when the async called has been fired
    proc = X.spawn()

    proc.pause()
    assert not proc.is_running
    assert proc.is_alive
    assert proc.is_paused

    with assert_raises(CoroutineNotRunning):
        proc.pause()

    retval = random.random()
    mock_d.callback(retval)

    assert not proc.d.called, "a paused coroutine should not be resumed when the call it's waiting on completes"

    proc.resume()

    assert proc.d.called

    assert async_result[0] == retval

    ### resuming when the async call has NOT been fired
    mock_d = Deferred()
    proc2 = X.spawn()

    proc2.pause()
    proc2.resume()

    ### can't resume twice
    with assert_raises(CoroutineAlreadyRunning, "it should not be possible to resume a microprocess twice"):
        proc2.resume()

    ### stopping
    mock_d = Deferred()
    proc3 = X.spawn()

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
    proc4 = X.spawn()

    proc4.pause()
    proc4.stop()

    assert stopped[0]


def test_coroutine_does_not_have_to_catch_coroutinestopped():
    @actor
    def X(self):
        yield Deferred()
    proc = X.spawn()
    with assert_not_raises(CoroutineStopped):
        proc.stop()


def test_coroutine_must_exit_after_being_stopped():
    # coroutine that violates the rule
    @actor
    def X(self):
        while True:
            try:
                yield Deferred()
            except CoroutineStopped:
                pass
    proc = X.spawn()
    with assert_raises(CoroutineRefusedToStop, "coroutine should not be allowed to continue working when stopped"):
        proc.stop()

    # coroutine that complies with the rule
    @actor
    def Proc2(self):
        while True:
            try:
                yield Deferred()
            except CoroutineStopped:
                break
    proc2 = Proc2.spawn()
    with assert_not_raises(CoroutineRefusedToStop):
        proc2.stop()


def test_microprocess_with_args():
    passed_values = [None, None]

    @actor
    def Proc(self, a, b):
        yield
        passed_values[:] = [a, b]

    Proc.spawn(1, b=2)
    assert passed_values == [1, 2]


def test_microprocess_doesnt_require_generator():
    @actor
    def Proc(self):
        pass

    proc = Proc()
    with assert_not_raises():
        proc.start()
    deferred_result(proc.d)


def test_get():
    c = Actor()
    c.send('foo')
    assert 'foo' == deferred_result(c.get())

    tmp = random.random()
    c.send(('foo', tmp))
    x, = deferred_result(c.get(filter=('foo', match._)))
    assert tmp == x

    tmp = random.random()
    c.send(('foo', tmp))
    msg_d = c.get(filter=('baz', match._))
    assert not msg_d.called

    c.send(('baz', tmp))
    x, = deferred_result(msg_d)
    assert tmp == x


def test_cancel_get():
    c = Actor()
    d = c.get()
    with assert_raises(QueueUnderflow):
        c.get()

    ###
    c = Actor()
    d = c.get()
    d.addErrback(lambda f: f.trap(CancelledError))
    d.cancel()
    with assert_not_raises(QueueUnderflow):
        c.get()


def test_actor_parent():
    a = Actor()
    assert a.parent == None

    a1 = Actor()
    a2 = a1.spawn(Actor)
    assert a2.parent == a1


# def test_child_non_empty_return_values_raise_a_warning():
#     a1 = Actor()

#     # ...with a plain function without a return value
#     with assert_no_warnings():
#         a1.spawn(actor(lambda self: None))

#     # ...with a plain function
#     with assert_one_warning():
#         a1.spawn(actor(lambda self: 123))

#     # ... with microprocess + generator
#     def bla2(self):
#         yield
#         returnValue(123)
#     with assert_one_warning():
#         a1.spawn(actor(bla2))


def test_root_actor_errors_are_returned_asynchronously():
    a = make_actor_cls(run_with_error)()
    with assert_not_raises(MockException):
        d = a.start()
    with assert_raises(MockException):
        deferred_result(d)


def test_child_actor_errors_are_sent_to_parent():
    a1 = Actor()
    a2 = a1.spawn(make_actor_cls(run_with_error))
    msg = deferred_result(a1.get())
    assert msg[0:2] == ('child-failed', a2) and isinstance(msg[2], MockException)


def test_pause_and_resume_actor():
    called = [0]
    d = Deferred()

    @actor
    def X(self):
        called[0] += 1
        yield d
        called[0] += 1
    a = X()
    a.start()
    assert called[0] == 1

    a.pause()
    d.callback(None)
    assert called[0] == 1

    assert not a.is_running
    assert a.is_alive
    assert a.is_paused

    a.resume()
    assert called[0] == 2


def test_stop_actor():
    stopped = [False]

    @actor
    def X(self):
        try:
            yield Deferred()
        except CoroutineStopped:
            stopped[0] = True
    a = X.spawn()
    a.stop()

    assert stopped[0]


def test_pausing_actor_with_children_pauses_the_children():
    children = []
    child_stopped = [False]

    @actor
    def Child(self):
        try:
            yield Deferred()
        except CoroutineStopped:
            child_stopped[0] = True

    @actor
    def Parent(self):
        children.append(self.spawn(Child))
        yield Deferred()
    a = Parent.spawn()

    a.pause()
    assert all(x.is_paused for x in children)

    a.resume()
    assert all(not x.is_paused for x in children)

    a.stop()
    assert all(not x.is_alive for x in children)
    assert child_stopped[0]


def test_pausing_and_stoping_actor_with_some_finished_children():
    @actor
    def Stillborn(self):
        yield

    child_stopped = [False]

    @actor
    def LongLivingChild(self):
        try:
            yield Deferred()
        except CoroutineStopped:
            child_stopped[0] = True

    mock_d = Deferred()

    @actor
    def Parent(self):
        self.spawn(Stillborn)
        self.spawn(LongLivingChild)
        yield mock_d

    a = Parent.spawn()

    with assert_not_raises(CoroutineNotRunning):
        a.pause()

    with assert_not_raises(CoroutineAlreadyStopped):
        a.resume()

    a.stop()

    assert child_stopped[0]


def test_actor_finishing_before_child():
    child_stopped = [False]

    @actor
    def Child(self):
        try:
            yield Deferred()
        except CoroutineStopped:
            child_stopped[0] = True

    @actor
    def Parent(self):
        self.spawn(Child)

    p = Parent.spawn()
    assert not child_stopped[0]
    assert p.is_running


def test_actor_joins_child():
    clock = Clock()

    child_died_naturally = [False]

    @actor
    def Child(self):
        try:
            yield sleep(1.0, clock)
        except CoroutineStopped:
            pass
        else:
            child_died_naturally[0] = True

    @actor
    def Parent(self):
        child = self.spawn(Child)
        yield self.join(child)

    p = Parent.spawn()
    assert p.is_running, "an actor should be waiting for a joining child actor to complete"

    clock.advance(1.0)
    assert not p.is_alive, "an actor should die when a joining child actor completes"

    assert child_died_naturally[0], "a joining child of an actor should die a natural death"

    ##########################
    # join all children

    @actor
    def Parent2(self):
        for _ in range(3):
            self.spawn(Child)
        yield self.join_children()

    p = Parent2.spawn()
    assert p.is_running

    clock.advance(1.0)
    assert not p.is_alive

    ##########################
    # join a child with error

    @actor
    def ErrorChild(self):
        raise Exception()

    @actor
    def Parent3(self):
        c = self.spawn(ErrorChild)
        try:
            yield self.join(c)
        except Exception:
            assert False

    Parent3.spawn()


# def test_spawn_microprocess():
#     bla = [False]

#     @microprocess
#     def P(self):
#         bla[0] = True
#         yield Deferred()

#     @actor
#     def A(self):
#         self.spawn(P)
#         yield Deferred()

#     a = A.spawn()
#     assert bla[0]

#     a.stop()


def make_actor_cls(run_fn):
    class MockActor(Actor):
        run = run_fn
    MockActor.__name__ = run_fn.__name__
    return MockActor


class MockException(Exception):
    pass


def run_with_error(self):
    raise MockException()
