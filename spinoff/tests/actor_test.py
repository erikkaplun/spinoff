from __future__ import print_function

import random
import warnings

from twisted.internet.defer import QueueUnderflow, Deferred, fail
from twisted.internet.task import Clock

from spinoff.actor import Actor, actor, ActorStopped, ActorNotRunning, ActorAlreadyStopped, ActorAlreadyRunning, ActorRefusedToStop
from spinoff.util.async import CancelledError, sleep
from spinoff.util.testing import deferred_result, assert_raises, assert_not_raises, assert_one_warning
from spinoff.util import pattern as match


warnings.simplefilter('always')


def test_connect_and_put():
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
    assert not called[0], "creating an actor should not automatically start the coroutine in it"

    assert callable(getattr(proc, 'start', None)), "actors should be startable"
    assert callable(getattr(proc, 'pause', None)), "actors should be pausable"
    assert callable(getattr(proc, 'resume', None)), "actors should be resumable"
    assert callable(getattr(proc, 'stop', None)), "actors should be stoppable"

    d = proc.start()
    assert isinstance(d, Deferred), "starting an actor returns a Deferred"

    with assert_raises(ActorAlreadyRunning):
        proc.start()

    mock_d.callback(None)

    assert called[0] == 2, "the coroutine in an actor should complete as normal"

    assert not proc.is_alive


def test_exception():
    mock_d = Deferred()
    exception_caught = [False]

    @actor
    def Y(self):
        try:
            yield mock_d
        except MockException:
            exception_caught[0] = True

    Y.spawn()
    mock_d.errback(MockException())
    assert exception_caught[0]


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


def test_pending_exceptions_are_discarded_with_a_warning():
    mock_d = Deferred()

    @actor
    def X(self):
        yield mock_d

    p = X.spawn()
    p.pause()

    mock_d.errback(Exception())
    with assert_one_warning():
        p.stop()


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
        except ActorStopped:
            stopped[0] = True

    ### resuming when the async called has been fired
    proc = X.spawn()

    proc.pause()
    assert not proc.is_running
    assert proc.is_alive
    assert proc.is_paused

    with assert_raises(ActorNotRunning):
        proc.pause()

    retval = random.random()
    mock_d.callback(retval)

    assert not proc.d.called, "a paused actor should not be resumed when the call it's waiting on completes"

    proc.resume()

    assert proc.d.called

    assert async_result[0] == retval

    ### resuming when the async call has NOT been fired
    mock_d = Deferred()
    proc2 = X.spawn()

    proc2.pause()
    proc2.resume()

    ### can't resume twice
    with assert_raises(ActorAlreadyRunning, "it should not be possible to resume an actor twice"):
        proc2.resume()

    ### stopping
    mock_d = Deferred()
    proc3 = X.spawn()

    proc3.stop()
    with assert_raises(ActorAlreadyStopped):
        proc3.stop()

    assert stopped[0]

    with assert_raises(ActorAlreadyStopped):
        proc3.start()
    with assert_raises(ActorAlreadyStopped):
        proc3.resume()

    ### stopping a paused actor
    mock_d = Deferred()
    proc4 = X.spawn()

    proc4.pause()
    proc4.stop()

    assert stopped[0]


def test_stopping_cancels_the_deferred_on_hold():
    cancelled = [False]
    mock_d = Deferred(lambda _: cancelled.__setitem__(0, True))

    @actor
    def X(self):
        yield mock_d

    X.spawn().stop()

    assert cancelled[0]


def test_actor_does_not_have_to_catch_actorstopped():
    @actor
    def X(self):
        yield Deferred()
    proc = X.spawn()
    with assert_not_raises(ActorStopped):
        proc.stop()


def test_actor_must_exit_after_being_stopped():
    # actor that violates the rule
    @actor
    def X(self):
        while True:
            try:
                yield Deferred()
            except ActorStopped:
                pass
    proc = X.spawn()
    with assert_raises(ActorRefusedToStop, "actor should not be allowed to continue working when stopped"):
        proc.stop()

    # actor that complies with the rule
    @actor
    def Proc2(self):
        while True:
            try:
                yield Deferred()
            except ActorStopped:
                break
    proc2 = Proc2.spawn()
    with assert_not_raises(ActorRefusedToStop):
        proc2.stop()


def test_actor_with_args():
    passed_values = [None, None]

    @actor
    def Proc(self, a, b):
        yield
        passed_values[:] = [a, b]

    Proc.spawn(1, b=2)
    assert passed_values == [1, 2]


def test_actor_doesnt_require_generator():
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

    c = Actor()
    d = c.get()
    with assert_raises(QueueUnderflow):
        c.get()

    c = Actor()
    d = c.get()
    d.addErrback(lambda f: f.trap(CancelledError))
    d.cancel()
    with assert_not_raises(QueueUnderflow):
        c.get()


def test_spawn_child_actor():
    a1 = Actor.spawn()

    ###########################
    @a1.spawn
    def a2(self):
        yield sleep(1.0)
    assert a2.parent == a1

    ###########################
    a2.stop()
    assert deferred_result(a1.get()) == ('exit', a2, ActorStopped), \
        "child actor forced exit should be sent to its parent"

    ###########################
    a1 = Actor()
    a2 = a1.spawn(run_with_error)
    msg = deferred_result(a1.get())
    assert msg[0:2] == ('exit', a2) and isinstance(msg[2], MockException), \
        "child actor errors should be sent to its parent"

    ###########################
    for retval in [random.random(), None]:
        a1 = Actor.spawn()
        a2 = a1.spawn(lambda self: retval)
        assert ('exit', a2, retval) == deferred_result(a1.get()), \
            "child actor return value should be sent to its parent"


def test_spawn_root_actor():
    a1 = Actor()
    assert not a1.parent

    ###########################
    a = run_with_error()
    with assert_not_raises(MockException, "root actor errors should be returned asynchronously"):
        a.start()

    ###########################
    with assert_raises(MockException, "root actor errors are returned to the code that spawned it"):
        deferred_result(a.d)

    ###########################
    @actor
    def X(self):
        yield fail(MockException())
    proc = X.spawn()
    with assert_raises(MockException, "root actor errors are returned to the code that spawned it"):
        deferred_result(proc.d)


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
        except ActorStopped:
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
        except ActorStopped:
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
        except ActorStopped:
            child_stopped[0] = True

    mock_d = Deferred()

    @actor
    def Parent(self):
        self.spawn(Stillborn)
        self.spawn(LongLivingChild)
        yield mock_d

    a = Parent.spawn()

    with assert_not_raises(ActorNotRunning):
        a.pause()

    with assert_not_raises(ActorAlreadyStopped):
        a.resume()

    a.stop()

    assert child_stopped[0]


def test_actor_finishing_before_child():
    child_stopped = [False]

    @actor
    def Child(self):
        try:
            yield Deferred()
        except ActorStopped:
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
        except ActorStopped:
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
    def Parent3(self):
        c = self.spawn(run_with_error)
        try:
            yield self.join(c)
        except MockException:
            assert False

    Parent3.spawn()


class MockException(Exception):
    pass


@actor
def run_with_error(self):
    raise MockException()
