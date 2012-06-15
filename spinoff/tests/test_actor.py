from __future__ import print_function

import warnings

from twisted.internet.defer import QueueUnderflow, Deferred, returnValue
from twisted.internet.task import Clock

from spinoff.actor.actor import Actor
from spinoff.util.async import CancelledError
from spinoff.util.microprocess import microprocess
from spinoff.util.testing import deferred_result, assert_raises, assert_not_raises
from spinoff.util.testing import assert_one_warning, assert_no_warnings
from spinoff.util.microprocess import CoroutineStopped, CoroutineNotRunning, CoroutineAlreadyStopped
from spinoff.util.async import sleep
from spinoff.actor.actor import actor


warnings.simplefilter('always')


def test_basic():
    c = Actor()
    mock = Actor()
    c.connect('default', ('default', mock))

    c.put(message='msg-1')
    assert deferred_result(mock.get()) == 'msg-1'


def test_cancel_get():
    c = Actor()
    c._inboxes['default']
    d = c.get()
    with assert_raises(QueueUnderflow):
        c.get()

    ###
    c = Actor()
    c._inboxes['default']
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


def test_child_non_empty_return_values_raise_a_warning():
    a1 = Actor()

    # ...with a plain function without a return value
    with assert_no_warnings():
        a1.spawn(actor(lambda self: None))

    # ...with a plain function
    with assert_one_warning():
        a1.spawn(actor(lambda self: 123))

    # ... with microprocess + generator
    @microprocess
    def bla2(self):
        yield
        returnValue(123)
    with assert_one_warning():
        a1.spawn(make_actor_cls(bla2))


def test_root_actor_errors_are_returned_asynchronously():
    a = make_actor_cls(run_with_error)()
    with assert_not_raises(MockException):
        d = a.start()
    with assert_raises(MockException):
        deferred_result(d)


def test_child_actor_errors_are_sent_to_parent():
    a1 = Actor()
    a2 = a1.spawn(make_actor_cls(run_with_error))
    msg = deferred_result(a1.get('child-errors'))
    assert msg[0] == a2 and isinstance(msg[1], MockException)


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


def test_spawn_microprocess():
    bla = [False]

    @microprocess
    def P(self):
        bla[0] = True
        yield Deferred()

    @actor
    def A(self):
        self.spawn(P)
        yield Deferred()

    a = A.spawn()
    assert bla[0]

    a.stop()


def make_actor_cls(run_fn):
    class MockActor(Actor):
        run = run_fn
    MockActor.__name__ = run_fn.__name__
    return MockActor


class MockException(Exception):
    pass


def run_with_error(self):
    raise MockException()
