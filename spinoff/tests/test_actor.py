from __future__ import print_function

from twisted.internet.defer import QueueUnderflow, Deferred, inlineCallbacks, returnValue
from twisted.internet.task import Clock

from spinoff.actor.actor import Actor
from spinoff.util.async import CancelledError
from spinoff.util.microprocess import microprocess
from spinoff.util.testing import deferred_result, assert_raises, assert_not_raises
from spinoff.actor.actor import ActorDoesNotSupportSuspending
from spinoff.util.testing import assert_one_warning, assert_no_warnings
from spinoff.util.microprocess import CoroutineStopped, CoroutineNotRunning, CoroutineAlreadyStopped
from spinoff.util.async import sleep
from spinoff.actor.actor import actor


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
        a1.spawn(make_actor_cls(lambda self: None))

    # ...with a plain function
    with assert_one_warning():
        a1.spawn(make_actor_cls(lambda self: 123))

    # ...with inlineCallbacks
    def bla(self):
        yield
        returnValue(123)
    with assert_one_warning():
        a1.spawn(make_actor_cls(inlineCallbacks(bla)))

    # ...with microprocesses + plain function
    with assert_one_warning():
        a1.spawn(make_actor_cls(microprocess(lambda self: 123)))

    # ... with microprocess + generator/inlineCallbacks
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


def test_pause_and_wake_actor():
    called = [0]
    d = Deferred()

    @microprocess
    def run(self):
        called[0] += 1
        yield d
        called[0] += 1
    a = make_actor_cls(run)()
    a.start()
    assert called[0] == 1

    a.suspend()
    d.callback(None)
    assert called[0] == 1

    assert not a.is_active
    assert a.is_alive
    assert a.is_suspended

    a.wake()
    assert called[0] == 2


def test_kill_actor():
    killed = [False]

    @microprocess
    def run(self):
        try:
            yield Deferred()
        except CoroutineStopped:
            killed[0] = True
    a = make_actor_cls(run)()
    a.start()
    a.kill()

    assert killed[0]


def test_pause_actor_without_microprocesses():
    d = Deferred()

    @inlineCallbacks
    def run(self):
        yield d

    a = make_actor_cls(run)()
    a.start()

    with assert_raises(ActorDoesNotSupportSuspending):
        a.suspend()
    with assert_raises(ActorDoesNotSupportSuspending):
        a.wake()


def test_pausing_actor_with_children_pauses_the_children():
    children = []
    child_killed = [False]

    @microprocess
    def child(self):
        try:
            yield Deferred()
        except CoroutineStopped:
            child_killed[0] = True

    @microprocess
    def parent(self):
        children.append(self.spawn(make_actor_cls(child)))
        yield Deferred()
    a = make_actor_cls(parent)()
    a.start()

    a.suspend()
    assert all(x.is_suspended for x in children)

    a.wake()
    assert all(not x.is_suspended for x in children)

    a.kill()
    assert all(not x.is_alive for x in children)
    assert child_killed[0]


def test_suspending_and_killing_actor_with_some_finished_children():
    @microprocess
    def stillborn(self):
        yield

    child_killed = [False]

    @microprocess
    def long_living_child(self):
        try:
            yield Deferred()
        except CoroutineStopped:
            child_killed[0] = True

    mock_d = Deferred()

    @microprocess
    def parent(self):
        self.spawn(make_actor_cls(stillborn))
        self.spawn(make_actor_cls(long_living_child))
        yield mock_d

    a = make_actor_cls(parent)()
    a.start()

    with assert_not_raises(CoroutineNotRunning):
        a.suspend()

    with assert_not_raises(CoroutineAlreadyStopped):
        a.wake()

    a.kill()

    assert child_killed[0]


def test_actor_finishing_before_child():
    child_stopped = [False]

    @microprocess
    def child(self):
        try:
            yield Deferred()
        except CoroutineStopped:
            child_stopped[0] = True

    @microprocess
    def parent(self):
        self.spawn(make_actor_cls(child))

    a = make_actor_cls(parent)()
    a.start()
    assert child_stopped[0]


def test_actor_joins_child():
    clock = Clock()

    child_exit = [None]

    @actor
    def Child(self):
        try:
            yield sleep(1.0, clock)
        except CoroutineStopped:
            child_exit[0] = 'forced'
        else:
            child_exit[0] = 'normal'

    @actor
    def Parent(self):
        child = self.spawn(Child)
        yield self.join(child)

    p = Parent.spawn()
    assert p.is_active, "an actor should be waiting for a joining child actor to complete"

    clock.advance(1.0)
    assert not p.is_alive, "an actor should die when a joining child actor completes"

    assert child_exit[0] == 'normal', "a joining child of an actor should die a natural death"

    ##########################
    # join all children

    @actor
    def Parent2(self):
        for _ in range(3):
            self.spawn(Child)
        yield self.join_children()

    p = Parent2.spawn()
    assert p.is_active

    clock.advance(1.0)
    assert not p.is_alive


def make_actor_cls(run_fn):
    class MockActor(Actor):
        run = run_fn
    MockActor.__name__ = run_fn.__name__
    return MockActor


class MockException(Exception):
    pass


def run_with_error(self):
    raise MockException()
