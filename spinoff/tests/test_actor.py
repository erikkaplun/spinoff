from twisted.internet.defer import QueueUnderflow, Deferred, inlineCallbacks, returnValue

from spinoff.actor.actor import Actor
from spinoff.util.async import CancelledError
from spinoff.util.microprocess import microprocess
from spinoff.util.testing import deferred_result, assert_raises, assert_not_raises
from spinoff.actor.actor import ActorDoesNotSupportSuspending
from spinoff.util.testing import assert_one_warning, assert_no_warnings


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


def test_pause_actor():
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


def make_actor_cls(run_fn=lambda self: None):
    class MockActor(Actor):
        run = run_fn
    return MockActor


class MockException(Exception):
    pass


def run_with_error(self):
    raise MockException()
