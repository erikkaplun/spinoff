from __future__ import print_function

import random
import warnings

from twisted.internet.defer import QueueUnderflow, Deferred, succeed

from spinoff.actor import Actor, actor, baseactor, ActorStopped, ActorNotRunning, ActorAlreadyStopped, ActorAlreadyRunning
from spinoff.util.pattern_matching import ANY, IS_INSTANCE
from spinoff.util.async import CancelledError
from spinoff.util.testing import deferred_result, assert_raises, assert_not_raises, assert_one_warning, MockActor, run, Container


warnings.simplefilter('always')


def test_base_actor_not_started():
    actor = MockActor()

    with assert_raises(ActorNotRunning):
        actor.send('whatev')


def test_base_actor():
    container, actor = run(MockActor)

    msg = random.random()
    actor.send(msg)
    assert actor.clear() == [msg]

    actor.pause()
    msg = random.random()
    actor.send(msg)
    assert actor.clear() == []

    actor.resume()
    assert actor.clear() == [msg]

    assert not container.messages

    actor.stop()
    assert [('stopped', actor)] == container.messages


def test_base_actor_error():
    @baseactor
    def SomeActor(self, message):
        raise MockException()
    _, some_actor = run(SomeActor)
    with assert_not_raises(MockException):
        some_actor.send('whatev')
    assert not some_actor.d.called

    @actor
    def Parent(self):
        some_actor = self.spawn(SomeActor)
        some_actor.send('whatev')
        msg_d = self.get(('error', some_actor, (IS_INSTANCE(MockException), ANY), True))
        assert msg_d.called
    run(Parent)


def test_failure_with_children():
    child_stopped = [False]

    @actor
    def Child(self):
        try:
            yield Deferred()
        except ActorStopped:
            child_stopped[0] = True

    @actor
    def A(self):
        self.spawn(Child)
        raise MockException()

    with Container(A) as (container, _):
        container.consume_message(('error', ANY, (IS_INSTANCE(AssertionError), ANY), ANY))

    assert child_stopped[0]


def test_actor_refuses_to_stop():
    mock_d = Deferred()

    @actor
    def A(self):
        try:
            yield mock_d
        except ActorStopped:
            pass
        yield succeed(None)
        assert False

    container, a = run(A)

    a.pause()
    a.stop()
    assert [('stopped', a, 'refused')] == container.messages, container.messages


def test_failure_while_stopping():
    mock_d = Deferred()

    @actor
    def A(self):
        try:
            yield mock_d
        except ActorStopped:
            raise MockException()

    r, a = run(A)
    assert a.is_running
    with assert_not_raises(MockException):
        a.stop()
    assert len(r.messages) == 1 and r.messages[0][0:3] == ('stopped', a, 'unclean')


def test_connect_and_put():
    received_msg = []

    @actor
    def Mock(self):
        received_msg.append((yield self.get()))

    container, mock = run(Mock)

    c = actor(lambda self: self.put('msg-1'))()
    c._parent = container
    c.connect(mock)
    c.start()

    assert received_msg == ['msg-1'], received_msg


def test_flow():
    called = [0]

    mock_d = Deferred()

    @actor
    def Proc(self):
        called[0] += 1
        yield mock_d
        called[0] += 1

    with Container(Proc, start_automatically=False) as (container, proc):
        assert not called[0], "creating an actor should not automatically start the coroutine in it"
        proc.start()
        with assert_raises(ActorAlreadyRunning):
            proc.start()

        mock_d.callback(None)
        assert called[0] == 2, "the coroutine in an actor should complete as normal"
        assert not proc.is_alive
        container.consume_message(('stopped', proc))
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

    run(Y)
    mock_d.errback(MockException())
    assert exception_caught[0]


def test_failure():
    exc = MockException()

    @actor
    def A(self):
        raise exc

    with Container(A) as (container, a):
        container.consume_message(('error', a, (exc, ANY), False))
        assert not a.is_alive


def test_yielding_a_non_deferred():
    @actor
    def Actor1(self):
        tmp = random.random()
        ret = yield tmp
        assert ret == tmp
    run(Actor1)

    @actor
    def Actor2(self):
        ret = yield
        assert ret is None
    run(Actor2)


def test_pending_failures_are_discarded_with_a_warning():
    mock_d = Deferred()

    @actor
    def X(self):
        yield mock_d

    container, p = run(X)
    p.pause()

    mock_d.errback(Exception())
    with assert_one_warning():
        p.stop()


def test_pausing_resuming_and_stopping():
    stopped = [False]

    mock_d = Deferred()
    retval = random.random()

    @actor
    def X(self):
        try:
            ret = yield mock_d
            assert ret == retval
        except ActorStopped:
            stopped[0] = True

    ### resuming when the async called has been fired
    container, proc = run(X)

    proc.pause()
    assert not proc.is_running
    assert proc.is_alive
    assert proc.is_paused

    with assert_raises(ActorNotRunning):
        proc.pause()

    mock_d.callback(retval)

    assert not proc.d.called, "a paused actor should not be resumed when the call it's waiting on completes"

    proc.resume()

    assert proc.d.called
    container.consume_message(('stopped', proc))

    ### resuming when the async call has NOT been fired
    mock_d = Deferred()
    container, proc2 = run(X)

    proc2.pause()
    proc2.resume()

    assert not stopped[0]
    assert not container.messages

    ### resuming when the async call has failed
    mock_d = Deferred()
    exception_caught = [False]

    @actor
    def Y(self):
        try:
            yield mock_d
        except MockException:
            exception_caught[0] = True

    container, x = run(Y)
    x.pause()
    mock_d.errback(MockException())
    x.resume()
    assert exception_caught[0]

    ### can't resume twice
    with assert_raises(ActorAlreadyRunning, "it should not be possible to resume an actor twice"):
        proc2.resume()

    ### stopping
    mock_d = Deferred()
    container, proc3 = run(X)

    proc3.stop()
    with assert_raises(ActorAlreadyStopped):
        proc3.stop()

    assert stopped[0]
    assert [('stopped', proc3)] == container.messages

    with assert_raises(ActorAlreadyStopped):
        proc3.start()
    with assert_raises(ActorAlreadyStopped):
        proc3.resume()

    ### stopping a paused actor
    mock_d = Deferred()
    container, proc4 = run(X)

    proc4.pause()
    proc4.stop()

    assert stopped[0]
    assert [('stopped', proc4)] == container.messages, container.messages


def test_stopping_cancels_the_deferred_on_hold():
    cancelled = [False]
    mock_d = Deferred(lambda _: cancelled.__setitem__(0, True))

    @actor
    def X(self):
        yield mock_d

    container, x = run(X)
    x.stop()

    assert cancelled[0]


def test_actor_does_not_have_to_catch_actorstopped():
    @actor
    def X(self):
        yield Deferred()
    container, proc = run(X)
    with assert_not_raises(ActorStopped):
        proc.stop()
    assert [('stopped', proc)] == container.messages


def test_actor_must_exit_after_being_stopped():
    # actor that violates the rule
    @actor
    def X(self):
        while True:
            try:
                yield Deferred()
            except ActorStopped:
                pass
    container, proc = run(X)
    proc.stop()
    assert [('stopped', proc, 'refused')] == container.messages, \
        "actor should not be allowed to continue working when stopped"

    # actor that complies with the rule
    @actor
    def Proc2(self):
        while True:
            try:
                yield Deferred()
            except ActorStopped:
                break
    container, proc2 = run(Proc2)
    proc2.stop()
    assert [('stopped', proc2)] == container.messages, container.messages


def test_actor_with_args():
    passed_values = [None, None]

    @actor
    def Proc(self, a, b):
        yield
        passed_values[:] = [a, b]

    run(Proc(1, b=2))
    assert passed_values == [1, 2]


def test_actor_doesnt_require_generator():
    @actor
    def Proc(self):
        pass

    container, proc = run(Proc)
    assert [('stopped', proc)] == container.messages

    @actor
    def Proc2(self):
        raise MockException()

    with Container(Proc2) as (container, _):
        container.consume_message(('error', ANY, (IS_INSTANCE(AssertionError), ANY), ANY))


def test_get():
    def _make_getter(filter=None):
        @actor
        def ret(self):
            received_msg.append((yield self.get(filter=filter)))
        return ret

    ###
    received_msg = []
    container, x = run(_make_getter())
    x.send('foo')
    assert ['foo'] == received_msg

    ###
    received_msg = []
    tmp = random.random()
    container, x = run(_make_getter(('foo', ANY)))
    x.send(('foo', tmp))
    container.raise_errors(only_asserts=False)
    assert [tmp] == received_msg, received_msg

    ###
    received_msg = []
    tmp = random.random()
    container, c = run(_make_getter(('baz', ANY)))
    c.send(('foo', tmp))
    container.raise_errors(only_asserts=False)
    assert received_msg == []

    c.send(('baz', tmp))
    container.raise_errors(only_asserts=False)
    assert received_msg == [tmp]


def test_get_removes_message_from_inbox():
    @actor
    def X(self):
        yield self.get()
        msg_d = self.get()
        assert not msg_d.called

    container, x = run(X)
    x.send('whatev')


def test_inbox_underflow():
    @actor
    def GetTwice(self):
        self.get()
        with assert_raises(QueueUnderflow):
            self.get()
    run(GetTwice)

    @actor
    def GetThenCancelAndGetAgain(self):
        msg_d = self.get()
        msg_d.addErrback(lambda f: f.trap(CancelledError))
        msg_d.cancel()
        self.get()
    run(GetThenCancelAndGetAgain)


def test_spawn_child_actor():
    @actor
    def Child(self):
        yield Deferred()

    @actor
    def Parent(self):
        c = self.spawn(Child)
        assert c.parent == self

        c.stop()
        msg = deferred_result(self.get())
        assert msg == ('stopped', c), "child actor forced exit should be sent to its parent"

        c = self.spawn(run_with_error)
        msg = deferred_result(self.get())
        assert msg[:2] == ('error', c) and msg[3] is False and isinstance(msg[-2][0], MockException), \
            "child actor errors should be sent to its parent"

        for retval in [random.random(), None]:
            c = self.spawn(lambda self: retval)
            msg = deferred_result(self.get())
            assert ('stopped', c) == msg, "child actor return value is ignored %s" % repr(msg)

    run(Parent)

    chld_stopped = [False]
    arg = random.random()

    @actor
    def ChildWithArgs(self, foo):
        assert foo == arg
        try:
            yield Deferred()
        except ActorStopped:
            chld_stopped[0] = True

    @actor
    def Parent2(self):
        self.spawn(ChildWithArgs(arg))

    container, parent2 = run(Parent2, raise_only_asserts=False)

    assert chld_stopped[0]

    class Parent3(Actor):
        def __init__(self):
            super(Parent3, self).__init__()
            self.spawn(Child)

    with Container(Parent3, start_automatically=False) as (_, parent3):
        with assert_not_raises(ActorAlreadyRunning, "it should be possible for actors to spawn children in the constructor"):
            parent3.start()


def test_pausing_resuming_and_stopping_actor_with_children_does_the_same_with_children():
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
    container, a = run(Parent)

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

    container, a = run(Parent)

    with assert_not_raises(ActorNotRunning):
        a.pause()

    with assert_not_raises(ActorAlreadyStopped):
        a.resume()

    a.stop()
    assert child_stopped[0]


def test_actor_finishing_before_child_stops_its_children():
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

    container, p = run(Parent)
    assert child_stopped[0]
    assert not p.is_running
    assert [('stopped', p)] == container.messages


def test_actor_failinig_stops_its_children():
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
        raise Exception()

    with Container(Parent) as (container, _):
        container.consume_message(('error', ANY, (IS_INSTANCE(Exception), ANY), ANY))

    assert child_stopped[0]


class MockException(Exception):
    pass


@actor
def run_with_error(self):
    raise MockException()
