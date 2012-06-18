from __future__ import print_function

import random
import warnings

from twisted.internet.defer import QueueUnderflow, Deferred, succeed

from spinoff.actor import actor, baseactor, ActorStopped, ActorNotRunning, ActorAlreadyStopped, ActorAlreadyRunning
from spinoff.util import pattern as match
from spinoff.util.async import CancelledError
from spinoff.util.testing import deferred_result, assert_raises, assert_not_raises, assert_one_warning, MockActor, run, RootActor


warnings.simplefilter('always')


def test_base_actor_not_started():
    parent = MockActor()

    with assert_raises(ActorNotRunning):
        parent.send('whatev')


def test_base_actor():
    root, x = run(MockActor)

    msg = random.random()
    x.send(msg)
    assert x.clear() == [msg]

    x.pause()
    msg = random.random()
    x.send(msg)
    assert x.clear() == []

    x.resume()
    assert x.clear() == [msg]

    assert not root.messages

    x.stop()
    assert [('stopped', x)] == root.messages


def test_base_actor_error():
    @baseactor
    def B(self, message):
        raise MockException()
    _, b = run(B)
    with assert_not_raises(MockException):
        b.send('whatev')
    assert not b.d.called

    @actor
    def P(self):
        b = self.spawn(B)
        b.send('whatev')
        msg = deferred_result(self.get())
        assert msg == ('error', b, (match.InstanceOf(MockException), match.Any), True)
    run(P)


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

    run(A, raise_only_asserts=True)

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

    root, a = run(A)

    a.pause()
    a.stop()
    assert [('stopped', a, 'refused')] == root.messages, root.messages


def test_failure_while_stopping():
    mock_d = Deferred()
    print(mock_d)

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

    root, mock = run(Mock)

    c = actor(lambda self: self.put('msg-1'))()
    c._parent = root
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

    proc = Proc()
    root = RootActor.spawn()
    proc._parent = root
    assert not called[0], "creating an actor should not automatically start the coroutine in it"

    proc.start()

    with assert_raises(ActorAlreadyRunning):
        proc.start()

    mock_d.callback(None)
    assert called[0] == 2, "the coroutine in an actor should complete as normal"
    assert not proc.is_alive
    assert root.messages == [('stopped', proc)]
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

    root, a = run(A, raise_only_asserts=True)
    assert not a.is_alive

    assert [('error', a, (exc, match.Any), False)] == root.messages


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

    root, p = run(X)
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
    root, proc = run(X)

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
    assert root.messages == [('stopped', proc)]

    ### resuming when the async call has NOT been fired
    mock_d = Deferred()
    root, proc2 = run(X)

    proc2.pause()
    proc2.resume()

    assert not stopped[0]
    assert not root.messages

    ### resuming when the async call has failed
    mock_d = Deferred()
    exception_caught = [False]

    @actor
    def Y(self):
        try:
            yield mock_d
        except MockException:
            exception_caught[0] = True

    root, x = run(Y)
    x.pause()
    mock_d.errback(MockException())
    x.resume()
    assert exception_caught[0]

    ### can't resume twice
    with assert_raises(ActorAlreadyRunning, "it should not be possible to resume an actor twice"):
        proc2.resume()

    ### stopping
    mock_d = Deferred()
    root, proc3 = run(X)

    proc3.stop()
    with assert_raises(ActorAlreadyStopped):
        proc3.stop()

    assert stopped[0]
    assert [('stopped', proc3)] == root.messages

    with assert_raises(ActorAlreadyStopped):
        proc3.start()
    with assert_raises(ActorAlreadyStopped):
        proc3.resume()

    ### stopping a paused actor
    mock_d = Deferred()
    root, proc4 = run(X)

    proc4.pause()
    proc4.stop()

    assert stopped[0]
    assert [('stopped', proc4)] == root.messages, root.messages


def test_stopping_cancels_the_deferred_on_hold():
    cancelled = [False]
    mock_d = Deferred(lambda _: cancelled.__setitem__(0, True))

    @actor
    def X(self):
        yield mock_d

    root, x = run(X)
    x.stop()

    assert cancelled[0]


def test_actor_does_not_have_to_catch_actorstopped():
    @actor
    def X(self):
        yield Deferred()
    root, proc = run(X)
    with assert_not_raises(ActorStopped):
        proc.stop()
    assert [('stopped', proc)] == root.messages


def test_actor_must_exit_after_being_stopped():
    # actor that violates the rule
    @actor
    def X(self):
        while True:
            try:
                yield Deferred()
            except ActorStopped:
                pass
    root, proc = run(X)
    proc.stop()
    assert [('stopped', proc, 'refused')] == root.messages, \
        "actor should not be allowed to continue working when stopped"

    # actor that complies with the rule
    @actor
    def Proc2(self):
        while True:
            try:
                yield Deferred()
            except ActorStopped:
                break
    root, proc2 = run(Proc2)
    proc2.stop()
    assert [('stopped', proc2)] == root.messages, root.messages


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

    root, proc = run(Proc)
    assert [('stopped', proc)] == root.messages

    @actor
    def Proc2(self):
        raise MockException()
    root, proc = run(Proc2, raise_only_asserts=True)


def test_get():
    def _make_getter(filter=None):
        @actor
        def ret(self):
            received_msg.append((yield self.get(filter=filter)))
        return ret

    ###
    received_msg = []
    root, x = run(_make_getter())
    x.send('foo')
    assert ['foo'] == received_msg

    ###
    received_msg = []
    tmp = random.random()
    root, x = run(_make_getter(filter=('foo', match._)))
    x.send(('foo', tmp))
    # x, = deferred_result(c.get(filter=('foo', match._)))
    assert [(tmp,)] == received_msg, received_msg

    ###
    received_msg = []
    tmp = random.random()
    root, c = run(_make_getter(filter=('baz', match._)))
    c.send(('foo', tmp))
    assert received_msg == []

    c.send(('baz', tmp))
    assert received_msg == [(tmp,)]


def test_get_removes_message_from_inbox():
    @actor
    def X(self):
        yield self.get()
        msg_d = self.get()
        assert not msg_d.called

    root, x = run(X)
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
    root, a = run(Parent)

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

    root, a = run(Parent)

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

    root, p = run(Parent)
    assert child_stopped[0]
    assert not p.is_running
    assert [('stopped', p)] == root.messages


class MockException(Exception):
    pass


@actor
def run_with_error(self):
    raise MockException()
