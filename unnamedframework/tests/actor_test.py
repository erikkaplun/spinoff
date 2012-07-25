from __future__ import print_function

import random
import warnings

from twisted.internet.defer import QueueUnderflow, Deferred, succeed, returnValue

from unnamedframework.actor import Process, Actor, process, actor, NotRunning, AlreadyStopped, AlreadyRunning, UnhandledMessage
from unnamedframework.util.pattern_matching import ANY, IS_INSTANCE, IGNORE
from unnamedframework.util.async import CancelledError
from unnamedframework.util.testing import (
    deferred_result, assert_raises, assert_not_raises, assert_one_warning,
    MockActor, run, Container, NOT, contain, deref, assert_no_warnings)


warnings.simplefilter('always')


def test_base_actor_not_started():
    with contain(MockActor, start_automatically=False) as (container, x):
        with assert_raises(NotRunning):
            x.send('whatev')

        deref(x).stop()


def test_base_actor():
    with contain(MockActor) as (container, x):
        msg = random.random()
        x.send(msg)
        assert deref(x).clear() == [msg]

        deref(x).pause()
        msg = random.random()
        x.send(msg)
        assert deref(x).clear() == []

        deref(x).resume()
        assert deref(x).clear() == [msg]

        assert not container.messages

        deref(x).stop()
        container.consume_message(('stopped', x))


def test_base_actor_error():
    @actor
    def SomeActor(self, message):
        raise MockException()

    with contain(SomeActor) as (container, some_actor):
        with assert_not_raises(MockException):
            some_actor.send('whatev')
        assert not container.has_message(('stopped', some_actor))
        container.consume_message(('error', some_actor, (IS_INSTANCE(MockException), IGNORE(ANY))))


def test_failure_with_children():
    child_stopped = [False]

    @process
    def Child(self):
        try:
            yield Deferred()
        except GeneratorExit:
            child_stopped[0] = True

    @process
    def A(self):
        self.spawn(Child)
        raise MockException()

    with Container(A) as container:
        container.ignore_non_assertions()

    assert child_stopped[0]


def test_actor_refuses_to_stop():
    @process
    def A(self):
        try:
            yield Deferred()
        except GeneratorExit:
            pass
        yield succeed(None)
        assert False

    with contain(A) as (container, a):
        deref(a).pause()
        deref(a).stop()
        container.consume_message(('error', a, (IGNORE(ANY), IGNORE(ANY))))


def test_failure_while_stopping():
    mock_d = Deferred()

    @process
    def A(self):
        try:
            yield mock_d
        except GeneratorExit:
            raise MockException()

    with contain(A) as (r, a):
        assert deref(a).is_running
        with assert_not_raises(MockException):
            deref(a).stop()
        r.consume_message(('error', a, (IGNORE(ANY), IGNORE(ANY))))


def test_connect_and_put():
    received_msg = []

    @process
    def Mock(self):
        received_msg.append((yield self.get()))

    with contain(Mock) as (container, mock):
        c = process(lambda self: self.put('msg-1'))()
        c._parent = container.ref
        c.connect(mock)
        c.start()

    assert received_msg == ['msg-1'], received_msg


def test_flow():
    called = [0]

    mock_d = Deferred()

    @process
    def Proc(self):
        called[0] += 1
        yield mock_d
        called[0] += 1

    with contain(Proc, start_automatically=False) as (container, proc):
        assert not called[0], "creating an process should not automatically start the coroutine in it"
        deref(proc).start()
        with assert_raises(AlreadyRunning):
            deref(proc).start()

        mock_d.callback(None)
        assert called[0] == 2, "the coroutine in an process should complete as normal"
        assert not deref(proc).is_alive
        container.consume_message(('stopped', proc))
        assert not deref(proc).is_alive


def test_exception():
    mock_d = Deferred()
    exception_caught = [False]

    @process
    def Y(self):
        try:
            yield mock_d
        except MockException:
            exception_caught[0] = True

    with contain(Y):
        mock_d.errback(MockException())
        assert exception_caught[0]


def test_baseactor_failure():
    exc = MockException()

    @actor
    def A(self, message):
        raise exc

    with contain(A) as (container, a):
        a.send(None)
        container.consume_message(('error', a, (exc, IGNORE(ANY))))
        assert ('stopped', ANY) not in container.messages
        assert deref(a).is_alive


def test_failure():
    exc = MockException()

    @process
    def A(self):
        raise exc

    with contain(A) as (container, a):
        container.consume_message(('error', a, (exc, IGNORE(ANY))))
        assert not deref(a).is_alive


def test_yielding_a_non_deferred():
    @process
    def Actor1(self):
        tmp = random.random()
        ret = yield tmp
        assert ret == tmp
    run(Actor1)

    @process
    def Actor2(self):
        ret = yield
        assert ret is None
    run(Actor2)


def test_pending_failures_are_discarded_with_a_warning():
    mock_d = Deferred()

    @process
    def X(self):
        yield mock_d

    with contain(X) as (container, p):
        deref(p).pause()

        mock_d.errback(Exception())
        with assert_one_warning():
            deref(p).stop()


def test_pausing_resuming_and_stopping():
    stopped = [False]

    mock_d = Deferred()
    retval = random.random()

    @process
    def X(self):
        try:
            ret = yield mock_d
            assert ret == retval
        except GeneratorExit:
            stopped[0] = True

    ### resuming when the async called has been fired
    with contain(X) as (container, proc):
        deref(proc).pause()
        assert not deref(proc).is_running
        assert deref(proc).is_alive
        assert deref(proc).is_paused

        with assert_raises(NotRunning):
            deref(proc).pause()

        mock_d.callback(retval)

        assert not container.has_message(('stopped', proc)), \
            "a paused process should not be resumed when the call it's waiting on completes"

        deref(proc).resume()

        container.consume_message(('stopped', proc))

    ### resuming when the async call has NOT been fired
    mock_d = Deferred()
    with contain(X) as (container, proc2):
        deref(proc2).pause()
        deref(proc2).resume()

        assert not stopped[0]
        assert not container.messages

    ### resuming when the async call has failed
    mock_d = Deferred()
    exception_caught = [False]

    @process
    def Y(self):
        try:
            yield mock_d
        except MockException:
            exception_caught[0] = True
        yield Deferred()

    with contain(Y) as (container, x):
        deref(x).pause()
        mock_d.errback(MockException())
        deref(x).resume()
        assert exception_caught[0]

        ### can't resume twice
        with assert_raises(AlreadyRunning, "it should not be possible to resume an process twice"):
            deref(x).resume()

    ### stopping
    mock_d = Deferred()
    with contain(X) as (container, proc3):
        deref(proc3).stop()
        with assert_raises(AlreadyStopped):
            deref(proc3).stop()

        assert stopped[0]
        container.consume_message(('stopped', proc3))

        with assert_raises(AlreadyStopped):
            deref(proc3).start()
        with assert_raises(AlreadyStopped):
            deref(proc3).resume()

    ### stopping a paused process
    mock_d = Deferred()
    with contain(X) as (container, proc4):
        deref(proc4).pause()
        deref(proc4).stop()

        assert stopped[0]
        container.consume_message(('stopped', proc4))


def test_stopping_cancels_the_deferred_on_hold():
    cancelled = [False]
    mock_d = Deferred(lambda _: cancelled.__setitem__(0, True))

    @process
    def X(self):
        yield mock_d

    with contain(X) as (container, x):
        deref(x).stop()

    assert cancelled[0]


def test_actor_does_not_have_to_catch_actorstopped():
    @process
    def X(self):
        yield Deferred()

    with contain(X) as (container, proc):
        with assert_not_raises(GeneratorExit):
            deref(proc).stop()
        container.consume_message(('stopped', proc))


def test_actor_with_args():
    passed_values = [None, None]

    @process
    def Proc(self, a, b):
        yield
        passed_values[:] = [a, b]

    run(Proc(1, b=2))
    assert passed_values == [1, 2]


def test_actor_doesnt_require_generator():
    @process
    def Proc(self):
        pass

    with contain(Proc) as (container, proc):
        container.consume_message(('stopped', proc))

    @process
    def Proc2(self):
        raise MockException()

    with Container(Proc2) as container:
        container.consume_message(('error', IGNORE(ANY), (NOT(IS_INSTANCE(AssertionError)), IGNORE(ANY))))


def test_get():
    def _make_getter(filter=None):
        @process
        def ret(self):
            received_msg.append((yield self.get(filter=filter)))
        return ret

    ###
    received_msg = []
    with contain(_make_getter()) as (container, x):
        x.send('foo')
        assert ['foo'] == received_msg

    ###
    received_msg = []
    tmp = random.random()
    with contain(_make_getter(('foo', ANY))) as (container, x):
        x.send(('foo', tmp))
        container.ignore_non_assertions()
    # container.raise_errors(only_asserts=False)
    assert [(tmp, )] == received_msg, received_msg

    ###
    received_msg = []
    tmp = random.random()
    with contain(_make_getter(('baz', ANY))) as (container, c):
        c.send(('foo', tmp))
        assert received_msg == []

        c.send(('baz', tmp))
        assert received_msg == [(tmp,)]

        container.ignore_non_assertions()

    ###
    @process
    def X(self):
        yield Deferred()

    with contain(X) as (container, x):
        x.send('foo')
        deref(x).get(filter='foo')


def test_get_removes_message_from_inbox():
    called = []

    @process
    def X(self):
        called.append(1)

        yield self.get()
        msg_d = self.get()
        assert not msg_d.called

    with contain(X) as (_, x):
        x.send('whatev')

    assert called


def test_inbox_underflow():
    @process
    def GetTwice(self):
        self.get()
        with assert_raises(QueueUnderflow):
            self.get()
    run(GetTwice)

    @process
    def GetThenCancelAndGetAgain(self):
        msg_d = self.get()
        msg_d.addErrback(lambda f: f.trap(CancelledError))
        msg_d.cancel()
        self.get()
    run(GetThenCancelAndGetAgain)


def test_spawn_child_actor():
    @process
    def Child(self):
        yield Deferred()

    @process
    def Parent(self):
        c = self.spawn(Child)
        assert deref(deref(c).parent) == self

        deref(c).stop()
        msg = deferred_result(self.get())
        assert msg == ('stopped', c), "child process forced exit should be sent to its parent"

        c = self.spawn(run_with_error)
        msg = deferred_result(self.get())
        assert msg == ('error', c, (IS_INSTANCE(MockException), ANY))

    run(Parent)

    child_stopped = [False]
    arg = random.random()

    @process
    def ChildWithArgs(self, foo):
        assert foo == arg
        try:
            yield Deferred()
        except GeneratorExit:
            child_stopped[0] = True

    @process
    def Parent2(self):
        self.spawn(ChildWithArgs(arg))

    with contain(Parent2) as (container, parent2):
        container.ignore_non_assertions()

    assert child_stopped[0]

    class Parent3(Process):
        def __init__(self):
            super(Parent3, self).__init__()
            self.spawn(Child)

    with contain(Parent3, start_automatically=False) as (_, parent3):
        with assert_not_raises(AlreadyRunning, "it should be possible for actors to spawn children in the constructor"):
            deref(parent3).start()


def test_actor_returns_value_raises_a_warning():
    @process
    def SomeActor(self):
        yield
        returnValue(123)

    with Container() as container:
        with assert_one_warning():
            container.spawn(process(lambda self: 123))
        with assert_one_warning():
            container.spawn(SomeActor)


def test_returnvalue_during_cleanup():
    @process
    def X(self):
        try:
            yield Deferred()
        except GeneratorExit:
            returnValue(123)

    with contain(X) as (contaner, x):
        with assert_one_warning():
            deref(x).stop()


def test_pausing_resuming_and_stopping_actor_with_children_does_the_same_with_children():
    children = []
    child_stopped = [False]

    @process
    def Child(self):
        try:
            yield Deferred()
        except GeneratorExit:
            child_stopped[0] = True

    @process
    def Parent(self):
        children.append(self.spawn(Child))
        yield Deferred()

    with contain(Parent) as (container, a):
        deref(a).pause()
        assert all(deref(x).is_paused for x in children)

        deref(a).resume()
        assert all(not deref(x).is_paused for x in children)

        deref(a).stop()
        assert all(not deref(x).is_alive for x in children)
        assert child_stopped[0]


def test_pausing_and_stoping_actor_with_some_finished_children():
    @process
    def Stillborn(self):
        yield

    child_stopped = [False]

    @process
    def LongLivingChild(self):
        try:
            yield Deferred()
        except GeneratorExit:
            child_stopped[0] = True

    mock_d = Deferred()

    @process
    def Parent(self):
        self.spawn(Stillborn)
        self.spawn(LongLivingChild)
        yield mock_d

    with contain(Parent) as (container, a):
        with assert_not_raises(NotRunning):
            deref(a).pause()
        with assert_not_raises(AlreadyStopped):
            deref(a).resume()
        deref(a).stop()
        assert child_stopped[0]


def test_actor_finishing_before_child_stops_its_children():
    child_stopped = [False]

    @process
    def Child(self):
        try:
            yield Deferred()
        except GeneratorExit:
            child_stopped[0] = True

    @process
    def Parent(self):
        self.spawn(Child)

    with contain(Parent) as (container, p):
        assert child_stopped[0]
        assert not deref(p).is_running
        container.consume_message(('stopped', p))


def test_actor_failinig_stops_its_children():
    child_stopped = [False]

    @process
    def Child(self):
        try:
            yield Deferred()
        except GeneratorExit:
            child_stopped[0] = True

    @process
    def Parent(self):
        self.spawn(Child)
        raise Exception()

    with Container(Parent) as container:
        container.consume_message(('error', IGNORE(ANY), IGNORE(ANY)))

    assert child_stopped[0]


def test_before_start_raises():
    class SomeActor(Actor):
        def _before_start(self):
            raise MockException

    with contain(SomeActor) as (container, some_actor):
        container.consume_message(('error', IGNORE(ANY), (IS_INSTANCE(MockException), IGNORE(ANY))))


def test_on_stop_raises():
    class SomeActor(Actor):
        def _on_stop(self):
            raise MockException

    with contain(SomeActor) as (container, some_actor):
        deref(some_actor).stop()
        container.consume_message(('error', IGNORE(ANY), (IS_INSTANCE(MockException), IGNORE(ANY))))


def test_unhandled_message():
    @actor
    def A(self, message):
        raise UnhandledMessage

    with contain(A) as (container, a):
        a.send('foo')
        assert ('error', a, ANY) not in container.messages, \
            "UnhandledMessage exceptions should not be treated as errors"


def test_unhandled_error_message():
    # process that does not handle child errors
    @actor
    def A(self, message):
        raise UnhandledMessage

    with contain(A) as (_, a):
        with assert_one_warning("Unhandled child errors should emit a warning"):
            a.send(('error', 'whatever', (Exception(), 'dummy traceback')))

    # process that handles child errors
    @actor
    def B(self, message):
        pass

    with contain(B) as (_, a):
        with assert_no_warnings():
            a.send(('error', 'whatever', (Exception(), 'dummy traceback')))


def test_supervision():
    """Essentially just tests error message handling but with actual actors."""

    @process
    def Child(self):
        raise MockException()

    @actor
    def GoodParent(self, message):
        pass
    GoodParent.def_before_start(lambda self: self.spawn(Child))

    @actor
    def BadParent(self, message):
        raise UnhandledMessage
    BadParent.def_before_start(lambda self: self.spawn(Child))

    with assert_no_warnings():
        run(GoodParent)

    with assert_one_warning():
        run(BadParent)


# TODO: unhandled messages with Erlang style actors


class MockException(Exception):
    pass


@process
def run_with_error(self):
    raise MockException()
