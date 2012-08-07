from __future__ import print_function

import random
import warnings

from twisted.internet.defer import QueueUnderflow, Deferred, succeed, returnValue

from spinoff.actor import Process, Actor, process, actor, UnhandledMessage, STOPPED, RUNNING
from spinoff.util.pattern_matching import ANY, IS_INSTANCE, IGNORE
from spinoff.util.async import CancelledError
from spinoff.util.testing import (
    deferred_result, assert_raises, assert_not_raises, assert_one_warning,
    MockActor, run, Container, NOT, contain, deref, assert_no_warnings)


warnings.simplefilter('always')


def test_base_actor_lifecycle():
    # -> stop
    with contain(MockActor) as (_, x):
        x << 'stop'

    # -> send -> stop
    with contain(MockActor) as (_, x):
        x << 'whatev'
        x << 'stop'

    # -> send -> stop -> restart -> send
    with contain(MockActor) as (_, x):
        x << 'whatev'
        x << 'stop'
        x << 'restart'
        x << 'whatev'
        msgs = deref(x).clear()
        assert msgs == ['whatev'], msgs

    # -> send -> restart without stopping -> send
    with contain(MockActor) as (_, x):
        x << 'whatev'

        x << 'restart'
        x << 'whatev'
        msgs = deref(x).clear()
        assert msgs == ['whatev']

    # -> restart -> send -> repeat 3 times
    with contain(MockActor) as (_, x):
        for _ in range(3):
            x << 'restart'
            x << 'whatev'
            msgs = deref(x).clear()
            assert msgs == ['whatev']


def test_inbox_survives_restart():
    received_msgs = []

    @process
    def BlockingMock(self):
        while True:
            received_msgs.append((yield self.get()))
            yield Deferred()

    with contain(BlockingMock) as (_, x):
        x << 'foo'
        x << 'foo'

        assert received_msgs == ['foo']
        received_msgs = []

        x << 'restart'
        assert received_msgs == ['foo']


def test_base_actor():
    with contain(MockActor) as (container, x):
        msg = random.random()
        x << msg
        assert deref(x).clear() == [msg]

        assert not container.messages

        x << 'stop'
        container.consume_message(('stopped', x))


def test_base_actor_error():
    @actor
    def SomeActor(self, message):
        raise MockException()

    with contain(SomeActor) as (container, some_actor):
        with assert_not_raises(MockException):
            some_actor << 'whatev'
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
        a << 'stop'
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
        assert deref(a)._state is RUNNING
        with assert_not_raises(MockException):
            a << 'stop'
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
        c._start()

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
        deref(proc)._start()

        mock_d.callback(None)
        assert called[0] == 2, "the coroutine in an process should complete as normal"
        assert deref(proc)._state is STOPPED
        container.consume_message(('stopped', proc))
        assert deref(proc)._state is STOPPED


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
        a << None
        container.consume_message(('error', a, (exc, IGNORE(ANY))))
        assert ('stopped', ANY) not in container.messages
        assert deref(a)._state is RUNNING


def test_failure():
    exc = MockException()

    @process
    def A(self):
        raise exc

    with contain(A) as (container, a):
        container.consume_message(('error', a, (exc, IGNORE(ANY))))
        assert deref(a)._state is STOPPED


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


def test_stopping_cancels_the_deferred_on_hold():
    cancelled = [False]
    mock_d = Deferred(lambda _: cancelled.__setitem__(0, True))

    @process
    def X(self):
        yield mock_d

    with contain(X) as (container, x):
        x << 'stop'

    assert cancelled[0]


def test_actor_does_not_have_to_catch_actorstopped():
    @process
    def X(self):
        yield Deferred()

    with contain(X) as (container, proc):
        with assert_not_raises(GeneratorExit):
            proc << 'stop'
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
        x << 'foo'
        assert ['foo'] == received_msg

    ###
    received_msg = []
    tmp = random.random()
    with contain(_make_getter(('foo', ANY))) as (container, x):
        x << ('foo', tmp)
        container.ignore_non_assertions()
    # container.raise_errors(only_asserts=False)
    assert [('foo', tmp)] == received_msg, received_msg

    ###
    received_msg = []
    tmp = random.random()
    with contain(_make_getter(('baz', ANY))) as (container, c):
        c << ('foo', tmp)
        assert received_msg == []

        c << ('baz', tmp)
        assert received_msg == [('baz', tmp,)]

        container.ignore_non_assertions()

    ###
    @process
    def X(self):
        yield Deferred()

    with contain(X) as (container, x):
        x << 'foo'
        deferred_result(deref(x).get(filter='foo'))


def test_get_removes_message_from_inbox():
    called = []

    @process
    def X(self):
        called.append(1)

        yield self.get()
        msg_d = self.get()
        assert not msg_d.called

    with contain(X) as (_, x):
        x << 'whatev'

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

        c << 'stop'
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
        deref(parent3)._start()


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
            x << 'stop'


def test_stopping_actor_with_children_also_stops_the_children():
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
        a << 'stop'
        assert all(deref(x)._state is STOPPED for x in children)
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
        a << 'stop'
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
        some_actor << 'stop'
        container.consume_message(('error', IGNORE(ANY), (IS_INSTANCE(MockException), IGNORE(ANY))))


def test_unhandled_message():
    @actor
    def A(self, message):
        raise UnhandledMessage

    with contain(A) as (container, a):
        a << 'foo'
        assert ('error', a, ANY) not in container.messages, \
            "UnhandledMessage exceptions should not be treated as errors"


def test_unhandled_error_message():
    # process that does not handle child errors
    @actor
    def A(self, message):
        raise UnhandledMessage

    with contain(A) as (_, a):
        with assert_one_warning("Unhandled child errors should emit a warning"):
            a << ('error', 'whatever', (Exception(), 'dummy traceback'))

    # process that handles child errors
    @actor
    def B(self, message):
        pass

    with contain(B) as (_, a):
        with assert_no_warnings():
            a << ('error', 'whatever', (Exception(), 'dummy traceback'))


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


def test_unhandled_message_with_processes():
    @process
    def Child1(self):
        yield Deferred()

    with contain(Child1) as (container, x):
        x.send('foo')  # will be pended because it doesn't match `'bar'`
        deref(x).get(filter='bar')
        assert not container.messages

        with assert_no_warnings():
            deref(x).flush()

        x << ('error', 'whatever', ('whatever', 'dummy-traceback'))
        with assert_one_warning():
            deref(x).flush()


class MockException(Exception):
    pass


@process
def run_with_error(self):
    raise MockException()
