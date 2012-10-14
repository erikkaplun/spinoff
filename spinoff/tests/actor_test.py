from __future__ import print_function

import functools
import gc
import inspect
import random
import re
import sys
import weakref

from nose.tools import eq_, ok_, set_trace
from nose.twistedtools import deferred
from twisted.internet.defer import Deferred, inlineCallbacks, DeferredQueue, fail, CancelledError, DebugInfo, returnValue
from twisted.internet.task import Clock

from spinoff.actor import (
    Actor, Props, Node, Unhandled, NameConflict, UnhandledTermination, CreateFailed, BadSupervision, Ref, Uri)
from spinoff.actor.events import Events, UnhandledMessage, DeadLetter, ErrorIgnored, HighWaterMarkReached
from spinoff.actor.process import Process
from spinoff.actor.supervision import Resume, Restart, Stop, Escalate, Default
from spinoff.actor.remoting import Hub, MockNetwork, HubWithNoRemoting
from spinoff.actor.exceptions import InvalidEscalation
from spinoff.util.async import _process_idle_calls, _idle_calls, with_timeout, sleep
from spinoff.util.pattern_matching import ANY, IS_INSTANCE
from spinoff.util.testing import (
    assert_raises, assert_one_warning, swallow_one_warning, MockMessages, assert_one_event, ErrorCollector, EvSeq,
    EVENT, NEXT, Latch, Trigger, Counter, expect_failure, Slot, simtime, Unclean, MockActor, assert_event_not_emitted,
    Barrier,)
from spinoff.actor.events import RemoteDeadLetter


# ATTENTION: all tests functions are smartly auto-wrapped with wrap_globals at the bottom of this file.


##
## SENDING & RECEIVING

def test_sent_message_is_received():
    """Trivial send and receive.

    By default, when sending a message to an actor, it is delivered to and handled by the receiver immediately for
    performance reasons.

    """
    spawn = TestNode().spawn

    messages = []
    a = spawn(Props(MockActor, messages))
    a << 'foo'
    assert messages == ['foo']


def test_sending_operator_is_chainable():
    """Trivial send and receive using the << operator.

    The main advantages of the << operator is that it's considerably fancier. Less importantly, it improves readability
    by bringing out message sends from the rest of the code. Also, it can be chained to send multiple messages to the
    same actor in a sequence.

    """
    spawn = TestNode().spawn

    messages = MockMessages()
    a = spawn(Props(MockActor, messages))
    a << 'foo' << 'bar'
    assert messages.clear() == ['foo', 'bar']


def test_async_sending():
    """Asynchronous sending by setting SENDING_IS_ASYNC or force_async=True.

    Optionally, it is possible to guarantee delayed deliveries by setting SENDING_IS_ASYNC or passing
    `force_async=True` to `send`.

    Caveat: if you do an async send, but the message is delayed because the actor is suspended or hasn't started yet,
    the message will be received immediately when the actor is resumed or started, respectively. This might change in
    the future.

    """
    spawn = TestNode().spawn

    for case in ['via-setting', 'via-explicit-param']:
        if case is 'via-setting':
            Actor.SENDING_IS_ASYNC = True

        messages = DeferredQueue()

        class MyActor(Actor):
            def receive(self, message):
                messages.put(message)

        a = spawn(MyActor)
        a.send('foo', **({'force_async': True} if case is 'via-explicit-param' else {}))
        a.send('bar', **({'force_async': True} if case is 'via-explicit-param' else {}))
        assert not messages.pending
        msg1 = yield messages.get()
        assert msg1 == 'foo'
        assert not messages.pending
        msg2 = yield messages.get()
        assert msg2 == 'bar'


def test_receive_of_the_same_actor_never_executes_concurrently():
    """Receive of the same actor is never concurrent.

    Regardless of the timing semantics of the send operation, it is guaranteed that an actor that is processing
    a message (i.e. is not idle), will not receive the next message before processing the previous one completes.

    This test case rules out only the basic recursive case with a non-deferred receive method.

    See test_receive_of_the_same_actor_never_executes_concurrently_even_with_deferred_receives for the deferred
    receive case.

    """
    spawn = TestNode().spawn

    receive_called = Counter()

    class MyActor(Actor):
        def receive(self, message):
            if message == 'init':
                receive_called()
                self.ref << None
                assert receive_called == 1
            else:
                receive_called()

    spawn(MyActor) << 'init'

    assert receive_called == 2


def test_receive_is_auto_wrapped_with_txcoroutine_if_its_a_generator_function():
    """Receive is auto-wrapped with txcoroutine if necessary

    This allows for conveniently writing delayed completion of handling messages.

    yield'ing Deferreds from `receive` is the preferred method over returning Deferreds.  The reason for this is that
    txcoroutine.coroutine supports transparent stopping, suspending and resuming of the asynchronous processing
    flow an, and defining clean-up procedures with try-finally.  It is not even possible to achieve the same
    consistency with manually returned Deferreds, because, even though it's possible to hook into cancellation, it is
    not possible to react to Deferred.pause and unpause, thus violating the principle that an Actor is not supposed to
    do any processing inside itself whatsoever while it's suspended.

    See also http://pypi.python.org/pypi/txcoroutine/ for more information.

    """
    spawn = TestNode().spawn

    started = Latch()

    class MyActor(Actor):
        def receive(self, message):
            started()
            yield

    spawn(MyActor) << None
    assert started


def test_receive_of_the_same_actor_never_executes_concurrently_even_with_deferred_receives():
    """Also deferred receive of the same actor is never concurrent.

    This allows for conveniently writing delayed completion of handling messages.

    An actor's `receive` method is allowed do delayed (non-blocking) processing by yielding or returning Deferreds.
    This test rules out concurrent receives on the same actor in case of a deferred-receive.

    See the comment about yielding vs returning Deferreds in
    test_receive_is_auto_wrapped_with_txcoroutine_if_its_a_generator_function.

    """
    spawn = TestNode().spawn

    receive_called = Counter()
    triggers = []

    def _do_test(actor_cls):
        # initialize/reset shared inspection/control variables
        receive_called.reset()

        release = Trigger()
        final = Trigger()
        triggers[:] = [final, release]

        a = spawn(actor_cls)
        a << None << None
        assert receive_called == 1

        release()
        assert receive_called == 2

        final()

    class ActorWithImplicitCoroutine(Actor):
        def receive(self, message):
            receive_called()
            yield triggers.pop()

    class ActorReturningDeferreds(Actor):
        def receive(self, message):
            receive_called()
            return triggers.pop()

    _do_test(ActorWithImplicitCoroutine)
    _do_test(ActorReturningDeferreds)


def test_unhandled_message_is_reported():
    """Unhandled messages are reported to Events"""
    spawn = TestNode().spawn

    class MyActor(Actor):
        def receive(self, _):
            raise Unhandled

    a = spawn(MyActor)
    with assert_one_event(UnhandledMessage(a, 'foo')):
        a << 'foo'


def test_unhandled_message_to_guardian_is_also_reported():
    guardian = TestNode().guardian
    with assert_one_event(UnhandledMessage(guardian, 'foo')):
        guardian << 'foo'


def test_with_no_receive_method_all_messages_are_unhandled():
    spawn = TestNode().spawn
    a = spawn(Actor)
    with assert_one_event(UnhandledMessage(a, 'dummy')):
        a << 'dummy'


##
## SPAWNING

def test_spawning_is_async():
    """Spawning child actors by default is delayed and the spawn call returns immediately."""
    spawn = TestNode().spawn

    Actor.SPAWNING_IS_ASYNC = True  # False by default during testing

    init_called = Latch()
    actor_spawned = Deferred()

    class MyActor(Actor):
        def __init__(self):
            init_called()

        def pre_start(self):
            actor_spawned.callback(None)

    spawn(MyActor)
    assert not init_called
    assert not actor_spawned.called
    yield actor_spawned
    assert init_called


def test_spawning_can_be_synchronous():
    """Spawning can be made synchronous.

    This is only recommended (and is the default) during testing.

    """
    spawn = TestNode().spawn

    Actor.SPAWNING_IS_ASYNC = False

    actor_spawned = Latch()

    class MyActor(Actor):
        def pre_start(self):
            actor_spawned()

    spawn(MyActor)
    assert actor_spawned


def test_spawning_a_toplevel_actor_assigns_guardian_as_its_parent():
    """Top-level actor's parent is the Guardian"""
    node = TestNode()
    spawn = node.spawn

    pre_start_called = Latch()

    class MyActor(Actor):
        def pre_start(self):
            assert self._parent is node.guardian
            pre_start_called()

    spawn(MyActor)
    assert pre_start_called


def test_spawning_child_actor_assigns_the_spawner_as_parent():
    """Child actor's parent is the spawner"""
    spawn = TestNode().spawn

    childs_parent = Slot()

    class MyActor(Actor):
        def receive(self, _):
            self.spawn(Child) << None

    class Child(Actor):
        def receive(self, _):
            childs_parent << self._parent

    a = spawn(MyActor)
    a << None
    assert childs_parent() is a


def test_spawning_returns_an_immediately_usable_ref():
    spawn = TestNode().spawn

    other_receive_called = Latch()

    class MyActor(Actor):
        def receive(self, _):
            spawn(Other) << 'foo'

    class Other(Actor):
        def receive(self, _):
            other_receive_called()

    spawn(MyActor) << None
    assert other_receive_called, "Spawned actor should have received a message"


def test_pre_start_is_called_after_constructor_and_ref_and_parent_are_available():
    node = TestNode()

    message_received = Latch()

    class MyActor(Actor):
        def receive(self, message):
            if message == 'init':
                self.child = self.spawn(Child)
            else:
                assert message is self.child
                message_received()

    class Child(Actor):
        def pre_start(self):
            self._parent << self.ref

    node.spawn(MyActor) << 'init'
    assert message_received


def test_pre_start_can_return_a_Deferred():
    """Pre start can return a Deferred."""
    spawn = TestNode().spawn

    mock_d = Deferred()
    received = Latch()

    class MyActor(Actor):
        def pre_start(self):
            return mock_d

        def receive(self, _):
            received()  # make sure the deferred in pre_start is actually respected

    spawn(MyActor)

    assert not received
    mock_d.callback(None)
    yield received


def test_pre_start_can_be_a_coroutine():
    """Pre start can be a coroutine."""
    node = TestNode()

    pre_start_called = Latch()
    release = Trigger()
    pre_start_continued = Latch()

    class MyActor(Actor):
        def pre_start(self):
            pre_start_called()
            yield release
            pre_start_continued()

    node.spawn(MyActor)

    assert pre_start_called
    assert not pre_start_continued

    release()

    assert pre_start_continued


def test_actor_is_not_started_until_deferred_pre_start_completes():
    """Pre start can return a Deferred or be a coroutine.

    The actor is not considered to be started (i.e. ready to receive messages) until `pre_start` completes.

    """
    spawn = TestNode().spawn

    release = Trigger()
    received = Latch()

    class MyActor(Actor):
        def pre_start(self):
            yield release

        def receive(self, message):
            received()

    spawn(MyActor) << 'dummy'

    assert not received
    release()
    assert received


def test_errors_in_deferred_pre_start_are_reported_as_in_a_normal_pre_start():
    """Pre start can return a Deferred or be a coroutine."""
    spawn = TestNode().spawn

    class MyActor(Actor):
        def pre_start(self):
            return fail(MockException())

    with expect_failure(CreateFailed) as basket:
        spawn(MyActor)
    with assert_raises(MockException):
        basket[0].raise_original()


def test_sending_to_self_does_not_deliver_the_message_until_after_the_actor_is_started():
    spawn = TestNode().spawn

    message_received = Latch()

    class MyActor(Actor):
        def pre_start(self):
            self.ref << 'dummy'
            assert not message_received

        def receive(self, message):
            message_received()

    spawn(MyActor)

    assert message_received


## REMOTE SPAWNING

def test_TODO_remote_spawning():
    pass


def test_TODO_remotely_spawned_actors_ref_is_registered_eagerly():
    # might not be necessary because a ref to the new remote child is returned anyway, and that does the registration;
    # but then if the parent immediately sends another message before waiting for the spawning to return, the message
    # will probably be dropped on the remote node
    pass


def test_TODO_remotely_spawned_actors_die_if_their_parent_node_seems_to_have_died():
    # might not be necessary because a ref to the new remote child is returned anyway, and that does the registration;
    # but then if the parent immediately sends another message before waiting for the spawning to return, the message
    # will probably be dropped on the remote node
    pass


def test_TODO_remote_actorref_determinism():
    pass


## REMOTE AUTO-DEPLOY

def test_TODO_test_stub_registration_and_sending_of_eggs():
    pass


##
## LIFECYCLE

def test_suspending():
    spawn = TestNode().spawn

    message_received = Counter()

    class MyActor(Actor):
        def receive(self, message):
            message_received()

    a = spawn(MyActor)

    message_received.reset()
    a << 'foo'
    assert message_received

    message_received.reset()
    a << '_suspend'
    assert not message_received

    message_received.reset()
    a << 'foo'
    assert not message_received


def test_suspending_while_pre_start_is_blocked_pauses_pre_start():
    spawn = TestNode().spawn

    release = Trigger()
    after_release = Latch()

    class MyActor(Actor):
        def pre_start(self):
            yield release
            after_release()

    a = spawn(MyActor)
    assert not after_release
    a << '_suspend'
    release()
    assert not after_release

    a << '_resume'
    assert after_release


def test_suspending_with_nonempty_inbox_while_receive_is_blocked():
    spawn = TestNode().spawn

    release = Trigger()
    message_received = Counter()

    class MyActor(Actor):
        def receive(self, message):
            message_received()
            if not release.called:  # only yield the first time for correctness
                yield release

    a = spawn(MyActor)
    a << None
    assert message_received == 1

    a << 'foo'
    a << '_suspend'
    release()
    assert message_received == 1

    a << '_resume'
    assert message_received == 2


def test_suspending_while_receive_is_blocked_pauses_the_receive():
    spawn = TestNode().spawn

    release_child = Trigger()
    after_release_reached = Latch()

    # this actually does not have to be like that. the goal is that if, while an actor is suspended, there is a failure
    # in it, it shouldn't get lost. now if the parent is handling a previous error, and decides to resume, it will
    # discard its fail child's suspended-due-to-error state. this can also be achieved however by enabling stacked suspends

    class MyActor(Actor):
        def receive(self, _):
            yield release_child
            after_release_reached()

    a = spawn(MyActor)
    a << 'dummy'
    a << '_suspend'
    release_child()
    assert not after_release_reached

    a << '_resume'
    assert after_release_reached


def test_suspending_while_already_suspended():
    """test_suspending_while_already_suspended

    This can happen when an actor is suspended and then its parent gets suspended.

    """
    spawn = TestNode().spawn

    message_received = Latch()

    class DoubleSuspendingActor(Actor):
        def receive(self, msg):
            message_received()

    a = spawn(DoubleSuspendingActor)
    a << '_suspend' << '_suspend' << '_resume' << 'dummy'
    assert message_received


def test_TODO_stopping():
    pass


def test_TODO_force_stopping_does_not_wait_for_a_deferred_post_stop_to_complete():
    pass


def test_resuming():
    spawn = TestNode().spawn

    class MyActor(Actor):
        def receive(self, message):
            message_received()

    a = spawn(MyActor)

    a << '_suspend'

    message_received = Counter()
    a << 'foo'
    a << 'foo'
    a << '_resume'
    assert message_received == 2


def test_restarting():
    spawn = TestNode().spawn

    actor_started = Counter()
    message_received = Latch()
    post_stop_called = Latch()

    class MyActor(Actor):
        def pre_start(self):
            actor_started()

        def receive(self, _):
            message_received()

        def post_stop(self):
            post_stop_called()

    a = spawn(MyActor)
    assert actor_started == 1
    a << '_restart'
    assert not message_received
    assert actor_started == 2
    assert post_stop_called


def test_restarting_is_not_possible_on_stopped_actors():
    spawn = TestNode().spawn

    actor_started = Counter()

    class MyActor(Actor):
        def pre_start(self):
            actor_started()

    a = spawn(MyActor)
    a.stop()
    a << '_restart'

    assert actor_started == 1


def test_restarting_doesnt_destroy_the_inbox():
    spawn = TestNode().spawn

    messages_received = []
    started = Counter()

    class MyActor(Actor):
        def pre_start(self):
            started()

        def receive(self, message):
            messages_received.append(message)

    a = spawn(MyActor)
    a << 'foo'
    a << '_restart'
    a << 'bar'

    assert messages_received == ['foo', 'bar']
    assert started == 2  # just for verification


def test_restarting_waits_till_the_ongoing_receive_is_complete():
    """Restarts are not carried out until the current receive finishes.

    For this reason, non-blocking operations in `receive` should always be guarded against infinitely blocking
    operations.
    """
    spawn = TestNode().spawn

    started = Counter()
    received_messages = []
    deferred_cancelled = Latch()

    mock_d = Deferred(deferred_cancelled)

    class MyActor(Actor):
        def pre_start(self):
            started()

        def receive(self, message):
            if started == 1:
                return mock_d.addCallback(lambda _: received_messages.append(message))
            else:
                received_messages.append(message)

    spawn(MyActor) << 'foo' << '_restart'
    assert not deferred_cancelled
    assert received_messages == []

    assert started == 1
    mock_d.callback(None)
    assert started == 2

    assert received_messages == ['foo']


def test_restarting_does_not_complete_until_a_deferred_pre_start_completes():
    spawn = TestNode().spawn

    release = Trigger()
    received = Latch()

    started = Latch()

    class MyActor(Actor):
        def pre_start(self):
            if not started:
                started()
            else:
                yield release  # only block on restart, not on creation

        def receive(self, message):
            received()

    a = spawn(MyActor)

    a << '_restart' << 'dummy'
    assert not received

    release()
    assert received


def test_tainted_resume_does_not_complete_until_the_underlying_restart_completes_a_deferred_pre_start():
    """Tainted resume waits on a deferred pre_start.

    Resuming can result in a restart (in case of a tainted actor), and that restart might in turn have to wait on a
    deferred `pre_start`, thus, the actor must not be marked as resumed until the deferred `pre_start` completes.

    """
    spawn = TestNode().spawn

    started = Latch()
    pre_start_complete = Trigger()
    received = Latch()
    child = Slot()

    class Parent(Actor):
        def pre_start(self):
            child << self.spawn(Child)

        def supervise(self, exc):
            assert isinstance(exc, CreateFailed), exc
            return Resume

    class Child(Actor):
        def pre_start(self):
            if not started:
                started()
                raise MockException
            else:
                return pre_start_complete

        def receive(self, _):
            received()

    with swallow_one_warning():
        spawn(Parent)

    child() << 'dummy'
    assert not received

    pre_start_complete()
    assert received


def test_actor_is_untainted_after_a_restarting_resume():
    spawn = TestNode().spawn

    started = Latch()
    child = Slot()

    class Parent(Actor):
        def pre_start(self):
            child << self.spawn(Child)

        def supervise(self, exc):
            if not (isinstance(exc, MockException) or
                    isinstance(exc, CreateFailed) and
                    isinstance(exc.cause, MockException)):
                return Escalate
            else:
                return Resume

    class Child(Actor):
        def pre_start(self):
            try:
                if not started:
                    raise MockException
            finally:
                started()

        def receive(self, _):
            raise MockException

    with swallow_one_warning():
        spawn(Parent)

    started.reset()
    child() << 'dummy'
    assert not started


def test_stopping_waits_till_the_ongoing_receive_is_complete():
    spawn = TestNode().spawn

    stopped = Counter()
    deferred_cancelled = []

    mock_d = Deferred(deferred_cancelled.append)

    class MyActor(Actor):
        def receive(self, message):
            return mock_d

        def post_stop(self):
            stopped()

    a = spawn(MyActor) << 'foo'
    a.stop()

    assert not deferred_cancelled
    assert not stopped

    mock_d.callback(None)
    assert stopped


def test_messages_sent_by_child_post_stop_to_restarting_parent_are_processed_after_restart():
    spawn = TestNode().spawn

    parent_started = Counter()
    parent_received = Latch()

    class Parent(Actor):
        def pre_start(self):
            parent_started()
            self.spawn(Child)

        def receive(self, message):
            assert parent_started == 2
            parent_received()

    class Child(Actor):
        def post_stop(self):
            self._parent.send('should-be-received-after-restart')

    p = spawn(Parent)
    p << '_restart'

    assert parent_received


def test_stopping_an_actor_prevents_it_from_processing_any_more_messages():
    spawn = TestNode().spawn

    class MyActor(Actor):
        def receive(self, _):
            received()

    received = Counter()
    a = spawn(MyActor)
    a << None

    assert received == 1

    received.reset()
    a.stop()
    assert not received, "the '_stop' message should not be receivable in the actor"
    with assert_one_event(DeadLetter(a, None)):
        a << None


def test_stopping_calls_post_stop():
    spawn = TestNode().spawn

    post_stop_called = Latch()

    class MyActor(Actor):
        def post_stop(self):
            post_stop_called()

    spawn(MyActor).stop()
    assert post_stop_called


def test_stopping_waits_for_post_stop():
    spawn = TestNode().spawn

    def _do_test(child_cls):
        class Parent(Actor):
            def pre_start(self):
                self.child = self.watch(self.spawn(child_cls))

            def receive(self, message):
                if message == 'stop-child':
                    self.child.stop()
                else:
                    parent_received()

        a = spawn(Parent)
        a << 'stop-child'
        assert not parent_received
        stop_complete()
        assert parent_received

    class ChildWithDeferredPostStop(Actor):
        def post_stop(self):
            return stop_complete

    stop_complete = Trigger()
    parent_received = Latch()

    _do_test(ChildWithDeferredPostStop)

    class ChildWithCoroutinePostStop(Actor):
        def post_stop(self):
            yield stop_complete

    stop_complete = Trigger()
    parent_received = Latch()

    _do_test(ChildWithCoroutinePostStop)


def test_actor_is_not_stopped_until_its_children_are_stopped():
    spawn = TestNode().spawn

    stop_complete = Trigger()
    parent_stopped = Latch()

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

        def post_stop(self):
            parent_stopped()

    class Child(Actor):
        def post_stop(self):
            return stop_complete

    a = spawn(Parent)
    a.stop()
    assert not parent_stopped
    stop_complete()
    assert parent_stopped


def test_actor_is_not_restarted_until_its_children_are_stopped():
    spawn = TestNode().spawn

    stop_complete = Trigger()
    parent_started = Counter()

    class Parent(Actor):
        def pre_start(self):
            parent_started()
            self.spawn(Child)

    class Child(Actor):
        def post_stop(self):
            return stop_complete

    a = spawn(Parent)
    a << '_restart'
    assert parent_started == 1
    stop_complete()
    assert parent_started == 2, parent_started.value


def test_error_reports_to_a_stopping_actor_are_ignored():
    spawn = TestNode().spawn

    child = Slot()
    release_child = Trigger()
    post_stop_called = Latch()

    class Child(Actor):
        def receive(self, _):
            yield release_child
            raise MockException()

    class Parent(Actor):
        def pre_start(self):
            child << self.spawn(Child)

        def post_stop(self):
            post_stop_called()

    p = spawn(Parent)
    child() << 'dummy'
    p.stop()
    assert not post_stop_called
    with expect_failure(MockException):
        release_child()


def test_stopping_an_actor_with_a_pending_deferred_receive_doesnt_cancel_the_deferred():
    spawn = TestNode().spawn

    canceller_called = Latch()
    stopped = Latch()
    mock_d = Deferred(canceller=lambda _: canceller_called.__setitem__(0, True))

    class MyActor(Actor):
        def receive(self, _):
            return mock_d

        def post_stop(self):
            stopped()

    a = spawn(MyActor)
    a << None
    a.stop()
    assert not canceller_called
    assert not stopped
    mock_d.callback(None)
    assert stopped


def test_stopping_in_pre_start_directs_any_refs_to_deadletters():
    spawn = TestNode().spawn

    message_received = Latch()

    class MyActor(Actor):
        def pre_start(self):
            self.ref.stop()

        def receive(self, message):
            message_received()

    a = spawn(MyActor)

    with assert_one_event(DeadLetter(a, 'dummy')):
        a << 'dummy'

    assert not message_received


def test_stop_message_received_twice_is_ignored():
    spawn = TestNode().spawn

    a = spawn(Actor)
    a.send('_stop', force_async=True)
    a.send('_stop', force_async=True)
    yield sleep(0.01)
    yield a.join()


def test_TODO_poisonpill():
    pass


## REMOTE LIFECYCLE


def test_TODO_remote_suspending():
    pass


def test_TODO_remote_resuming():
    pass


def test_TODO_remote_restarting():
    pass


def test_TODO_remote_stopping():
    pass


##
## ACTORREFS, URIS & LOOKUP

# def test_TODO_actorrefs_with_equal_paths_are_equal():
#     assert Ref(None, path='123') == Ref(None, path='123')


def test_actors_are_garbage_collected_on_termination():
    spawn = TestNode().spawn

    del_called = Latch()

    class MyActor(Actor):
        def __del__(self):
            del_called()

    ac = spawn(MyActor)
    assert not del_called

    ac.stop()

    gc.collect()
    assert del_called


def test_cells_are_garbage_collected_on_termination():
    spawn = TestNode().spawn

    ac = spawn(Actor)

    cell = weakref.ref(ac._cell)
    assert cell()
    ac.stop()

    gc.collect()
    assert not cell()


def test_messages_to_dead_actors_are_sent_to_dead_letters():
    spawn = TestNode().spawn

    a = spawn(Actor)
    a.stop()

    with assert_one_event(DeadLetter(a, 'should-end-up-as-letter')):
        a << 'should-end-up-as-letter'


def test_guardians_path_is_the_root_uri():
    node = TestNode()
    eq_(node.guardian.uri.path, '')


def test_toplevel_actorrefs_paths_are_prefixed_with_guardians_path():
    spawn = TestNode().spawn

    a = spawn(Actor, name='a')
    assert a.uri.path == '/a'

    b = spawn(Actor, name='b')
    assert b.uri.path == '/b'


def test_non_toplevel_actorrefs_are_prefixed_with_their_parents_path():
    spawn = TestNode().spawn

    child_ref = Slot()

    class MyActor(Actor):
        def pre_start(self):
            child_ref << self.spawn(Actor, name='child')

    a = spawn(MyActor, name='parent')
    assert child_ref and child_ref().uri.path == a.uri.path + '/child'


def test_toplevel_actor_paths_must_be_unique():
    spawn = TestNode().spawn

    spawn(Actor, name='a')
    with assert_raises(NameConflict):
        spawn(Actor, name='a')


def test_non_toplevel_actor_paths_must_be_unique():
    spawn = TestNode().spawn

    class MyActor(Actor):
        def pre_start(self):
            self.spawn(Actor, name='a')
            with assert_raises(NameConflict):
                self.spawn(Actor, name='a')

    spawn(MyActor, name='a')


def test_spawning_toplevel_actors_without_name_assigns_autogenerated_names():
    spawn = TestNode().spawn

    a = spawn(Actor)
    assert re.match(r'/[^/]+$', a.uri.path)

    b = spawn(Actor)
    assert re.match(r'/[^/]+$', b.uri.path)

    assert b.uri.path != a.uri.path


def test_spawning_non_toplevel_actors_without_name_assigns_autogenerated_names_with_prefixed_parent_path():
    spawn = TestNode().spawn

    class MyActor(Actor):
        def pre_start(self):
            l = len(self.ref.uri.path)

            a = self.spawn(Actor)
            assert a.uri.path[l:]

            assert a.uri.path.startswith(self.ref.uri.path + '/'), a.path

            b = self.spawn(Actor)
            assert b.uri.path[l:]

            assert a.uri.path != b.uri.path

    spawn(MyActor)


def test_spawning_with_autogenerated_looking_name_raises_an_exception():
    spawn = TestNode().spawn

    with assert_raises(ValueError):
        spawn(Actor, name='$1')


## URIs

def test_relative_uri():
    #
    uri = Uri.parse('foo')
    eq_(str(uri), 'foo')
    eq_(uri.name, 'foo')
    eq_(uri.path, 'foo')
    ok_(not uri.url)

    eq_(uri, Uri.parse('foo'))
    eq_(uri, 'foo', "Uri.__eq__ supports str")

    ok_(not uri.parent)
    ok_(not uri.node)
    eq_(uri.root, uri)

    eq_(list(uri.steps), ['foo'])

    ok_(uri.local is uri)

    #
    uri = Uri.parse('foo/bar')
    eq_(str(uri), 'foo/bar')
    eq_(uri.name, 'bar')
    eq_(uri.path, 'foo/bar')
    ok_(not uri.url)

    eq_(uri, Uri.parse('foo/bar'))
    eq_(uri, 'foo/bar', "Uri.__eq__ supports str")

    eq_(uri.parent, 'foo')
    ok_(not uri.node)
    eq_(uri.root, 'foo')

    eq_(list(uri.steps), ['foo', 'bar'])

    ok_(uri.local is uri)


def test_absolute_uri():
    #
    uri = Uri.parse('')
    eq_(str(uri), '')
    eq_(uri.name, '')
    eq_(uri.path, '')
    ok_(not uri.url)

    eq_(uri, Uri.parse(''))
    eq_(uri, '')

    ok_(not uri.parent)
    ok_(not uri.node)
    eq_(uri.root, uri)

    eq_(list(uri.steps), [''])

    ok_(uri.local is uri)

    #
    uri = Uri.parse('/foo')
    eq_(str(uri), '/foo')
    eq_(uri.name, 'foo')
    eq_(uri.path, '/foo')
    ok_(not uri.url)

    eq_(uri, Uri.parse('/foo'))
    eq_(uri, '/foo', "Uri.__eq__ supports str")

    ok_(uri.parent == '')
    ok_(not uri.node)
    eq_(uri.root, '')

    eq_(list(uri.steps), ['', 'foo'])

    ok_(uri.local is uri)

    #
    uri = Uri.parse('/foo/bar')
    eq_(uri.name, 'bar')
    eq_(uri, '/foo/bar', "Uri.__eq__ supports str")
    eq_(uri.path, '/foo/bar')
    ok_(not uri.url)

    eq_(uri, Uri.parse('/foo/bar'))
    eq_(uri, '/foo/bar', "Uri.__eq__ supports str")

    eq_(uri.parent, '/foo')
    ok_(not uri.node)
    eq_(uri.root, '')

    eq_(list(uri.steps), ['', 'foo', 'bar'])

    ok_(uri.local is uri)


def test_fully_qualified_uri():
    #
    uri = Uri.parse('localhost:123')
    eq_(str(uri), 'localhost:123')
    eq_(uri.name, '')
    eq_(uri.path, '')
    eq_(uri.url, 'tcp://localhost:123')

    eq_(uri, Uri.parse('localhost:123'))
    eq_(uri, 'localhost:123')

    ok_(not uri.parent)
    eq_(uri.node, 'localhost:123')
    eq_(uri.root, uri)

    eq_(list(uri.steps), [''])

    eq_(uri.local, '')

    #
    uri = Uri.parse('localhost:123/foo')
    eq_(str(uri), 'localhost:123/foo')
    eq_(uri.name, 'foo')
    eq_(uri.path, '/foo')
    eq_(uri.url, 'tcp://localhost:123/foo')

    eq_(uri, Uri.parse('localhost:123/foo'))
    eq_(uri, 'localhost:123/foo', "Uri.__eq__ supports str")

    eq_(uri.parent, 'localhost:123')
    eq_(uri.node, 'localhost:123')
    eq_(uri.root, 'localhost:123')

    eq_(list(uri.steps), ['', 'foo'])

    eq_(uri.local, '/foo')

    #
    uri = Uri.parse('localhost:123/foo/bar')
    eq_(str(uri), 'localhost:123/foo/bar')
    eq_(uri.name, 'bar')
    eq_(uri.path, '/foo/bar')
    eq_(uri.url, 'tcp://localhost:123/foo/bar')

    eq_(uri, Uri.parse('localhost:123/foo/bar'))
    eq_(uri, 'localhost:123/foo/bar', "Uri.__eq__ supports str")

    eq_(uri.parent, 'localhost:123/foo')
    eq_(uri.node, 'localhost:123')
    eq_(uri.root, 'localhost:123')

    eq_(list(uri.steps), ['', 'foo', 'bar'])

    eq_(uri.local, '/foo/bar')


def test_uri_hash():
    eq_(hash(Uri.parse('foo')), hash(Uri.parse('foo')))
    eq_(hash(Uri.parse('/foo')), hash(Uri.parse('/foo')))
    eq_(hash(Uri.parse('localhost:123/foo')), hash(Uri.parse('localhost:123/foo')))


## LOOKUP

def test_looking_up_an_actor_by_its_absolute_path_returns_the_original_reference_to_it():
    node = TestNode()
    toplevel_actor = node.spawn(Actor, name='toplevel')
    assert node.lookup('/toplevel') is toplevel_actor
    assert node.lookup(Uri.parse('/toplevel')) is toplevel_actor

    child_actor = toplevel_actor._cell.spawn(Actor, name='child')
    assert node.lookup('/toplevel/child') is child_actor
    assert node.lookup(Uri.parse('/toplevel/child')) is child_actor


def test_looking_up_an_actor_by_a_relative_path_returns_the_original_reference_to_it():
    node = TestNode()
    toplevel_actor = node.spawn(Actor, name='toplevel')
    assert node.lookup('toplevel') is toplevel_actor

    child_actor = toplevel_actor._cell.spawn(Actor, name='child')
    assert node.lookup('toplevel/child') is child_actor
    assert toplevel_actor / 'child' is child_actor


def test_looking_up_an_actor_by_a_parent_traversing_relative_path_returns_a_reference_to_it():
    node = TestNode()

    a = node.spawn(Actor, name='a')
    ok_(node.guardian / 'a' is a)

    b = a._cell.spawn(Actor, name='b')
    ok_(a / 'b' is b)
    ok_(node.guardian / 'a/b' is b)


def test_looking_up_an_absolute_path_as_if_it_were_relative_just_does_an_absolute_lookup():
    node = TestNode()

    a = node.spawn(Actor, name='a')
    a._cell.spawn(Actor, name='b')
    root_b = node.spawn(Actor, name='b')

    eq_(a / '/b', root_b)


def test_looking_up_a_non_existent_local_actor_raises_runtime_error():
    node = TestNode()

    with assert_raises(RuntimeError):
        node.guardian / 'noexist'


def test_looking_up_a_non_existent_local_actor_returns_a_dead_ref_with_nevertheless_correct_uri():
    network = MockNetwork(Clock())
    node = network.node('local:123')
    eq_(node.guardian, node.lookup('local:123'))

    noexist = node.lookup('local:123/a/b/c')
    eq_(noexist.uri, 'local:123/a/b/c')
    ok_(noexist.is_local)
    with assert_one_event(DeadLetter(noexist, 'foo')):
        noexist << 'foo'

    hypotheticalchild = noexist / 'hypotheticalchild'
    ok_(hypotheticalchild.is_local)
    eq_(hypotheticalchild.uri, 'local:123/a/b/c/hypotheticalchild')
    with assert_one_event(DeadLetter(hypotheticalchild, 'foo')):
        hypotheticalchild << 'foo'


def test_manually_ceated_remote_looking_ref_to_a_non_existend_local_actor_is_converted_to_dead_ref_on_send():
    network = MockNetwork(Clock())
    node = network.node('local:123')

    noexist = Ref(cell=None, is_local=False, uri=Uri.parse('local:123/a/b/c'), hub=node.hub)

    with assert_one_event(DeadLetter(noexist, 'foo')):
        noexist << 'foo'
    ok_(noexist.is_local)


##
## SUPERVISION & ERROR HANDLING

def test_child_is_resumed_if_supervise_returns_resume():
    """Child is resumed if `supervise` returns `Resume`"""
    spawn = TestNode().spawn

    message_received = Latch()

    class Child(Actor):
        def receive(self, message):
            if message == 'raise':
                raise MockException
            else:
                message_received()

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'raise' << 'other-message'

        def supervise(self, _):
            return Resume

    spawn(Parent)

    assert message_received


def test_child_is_restarted_if_supervise_returns_restart():
    """Child is restarted if `supervise` returns `Restart`"""
    spawn = TestNode().spawn

    child_started = Counter()

    class Child(Actor):
        def pre_start(self):
            child_started()

        def receive(self, message):
            raise MockException

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'raise'

        def supervise(self, _):
            return Restart

    spawn(Parent)
    assert child_started == 2


def test_child_is_stopped_if_supervise_returns_stop():
    """Child is stopped if `supervise` returns `Stop`"""
    spawn = TestNode().spawn

    child_stopped = Latch()

    class Child(Actor):
        def post_stop(self):
            child_stopped()

        def receive(self, message):
            raise MockException

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'raise'

        def supervise(self, _):
            return Stop

    spawn(Parent)

    assert child_stopped


def test_child_is_stop_if_supervise_returns_stop():
    """Exception is escalated if `supervise` returns `Escalate`"""
    spawn = TestNode().spawn

    escalated = Latch()

    class Child(Actor):
        def receive(self, message):
            raise MockException

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'raise'

        def supervise(self, _):
            return Escalate

    class ParentsParent(Actor):
        def pre_start(self):
            self.spawn(Parent)

        def supervise(self, exc):
            escalated()
            return Stop

    spawn(ParentsParent)

    assert escalated


def test_TODO_exception_escalations_are_reported_as_events():
    pass


def test_child_error_suspends_child():
    spawn = TestNode().spawn

    release_parent = Trigger()

    child = Slot()

    class Parent(Actor):
        def pre_start(self):
            child << self.spawn(Child)

        def supervise(self, exc):
            assert isinstance(exc, MockException)
            evseq('parent_received_error')
            return Resume

        def receive(self, message):
            return release_parent

    class Child(Actor):
        def receive(self, message):
            if message == 'cause-error':
                evseq('child_causing_error')
                raise MockException
            else:
                evseq('child_received_message')

    evseq = EvSeq()

    parent = spawn(Parent)
    parent << 'block'

    child() << 'cause-error' << 'dummy'

    release_parent()

    yield evseq.await(EVENT('child_causing_error'))
    yield evseq.await(EVENT('parent_received_error'))
    yield evseq.await(EVENT('child_received_message'))


def test_error_in_deferred_receive_behaves_the_same_as_non_deferred():
    spawn = TestNode().spawn

    class MyActor(Actor):
        def receive(self, _):
            return fail(MockException())

    a = spawn(MyActor)

    with expect_failure(MockException):
        a << 'dummy'


def test_exception_after_stop_is_ignored_and_does_not_report_to_parent():
    spawn = TestNode().spawn

    class Child(Actor):
        def receive(self, _):
            self.stop()
            raise MockException

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'dummy'

        def supervise(self, exc):
            assert False

    with assert_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        spawn(Parent)


def test_error_in_post_stop_prints_but_doesnt_report_to_parent_and_termination_messages_are_sent():
    spawn = TestNode().spawn

    termination_message_received = Latch()

    class Child(Actor):
        def post_stop(self):
            raise MockException

    class Parent(Actor):
        def pre_start(self):
            self.child = self.spawn(Child)
            self.watch(self.child)
            self.child.stop()

        def receive(self, message):
            assert message == ('terminated', self.child)
            termination_message_received()

    with assert_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        spawn(Parent)

    assert termination_message_received


def test_supervision_message_is_handled_directly_by_supervise_method():
    spawn = TestNode().spawn

    supervise_called = Latch()
    exc_received = Slot()

    exc = MockException('arg1', 'arg2')

    class Parent(Actor):
        def supervise(self, exc):
            supervise_called()
            exc_received << exc
            return Stop

        def pre_start(self):
            self.spawn(Child) << None

    class Child(Actor):
        def receive(self, _):
            raise exc

    spawn(Parent)
    assert supervise_called
    assert isinstance(exc_received(), MockException) and exc_received().args == exc.args


def test_init_error_reports_to_supervisor():
    spawn = TestNode().spawn

    # TODO: see akka ActorInitializationException
    received_exception = Slot()

    class ChildWithFailingInit(Actor):
        def __init__(self):
            raise MockException

    class Parent(Actor):
        def supervise(self, exc):
            received_exception << exc
            return Stop

        def pre_start(self):
            self.spawn(ChildWithFailingInit)

    spawn(Parent)

    exc = received_exception()
    assert isinstance(exc, CreateFailed)
    with assert_raises(MockException):
        exc.raise_original()


def test_pre_start_error_reports_to_supervisor():
    spawn = TestNode().spawn

    received_exception = Slot()
    post_stop_called = Latch()

    class ChildWithFailingInit(Actor):
        def pre_start(self):
            raise MockException

        def post_stop(self):
            post_stop_called()

    class Parent(Actor):
        def supervise(self, exc):
            received_exception << exc
            return Stop

        def pre_start(self):
            self.spawn(ChildWithFailingInit)

    spawn(Parent)

    exc = received_exception()
    assert isinstance(exc, CreateFailed)
    with assert_raises(MockException):
        exc.raise_original()

    assert not post_stop_called, "post_stop must not be called if there was an error in pre_start"


def test_receive_error_reports_to_supervisor():
    spawn = TestNode().spawn

    received_exception = Slot()

    class Parent(Actor):
        def supervise(self, exc):
            received_exception << exc
            return Stop

        def pre_start(self):
            self.spawn(ChildWithExcInReceive) << None

    class ChildWithExcInReceive(Actor):
        method_under_test = 'receive'

        def receive(self, _):
            raise MockException

    spawn(Parent)
    assert isinstance(received_exception(), MockException), \
        "Child errors in the 'receive' method should be sent to its parent"


def test_restarting_or_resuming_an_actor_that_failed_to_init_or_in_pre_start():
    spawn = TestNode().spawn

    class Parent(Actor):
        def __init__(self, child_cls, supervisor_decision):
            self.child_cls = child_cls
            self.supervisor_decision = supervisor_decision

        def supervise(self, exc):
            return self.supervisor_decision

        def pre_start(self):
            self.spawn(self.child_cls)

    #

    class ChildWithErrorInInit(Actor):
        def __init__(self):
            child_created()

            if not init_already_raised:  # avoid infinite ping-pong
                init_already_raised()
                raise MockException

    init_already_raised = Latch()
    child_created = Counter()
    spawn(Props(Parent, child_cls=ChildWithErrorInInit, supervisor_decision=Restart))
    assert child_created == 2

    init_already_raised = Latch()
    child_created = Counter()
    with assert_one_warning():
        spawn(Props(Parent, child_cls=ChildWithErrorInInit, supervisor_decision=Resume))
    assert child_created == 2

    # #

    class ChildWithErrorInPreStart(Actor):
        def pre_start(self):
            child_started()

            if not pre_start_already_raised:  # avoid infinite ping-pong
                pre_start_already_raised()
                raise MockException

    pre_start_already_raised = Latch()
    child_started = Counter()
    spawn(Props(Parent, child_cls=ChildWithErrorInPreStart, supervisor_decision=Restart))
    assert child_started == 2

    pre_start_already_raised = Latch()
    child_started = Counter()
    with assert_one_warning():
        spawn(Props(Parent, child_cls=ChildWithErrorInPreStart, supervisor_decision=Resume))
    assert child_started == 2


def test_error_report_after_restart_is_ignored():
    spawn = TestNode().spawn

    child = Slot()
    release_child = Trigger()

    class Child(Actor):
        def receive(self, _):
            yield release_child
            raise MockException

    class Parent(Actor):
        def pre_start(self):
            if not child():  # so that after the restart the child won't exist
                child << self.spawn(Child)

    parent = spawn(Parent)

    child() << 'dummy'
    parent << '_restart'

    with assert_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        release_child()


def test_default_supervision_stops_for_create_failed():
    spawn = TestNode().spawn

    class Parent(Actor):
        def pre_start(self):
            self.child = self.watch(self.spawn(Child))

        def receive(self, message):
            assert message == ('terminated', self.child)
            termination_message_received()

    class Child(Actor):
        def __init__(self):
            raise MockException

    termination_message_received = Latch()
    spawn(Parent)
    assert termination_message_received

    #

    class Child(Actor):
        def pre_start(self):
            raise MockException

    termination_message_received = Latch()
    spawn(Parent)
    assert termination_message_received


# def test_TODO_default_supervision_stops_for_actorkilled():
#     # TODO: akka treats actor-killed as an exception--does this make sense?
#     pass


def test_default_supervision_restarts_for_any_other_exception():
    spawn = TestNode().spawn

    child_started = Counter()

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child) << 'fail'

    class Child(Actor):
        def pre_start(self):
            child_started()

        def receive(self, _):
            raise MockException

    spawn(Parent)
    assert child_started == 2


def test_default_supervision_is_applied_if_supervision_returns_default():
    spawn = TestNode().spawn

    class Parent(Actor):
        def pre_start(self):
            c = self.spawn(child_cls)
            child << c
            Events.consume_one(DeadLetter)
            c << 'dummy'

        def supervise(self, _):
            return Default

    class ChildWithFailingInit(Actor):
        def pre_start(self):
            child_started()
            raise MockException

    child_cls = ChildWithFailingInit
    child = Slot()
    child_started = Counter()
    spawn(Parent)
    assert child_started == 1
    assert child().is_stopped  # no better way as of now to check if child has stopped

    #

    class ChildWithFailingReceive(Actor):
        def pre_start(self):
            child_started()

        def receive(self, _):
            raise MockException

    child_cls = ChildWithFailingReceive
    child_started = Counter()
    spawn(Parent)
    assert child_started == 2


# def test_TODO_other_error_escalates():
#     # TODO: what does this mean in Akka anyway?
#     pass


def test_error_is_escalated_if_supervision_returns_escalate_or_nothing():
    spawn = TestNode().spawn

    class Supervisor(Actor):
        def pre_start(self):
            self.spawn(Child)

        def supervise(self, exc):
            return supervision_decision

    class Child(Actor):
        def pre_start(self):
            self.send('dummy')

        def receive(self, _):
            raise MockException

    supervision_decision = Escalate
    with expect_failure(MockException):
        spawn(Supervisor)

    supervision_decision = None
    with expect_failure(MockException):
        spawn(Supervisor)


def test_error_is_escalated_if_supervision_raises_exception():
    spawn = TestNode().spawn

    class SupervisorException(Exception):
        pass

    class Supervisor(Actor):
        def pre_start(self):
            self.spawn(Child)

        def supervise(self, exc):
            raise SupervisorException

    class Child(Actor):
        def pre_start(self):
            self.send('dummy')

        def receive(self, _):
            raise MockException

    with expect_failure(SupervisorException):
        spawn(Supervisor)


def test_bad_supervision_is_raised_if_supervision_returns_an_illegal_value():
    spawn = TestNode().spawn

    class Supervisor(Actor):
        def pre_start(self):
            self.spawn(Child)

        def supervise(self, exc):
            return 'illegal-value'

    class Child(Actor):
        def pre_start(self):
            self.send('dummy')

        def receive(self, _):
            raise MockException

    with expect_failure(BadSupervision, "Should raise BadSupervision if supervision returns an illegal value") as basket:
        spawn(Supervisor)

    with assert_raises(MockException):
        basket[0].raise_original()


def test_TODO_baseexceptions_are_also_propagated_through_the_hierarchy():
    pass


# def test_TODO_supervise_can_specify_maxrestarts():
#     class Parent(Actor):
#         def supervise(self, _):
#             return Restart(max=3)

#         def pre_start(self):
#             self.spawn(Child)

#     child_started = Counter()

#     class Child(Actor):
#         def pre_start(self):
#             child_started()
#             raise MockException

#     spawn(Parent)

#     assert child_started == 4, child_started


def test_TODO_supervision_can_be_marked_as_allforone_or_oneforone():
    pass


##
## GUARDIAN

def test_TODO_guardian_supervision():
    pass


##
## HIERARCHY

def test_actors_remember_their_children():
    spawn = TestNode().spawn

    class MyActor(Actor):
        def pre_start(self):
            assert not self.children

            child1 = self.spawn(Actor)
            assert child1 in self.children

            child2 = self.spawn(Actor)
            assert child2 in self.children

    spawn(MyActor)


def test_stopped_child_is_removed_from_its_parents_list_of_children():
    spawn = TestNode().spawn

    receive_called = Latch()

    class MyActor(Actor):
        def pre_start(self):
            child = self.spawn(Actor)
            assert child in self.children
            self.watch(child)
            child.stop()
            # XXX: assert child in self._children

        def receive(self, message):
            receive_called()
            assert message == ('terminated', ANY)
            _, child = message
            assert child not in self.children

    spawn(MyActor)
    assert receive_called


def test_suspending_suspends_and_resuming_resumes_all_children():
    # Actor.SENDING_IS_ASYNC = True  # so that we could use EvSeq

    spawn = TestNode().spawn

    # so that we could control them from the outside
    child, subchild = Slot(), Slot()

    class Parent(Actor):
        def pre_start(self):
            child << self.spawn(Child)

        def supervise(self, exc):
            assert isinstance(exc, MockException)
            evseq('parent_received_error')
            return Resume  # should resume both Child and SubChild, and allow SubChild to process its message

    class Child(Actor):
        def pre_start(self):
            subchild << self.spawn(SubChild)

        def receive(self, message):
            raise MockException  # should also cause SubChild to be suspended

    class SubChild(Actor):
        def receive(self, message):
            evseq('subchild_received_message')

    evseq = EvSeq()

    spawn(Parent)

    child() << 'dummy'  # will cause Child to raise MockException
    subchild() << 'dummy'

    yield evseq.await(NEXT('parent_received_error'))
    yield evseq.await(NEXT('subchild_received_message'))


def test_stopping_stops_children():
    spawn = TestNode().spawn

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

    class Child(Actor):
        def post_stop(self):
            child_stopped()

    child_stopped = Latch()

    p = spawn(Parent)
    p.stop()

    assert child_stopped


def test_stopping_parent_from_child():
    spawn = TestNode().spawn

    child = Slot()
    started = Counter()

    class PoorParent(Actor):
        def supervise(self, _):
            return Restart

        def pre_start(self):
            child << self.spawn(EvilChild)

    class EvilChild(Actor):
        def pre_start(self):
            started()
            if started == 2:
                # this is the result of the `Restart` above, which means the parent is currently processing `_error`;
                # so we're going to deceive the parent and kill it by its own child!

                self._parent.stop()  # this will queue a `_stop` in the parent's queue
                self.stop()  # this will queue `_child_terminated` in the parent's queue on top of `_stop`

                # the parent is still processing `_error`

        def receive(self, _):
            # invoke the parent's supervision
            raise MockException

    parent = spawn(PoorParent)
    child() << 'begin-conspiracy'
    yield with_timeout(0.01, parent.join())


def test_restarting_stops_children():
    spawn = TestNode().spawn

    started = Counter()

    class Parent(Actor):
        def pre_start(self):
            started()
            if started == 1:  # only start the first time, so after the restart, there should be no children
                self.spawn(Child)
            else:
                assert not self.children, self.children

    class Child(Actor):
        def post_stop(self):
            child_stopped()

    child_stopped = Latch()

    p = spawn(Parent)
    p << '_restart'

    assert child_stopped


def test_TODO_restarting_does_not_restart_children_if_told_so():
    pass  # Parent.stop_children_on_restart = False


def test_sending_message_to_stopping_parent_from_post_stop_should_deadletter_the_message():
    spawn = TestNode().spawn

    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

        def receive(self, message):
            assert False

    class Child(Actor):
        def post_stop(self):
            self._parent.send('should-not-be-received')

    p = spawn(Parent)

    with assert_one_event(DeadLetter(ANY, ANY)):
        p.stop()


def test_queued_messages_are_logged_as_deadletters_after_stop():
    spawn = TestNode().spawn

    Actor.SENDING_IS_ASYNC = True

    deadletter_event_emitted = Events.consume_one(DeadLetter)

    a = spawn(Actor)

    a.stop()

    a << 'dummy'

    assert (yield deadletter_event_emitted) == DeadLetter(a, 'dummy')


def test_child_termination_message_from_an_actor_not_a_child_of_the_recipient_is_ignored():
    node = TestNode()
    a = node.spawn(Actor)
    a << ('_child_terminated', node.spawn(Actor))


##
## DEATH WATCH

def test_watch_returns_the_actor_that_was_watched():
    """Actor.watch returns the actor that was passed to it"""
    spawn = TestNode().spawn

    class Parent(Actor):
        def pre_start(self):
            a = self.watch(self.spawn(Actor))
            assert a

    spawn(Parent)


def test_watching_running_actor():
    # when spawning is async, the actor is immediately spawn and thus we are watching an already running actor
    return _do_test_watching_actor(async=False)


def test_watching_new_actor():
    # when spawning is synchronous, the actor has not yet been spawn at the time we start watching it
    return _do_test_watching_actor(async=True)


@inlineCallbacks
def _do_test_watching_actor(async=False):
    # We could just set this after we have already spawned the Watcher to avoid needing 2 monitoring Deferreds, but for
    # safety, we set it for the entire duration of the test.
    Actor.SPAWNING_IS_ASYNC = async

    spawn = TestNode().spawn

    watcher_spawned = Deferred()
    message_receieved = Deferred()

    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)
            watcher_spawned.callback(None)

        def receive(self, message):
            message_receieved.callback(message)

    watchee = spawn(Actor)

    spawn(Watcher)
    yield watcher_spawned

    assert not message_receieved.called

    watchee.stop()
    assert (yield message_receieved) == ('terminated', watchee)


def test_watching_self_is_noop_or_warning_and_returns_self():
    """Watching self warns by default and does nothing if explicitly told it's safe"""
    spawn = TestNode().spawn

    class MyActor(Actor):
        def pre_start(self):
            assert self.watch(self.ref) == self.ref

        def receive(self, message):
            assert False, message

    a = spawn(MyActor)

    dead_letter_emitted_d = Events.consume_one(DeadLetter)
    a.stop()
    assert not dead_letter_emitted_d.called


def test_termination_message_contains_ref_that_forwards_to_deadletters():
    spawn = TestNode().spawn

    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            _, sender = message
            with assert_one_event(DeadLetter(sender, 'dummy')):
                sender << 'dummy'

    watchee = spawn(Actor)

    spawn(Watcher)
    watchee.stop()


def test_watching_dead_actor():
    spawn = TestNode().spawn

    message_receieved = Latch()

    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            message_receieved()

    watchee = spawn(Actor)
    watchee.stop()

    spawn(Watcher)

    assert message_receieved


def test_watching_dying_actor():
    spawn = TestNode().spawn

    release_watchee = Trigger()
    message_receieved = Latch()

    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            assert message == ('terminated', ANY)
            message_receieved()

    class SlowWatchee(Actor):
        @inlineCallbacks
        def post_stop(self):
            yield release_watchee

    watchee = spawn(SlowWatchee)
    watchee.stop()

    spawn(Watcher)

    release_watchee()

    assert message_receieved


def test_unhandled_termination_message_causes_receiver_to_raise_unhandledtermination():
    spawn = TestNode().spawn

    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            raise Unhandled

    watchee = spawn(Actor)
    watchee.stop()

    with expect_failure(UnhandledTermination):
        spawn(Watcher)


def test_system_messages_to_dead_actorrefs_are_discarded():
    spawn = TestNode().spawn

    a = spawn(Actor)
    a.stop()

    for event in ['_stop', '_suspend', '_resume', '_restart']:
        d = Events.consume_one(DeadLetter)
        a << event
        assert not d.called, "message %r sent to a dead actor should be discarded" % (event,)
        d.addErrback(lambda f: f.trap(CancelledError)).cancel()


def test_termination_message_to_dead_actor_is_discarded():
    spawn = TestNode().spawn

    class Parent(Actor):
        def pre_start(self):
            self.watch(self.spawn(Actor)).stop()
            self.ref.stop()

    d = Events.consume_one(DeadLetter)
    spawn(Parent)
    assert not d.called, d.result

    d.addErrback(lambda f: f.trap(CancelledError)).cancel()  # just to be nice


@simtime
def test_watching_running_remote_actor_that_stops_causes_termination_message(clock):
    network = MockNetwork(clock)
    node1, node2 = network.node('host1:123'), network.node('host2:123')

    received = Latch()

    class Watcher(Actor):
        def pre_start(self):
            self.watchee = self.watch(self.root.node.lookup('host2:123/remote-watchee'))

        def receive(self, msg):
            eq_(msg, ('terminated', self.watchee))
            received()
    node1.spawn(Watcher)

    remote_watchee = node2.spawn(Actor, name='remote-watchee')

    network.simulate(duration=1.0)
    assert not received

    remote_watchee.stop()

    network.simulate(duration=1.0)
    assert received


@simtime
def test_watching_remote_actor_that_restarts_doesnt_cause_termination_message(clock):
    network = MockNetwork(clock)
    node1, node2 = network.node('host1:123'), network.node('host2:123')

    received = Latch()

    class Watcher(Actor):
        def pre_start(self):
            self.watchee = self.watch(self.root.node.lookup('host2:123/remote-watchee'))

        def receive(self, msg):
            eq_(msg, ('terminated', self.watchee))
            received()
    node1.spawn(Watcher)

    remote_watchee = node2.spawn(Actor, name='remote-watchee')

    network.simulate(duration=1.0)
    assert not received

    remote_watchee << '_restart'

    network.simulate(duration=1.0)
    assert not received


@simtime
def test_watching_nonexistent_remote_actor_causes_termination_message(clock):
    network = MockNetwork(clock)
    node1, _ = network.node('host1:123'), network.node('host2:123')

    received = Latch()

    class Watcher(Actor):
        def pre_start(self):
            self.watchee = self.watch(self.root.node.lookup('host2:123/nonexistent-watchee'))

        def receive(self, msg):
            eq_(msg, ('terminated', self.watchee))
            received()
    node1.spawn(Watcher)

    network.simulate(duration=2.0)
    assert received


@simtime
def test_watching_an_actor_on_a_node_with_whom_connectivity_is_lost_or_limited(clock):
    def test_it(packet_loss_src, packet_loss_dst):
        network = MockNetwork(clock)
        node1, node2 = network.node('watcher-host:123'), network.node('watchee-host:123')

        received = Latch()

        class Watcher(Actor):
            def pre_start(self):
                self.watchee = self.watch(self.root.node.lookup('watchee-host:123/remote-watchee'))

            def receive(self, msg):
                eq_(msg, ('terminated', self.watchee))
                received()
        node1.spawn(Watcher)

        node2.spawn(Actor, name='remote-watchee')

        network.simulate(duration=2.0)
        assert not received

        network.packet_loss(100.0,
                            src='tcp://' + packet_loss_src + '-host:123',
                            dst='tcp://' + packet_loss_dst + '-host:123')
        network.simulate(duration=10.0)
        assert received

    test_it(packet_loss_src='watchee', packet_loss_dst='watcher')
    test_it(packet_loss_src='watcher', packet_loss_dst='watchee')


##
## REMOTING

@simtime
def test_actorref_remote_returns_a_ref_that_when_sent_a_message_delivers_it_on_another_node(clock):
    # This just tests the routing logic and not heartbeat or reliability or deadletters or anything.

    # emulate a scenario in which a single node sends many messages to other nodes;
    # a total of NUM_NODES * NUM_ACTORS messages will be sent out.
    NUM_NODES = 3
    NUM_ACTORS_PER_NODE = 20

    network = MockNetwork(clock)

    sender_node = network.node('senderhost:123')
    assert not network.queue

    recipient_nodes = []

    for node_num in range(1, NUM_NODES + 1):
        nodeaddr = 'recphost%d:123' % node_num
        remote_node = network.node(nodeaddr)

        receive_boxes = []
        sent_msgs = []

        for actor_num in range(1, NUM_ACTORS_PER_NODE + 1):
            actor_box = []  # collects whatever the MockActor receives
            actor = remote_node.spawn(Props(MockActor, actor_box), name='actor%d' % actor_num)
            # we only care about the messages received, not the ref itself
            receive_boxes.append(actor_box)

            # format: dummy-<nodename>-<actorname>-<random-stuff-for-good-measure> (just for debuggability)
            msg = 'dummy-%s-%s-%s' % (nodeaddr, actor.uri.name, random.randint(1, 10000000))
            sender_node.lookup(actor.uri) << msg
            sent_msgs.append(msg)

        recipient_nodes.append((sent_msgs, receive_boxes))

    random.shuffle(network.queue)  # for good measure
    network.simulate(duration=3.0)

    for sent_msgs, receive_boxes in recipient_nodes:
        all(eq_(receive_box, [sent_msg])
            for sent_msg, receive_box in zip(sent_msgs, receive_boxes))


def test_transmitting_refs_and_sending_to_received_refs():
    # This just tests the Ref serialisation and deserialization logic.

    @simtime
    def test_it(clock, make_actor1):
        network = MockNetwork(clock)

        #
        node1 = network.node('host1:123')

        actor1_msgs = MockMessages()
        actor1 = make_actor1(node1, Props(MockActor, actor1_msgs))

        #
        node2 = network.node('host2:123')

        actor2_msgs = []
        node2.spawn(Props(MockActor, actor2_msgs), name='actor2')

        # send: node1 -> node2:
        node1.lookup('host2:123/actor2') << ('msg-with-ref', actor1)

        network.simulate(duration=2.0)

        # reply: node2 -> node1:
        assert actor2_msgs == [ANY], "should be able to send messages to explicitly constructed remote refs"
        _, received_ref = actor2_msgs[0]
        received_ref << ('hello', received_ref)

        network.simulate(duration=2.0)
        assert actor1_msgs == [('hello', received_ref)], "should be able to send messages to received remote refs"

        # send to self without knowing it
        (_, re_received_ref), = actor1_msgs
        del actor1_msgs[:]
        re_received_ref << 'to-myself'

        assert actor1_msgs == ['to-myself']

    @test_it
    def make_toplevel(node, factory):
        return node.spawn(factory, name='actor1')

    @test_it
    def make_non_toplevel(node, factory):
        class Parent(Actor):
            def pre_start(self):
                child << self.spawn(factory, name='actor1')

        child = Slot()
        node.spawn(Parent, name='parent')
        return child()


@simtime
def test_sending_remote_refs(clock):
    """Sending remote refs.

    The sender acquires a remote ref to an actor on the target and sends it to the sender, who then sends a message.
    to the target. It forms a triangle where 1) M obtains a reference to T, 2) sends it over to S, and then 3) S uses it
    to start communication with T.

    T ---- S
     \   /
      \ /
       M

    """
    network = MockNetwork(clock)

    #
    target_node = network.node('target:123')
    target_msgs = []
    target_node.spawn(Props(MockActor, target_msgs), name='T')

    #
    sender_node = network.node('sender:123')

    class SenderActor(Actor):
        def receive(self, msg):
            assert msg == ('send-msg-to', ANY)
            _, target = msg
            target << 'helo'
    sender_node.spawn(SenderActor, name='S')

    #
    middle_node = network.node('middle:123')
    ref_to_sender = middle_node.lookup('sender:123/S')
    ref_to_target = middle_node.lookup('target:123/T')
    ref_to_sender << ('send-msg-to', ref_to_target)

    network.simulate(duration=1.0)

    eq_(target_msgs, ['helo'])


@simtime
def test_messages_sent_to_nonexistent_remote_actors_are_deadlettered(clock):
    network = MockNetwork(clock)

    sender_node, _ = network.node('sendernode:123'), network.node('receivernode:123')

    noexist = sender_node.lookup('receivernode:123/non-existent-actor')
    noexist << 'straight-down-the-drain'
    with assert_one_event(RemoteDeadLetter):
        network.simulate(0.2)


## HEARTBEAT

@simtime
def test_sending_to_an_unknown_node_doesnt_start_if_the_node_doesnt_become_visible_and_the_message_is_later_dropped(clock):
    network = MockNetwork(clock)

    sender_node = network.node('sender:123')
    sender_node.hub.QUEUE_ITEM_LIFETIME = 10.0

    # recipient host not reachable within time limit--message dropped after `QUEUE_ITEM_LIFETIME`

    ref = sender_node.lookup('nonexistenthost:123/actor2')
    ref << 'bar'

    with assert_one_event(DeadLetter(ref, 'bar')):
        network.simulate(sender_node.hub.QUEUE_ITEM_LIFETIME + 1.0)


@simtime
def test_sending_to_an_unknown_host_that_becomes_visible_in_time(clock):
    network = MockNetwork(clock)

    node1 = network.node('host1:123')
    node1.hub.QUEUE_ITEM_LIFETIME = 10.0

    # recipient host reachable within time limit

    ref = node1.lookup('host2:123/actor1')
    ref << 'foo'
    with assert_event_not_emitted(DeadLetter):
        network.simulate(duration=1.0)

    node2 = network.node('host2:123')
    actor2_msgs = []
    node2.spawn(Props(MockActor, actor2_msgs), name='actor1')

    network.simulate(duration=3.0)

    assert actor2_msgs == ['foo']


@simtime
def test_sending_stops_if_visibility_is_lost(clock):
    network = MockNetwork(clock)

    node1 = network.node('host1:123')

    network.node('host2:123')

    # set up a normal sending state first

    ref = node1.lookup('host2:123/actor2')
    ref << 'foo'  # causes host2 to also start the heartbeat

    network.simulate(duration=1.0)

    # ok, now they both know/have seen each other; let's change that:
    network.packet_loss(percent=100.0, src='tcp://host2:123', dst='tcp://host1:123')
    network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)

    # the next message should fail after 5 seconds
    ref << 'bar'

    with assert_one_event(DeadLetter(ref, 'bar')):
        network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)


@simtime
def test_sending_resumes_if_visibility_is_restored(clock):
    network = MockNetwork(clock)

    node1 = network.node('host1:123')

    network.node('host2:123')

    ref = node1.lookup('host2:123/actor2')
    ref << 'foo'
    network.simulate(duration=1.0)
    # now both visible to the other

    network.packet_loss(percent=100.0, src='tcp://host2:123', dst='tcp://host1:123')
    network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)
    # now host2 is not visible to host1

    ref << 'bar'
    network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE - 1.0)
    # host1 still hasn't lost faith

    network.packet_loss(percent=0, src='tcp://host2:123', dst='tcp://host1:123')
    # and it is rewarded for its patience
    with assert_event_not_emitted(DeadLetter):
        network.simulate(duration=2.0)


## REMOTE NAME-TO-PORT MAPPING

def test_TODO_node_identifiers_are_mapped_to_addresses_on_the_network():
    pass  # nodes connect to remote mappers on demand


def test_TODO_node_addresses_are_discovered_automatically_using_a_mapper_daemon_on_each_host():
    pass  # node registers itself


def test_TODO_nodes_can_acquire_ports_automatically():
    pass  # node tries several ports (or asks the mapper?) and settles with the first available one


def test_TODO_mapper_daemon_can_be_on_a_range_of_ports():
    pass


## OPTIMIZATIONS

@simtime
def test_incoming_refs_pointing_to_local_actors_are_converted_to_local_refs(clock):
    network = MockNetwork(clock)

    # node1:

    node1 = network.node('host1:123')

    actor1_msgs = MockMessages()
    actor1 = node1.spawn(Props(MockActor, actor1_msgs), name='actor1')

    # node2:

    node2 = network.node('host2:123')
    # guardian2 = Guardian(hub=hub2)

    actor2_msgs = []
    node2.spawn(Props(MockActor, actor2_msgs), name='actor2')

    # send from node1 -> node2:
    node1.lookup('host2:123/actor2') << ('msg-with-ref', actor1)
    network.simulate(duration=2.0)

    # reply from node2 -> node1:
    _, received_ref = actor2_msgs[0]
    received_ref << ('msg-with-ref', received_ref)

    network.simulate(duration=2.0)
    (_, remote_local_ref), = actor1_msgs
    assert remote_local_ref.is_local


@simtime
def test_looking_up_addresses_that_actually_point_to_the_local_node_return_a_local_ref(clock):
    node = MockNetwork(clock).node('localhost:123')
    node.spawn(Actor, name='localactor')
    ref = node.lookup('localhost:123/localactor')
    assert ref.is_local


@simtime
def test_sending_to_a_remote_ref_that_points_to_a_local_ref_is_redirected(clock):
    network = MockNetwork(clock)
    node = network.node('localhost:123')

    msgs = []
    node.spawn(Props(MockActor, msgs), name='localactor')

    ref = Ref(cell=None, uri=Uri.parse('localhost:123/localactor'), is_local=False, hub=node.hub)
    ref << 'foo'

    network.simulate(5.0)
    assert msgs == ['foo']
    assert ref.is_local

    ref << 'bar'
    # no network.simulate should be needed
    assert msgs == ['foo', 'bar']


# ZMQ

def test_remoting_with_real_zeromq():
    from nose.twistedtools import reactor
    from txzmq import ZmqFactory, ZmqPushConnection, ZmqPullConnection

    class MyActor(Actor):
        def __init__(self, msgs, triggers):
            self.msgs = msgs
            self.triggers = triggers

        def receive(self, msg):
            self.msgs.append(msg)
            self.triggers.pop(0)()

    f1 = ZmqFactory()
    insock = ZmqPullConnection(f1)
    outsock_factory = lambda: ZmqPushConnection(f1, linger=0)
    node1 = Node(hub=Hub(insock, outsock_factory, '127.0.0.1:19501'))

    f2 = ZmqFactory()
    insock = ZmqPullConnection(f2, linger=0)
    outsock_factory = lambda: ZmqPushConnection(f2, linger=0)
    node2 = Node(hub=Hub(insock, outsock_factory, '127.0.0.1:19502'))

    yield sleep(0.001)

    actor1_msgs, actor1_triggers = MockMessages(), [Barrier(), Barrier(), Barrier()]
    actor1 = node1.spawn(Props(MyActor, actor1_msgs, actor1_triggers), 'actor1')

    actor2_msgs, actor2_triggers = MockMessages(), [Barrier(), Barrier(), Barrier()]
    node2.spawn(Props(MyActor, actor2_msgs, actor2_triggers), 'actor2')

    # simple message: @node1 => actor2@node2
    actor2_from_node1 = node1.lookup('127.0.0.1:19502/actor2')

    actor2_from_node1 << 'helloo!'
    yield actor2_triggers[0]
    eq_(actor2_msgs.clear(), ['helloo!'])

    # message containing actor1's ref: @node1 => actor2@node2
    actor2_from_node1 << actor1

    yield actor2_triggers[0]
    tmp = actor2_msgs.clear()
    eq_(tmp, [actor1])
    actor1_from_node2 = tmp[0]

    # message to received actor1's ref: @node2 => actor1@node1
    actor1_from_node2 << 'helloo2!'

    yield actor1_triggers[0]
    eq_(actor1_msgs.clear(), ['helloo2!'])

    # message containing the received actor1's ref to actor1 (via the same ref): @node2 => actor1@node1
    actor1_from_node2 << ('msg-with-ref', actor1_from_node2)

    yield actor1_triggers[0]
    tmp = actor1_msgs.clear()
    eq_(tmp, [('msg-with-ref', actor1)])
    _, self_ref = tmp[0]
    ok_(self_ref.is_local)
    ok_(self_ref, actor1)

    self_ref << 'hello, stranger!'
    eq_(actor1_msgs.clear(), ['hello, stranger!'])


##
## PROCESSES

def test_process_run_must_return_a_generator():
    with assert_raises(TypeError):
        class MyProc(Process):
            def run(self):
                pass


def test_processes_run_is_called_when_the_process_is_spawned():
    spawn = TestNode().spawn

    run_called = Latch()

    class MyProc(Process):
        def run(self):
            run_called()
            yield

    spawn(MyProc)

    assert run_called


def test_process_run_is_a_coroutine():
    spawn = TestNode().spawn

    step1_reached = Latch()
    release = Trigger()
    step2_reached = Latch()

    class MyProc(Process):
        def run(self):
            step1_reached()
            yield release
            step2_reached()

    spawn(MyProc)

    assert step1_reached
    assert not step2_reached

    release()
    assert step2_reached


def test_warning_is_emitted_if_process_run_returns_a_value():
    spawn = TestNode().spawn

    class ProcThatReturnsValue(Process):
        def run(self):
            yield
            returnValue('foo')

    with assert_one_warning():
        spawn(ProcThatReturnsValue)


def test_process_run_is_paused_and_unpaused_if_the_actor_is_suspended_and_resumed():
    spawn = TestNode().spawn

    release = Trigger()
    after_release = Latch()

    class MyProc(Process):
        def run(self):
            yield release
            after_release()

    p = spawn(MyProc)
    assert not after_release

    p << '_suspend'
    release()
    assert not after_release

    p << '_resume'
    assert after_release


def test_process_run_is_cancelled_if_the_actor_is_stopped():
    spawn = TestNode().spawn

    exited = Latch()

    class MyProc(Process):
        def run(self):
            try:
                yield self.get()
            except GeneratorExit:
                exited()

    p = spawn(MyProc)
    p.stop()
    assert exited


def test_sending_to_a_process_injects_the_message_into_its_coroutine():
    spawn = TestNode().spawn

    random_message = 'dummy-%s' % (random.random(),)

    received_message = Slot()

    class MyProc(Process):
        def run(self):
            msg = yield self.get()
            received_message << msg

    p = spawn(MyProc)
    assert not received_message
    p << random_message

    assert received_message == random_message


def test_getting_two_messages_in_a_row_waits_till_the_next_message_is_received():
    spawn = TestNode().spawn

    second_message = Slot()
    first_message = Slot()

    class MyProc(Process):
        def run(self):
            first_message << (yield self.get())
            second_message << (yield self.get())

    p = spawn(MyProc)
    p << 'dummy1'
    eq_(first_message, 'dummy1')
    assert not second_message

    p << 'dummy2'
    eq_(second_message, 'dummy2')


def test_sending_to_a_process_that_is_processing_a_message_queues_it():
    spawn = TestNode().spawn

    first_message_received = Latch()
    second_message_received = Latch()

    release_proc = Trigger()

    class MyProc(Process):
        def run(self):
            yield self.get()
            first_message_received()

            yield release_proc

            yield self.get()
            second_message_received()

    p = spawn(MyProc)
    p << 'dummy'
    assert first_message_received

    p << 'dummy2'
    assert not second_message_received
    release_proc()
    assert second_message_received


def test_errors_in_process_run_before_the_first_get_are_reported_as_startup_errors():
    spawn = TestNode().spawn

    class MyProc(Process):
        def run(self):
            raise MockException
            yield

    with expect_failure(CreateFailed):
        spawn(MyProc)

    #

    release = Trigger()

    class MyProcWithSlowStartup(Process):
        def run(self):
            yield release
            raise MockException

    spawn(MyProcWithSlowStartup)

    with expect_failure(CreateFailed) as basket:
        release()

    with assert_raises(MockException):
        basket[0].raise_original()


def test_errors_in_process_while_processing_a_message_are_reported_as_normal_failures():
    spawn = TestNode().spawn

    class MyProc(Process):
        def run(self):
            yield self.get()
            raise MockException

    p = spawn(MyProc)

    with expect_failure(MockException):
        p << 'dummy'

    #

    release = Trigger()

    class MyProcWithSlowStartup(Process):
        def run(self):
            yield release
            yield self.get()
            raise MockException

    p = spawn(MyProcWithSlowStartup)
    release()
    with expect_failure(MockException):
        p << 'dummy'


def test_errors_in_process_when_retrieving_a_message_from_queue_are_reported_as_normal_failures():
    spawn = TestNode().spawn

    release = Trigger()

    class MyProc(Process):
        def run(self):
            yield self.get()
            yield release
            yield self.get()
            raise MockException

    p = spawn(MyProc)
    p << 'dummy1'
    p << 'dummy2'
    with expect_failure(MockException):
        release()


def test_restarting_a_process_reinvokes_its_run_method():
    spawn = TestNode().spawn

    proc = Slot()
    supervision_invoked = Trigger()
    restarted = Trigger()
    post_stop_called = Latch()

    class Parent(Actor):
        def supervise(self, exc):
            supervision_invoked()
            return Restart

        def pre_start(self):
            proc << self.spawn(MyProc)

    class MyProc(Process):
        started = 0

        def run(self):
            MyProc.started += 1
            if MyProc.started == 2:
                restarted()
                return
            yield self.get()
            raise MockException

        def post_stop(self):
            post_stop_called()

    spawn(Parent)

    proc() << 'dummy'
    yield supervision_invoked
    yield restarted
    assert post_stop_called


def test_error_in_process_suspends_and_taints_and_resuming_it_warns_and_restarts_it():
    spawn = TestNode().spawn

    proc = Slot()
    restarted = Trigger()

    class Parent(Actor):
        def supervise(self, exc):
            return Resume

        def pre_start(self):
            proc << self.spawn(MyProc)

    class MyProc(Process):
        started = 0

        def run(self):
            MyProc.started += 1
            if MyProc.started == 2:
                restarted()
                return
            yield self.get()
            raise MockException

    spawn(Parent)

    proc() << 'dummy'

    with assert_one_warning():  # reporting is async, so the warning should be emitted at some point as we're waiting
        yield restarted


def test_errors_while_stopping_and_finalizing_are_treated_the_same_as_post_stop_errors():
    spawn = TestNode().spawn

    class MyProc(Process):
        def run(self):
            try:
                yield self.get()
            finally:
                # this also covers the process trying to yield stuff, e.g. for getting another message
                raise MockException

    with assert_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        spawn(MyProc).stop()


def test_all_queued_messages_are_reported_as_unhandled_on_flush():
    spawn = TestNode().spawn

    release = Trigger()

    class MyProc(Process):
        def run(self):
            yield self.get()
            yield release
            self.flush()

    p = spawn(MyProc)
    p << 'dummy'
    p << 'should-be-reported-as-unhandled'
    with assert_one_event(UnhandledMessage(p, 'should-be-reported-as-unhandled')):
        release()


def test_process_is_stopped_when_the_coroutine_exits():
    spawn = TestNode().spawn

    class MyProc(Process):
        def run(self):
            yield self.get()

    p = spawn(MyProc)
    p << 'dummy'
    yield p.join()


def test_process_is_stopped_when_the_coroutine_exits_during_startup():
    spawn = TestNode().spawn

    class MyProc(Process):
        def run(self):
            yield

    p = spawn(MyProc)
    yield p.join()


def test_process_can_get_messages_selectively():
    spawn = TestNode().spawn

    messages = []

    release1 = Trigger()
    release2 = Trigger()

    class MyProc(Process):
        def run(self):
            messages.append((yield self.get(ANY)))
            messages.append((yield self.get('msg3')))
            messages.append((yield self.get(ANY)))

            yield release1

            messages.append((yield self.get(IS_INSTANCE(int))))

            yield release2

            messages.append((yield self.get(IS_INSTANCE(float))))

    p = spawn(MyProc)
    p << 'msg1'
    eq_(messages, ['msg1'])

    p << 'msg2'
    eq_(messages, ['msg1'])

    p << 'msg3'
    eq_(messages, ['msg1', 'msg3', 'msg2'])

    # (process blocked here)

    p << 'not-an-int'
    release1()
    assert 'not-an-int' not in messages

    p << 123
    assert 123 in messages

    # (process blocked here)

    p << 321
    p << 32.1

    release2()
    assert 321 not in messages and 32.1 in messages


def test_process_can_delegate_handling_of_caught_exceptions_to_parent():
    spawn = TestNode().spawn

    process_continued = Latch()
    supervision_invoked = Trigger()

    class Parent(Actor):
        def supervise(self, exc):
            assert isinstance(exc, MockException)
            supervision_invoked()
            return Restart

        def pre_start(self):
            self.spawn(Child) << 'invoke'

    class Child(Process):
        def run(self):
            yield self.get()  # put the process into receive mode (i.e. started)
            try:
                raise MockException()
            except MockException:
                yield self.escalate()
            process_continued()

    spawn(Parent)

    yield supervision_invoked
    assert not process_continued
    yield sleep(0)


def test_calling_escalate_outside_of_error_context_causes_runtime_error():
    spawn = TestNode().spawn

    exc_raised = Latch()

    class FalseAlarmParent(Actor):
        def supervise(self, exc):
            ok_(isinstance(exc, InvalidEscalation))
            exc_raised()
            return Stop

        def pre_start(self):
            self.spawn(FalseAlarmChild)

    class FalseAlarmChild(Process):
        def run(self):
            self << 'foo'
            yield self.get()  # put in started-mode
            self.escalate()

    spawn(FalseAlarmParent)
    assert exc_raised


def test_attempt_to_delegate_an_exception_during_startup_instead_fails_the_actor_immediately():
    spawn = TestNode().spawn

    started = Counter()

    class Parent(Actor):
        def supervise(self, exc):
            assert isinstance(exc, CreateFailed)
            return Resume  # should cause a restart instead

        def pre_start(self):
            self.spawn(Child)

    class Child(Process):
        def run(self):
            started()
            if started == 1:
                try:
                    raise MockException()
                except MockException:
                    yield self.escalate()

    with swallow_one_warning():
        spawn(Parent)

    assert started == 2


def test_optional_process_high_water_mark_emits_an_event_for_every_multiple_of_that_nr_of_msgs_in_the_queue():
    spawn = TestNode().spawn

    class MyProc(Process):
        hwm = 100  # emit warning every 100 pending messages in the queue

        def run(self):
            yield self.get()  # put in receive mode
            yield Deferred()  # queue all further messages

    p = spawn(MyProc)
    p << 'ignore'

    for _ in range(3):
        for _ in range(99):
            p << 'dummy'
        with assert_one_event(HighWaterMarkReached):
            p << 'dummy'


##
## SUBPROCESS

def test_TODO_spawning_an_actor_in_subprocess_uses_a_special_agent_guardian():
    pass  # TODO: subprocess contains another actor system; maybe add a
          # guardian actor like in Akka, and that guardian could be
          # different when running in a subprocess and instead of dumping a log message on stderr
          # it prints a parsable message


def test_TODO_entire_failing_subproccess_reports_the_subprocessed_actor_as_terminated():
    # TODO: and how to report  the cause?
    pass


def test_TODO_stopping_a_subprocessed_actor_kills_the_subprocess():
    pass


##
##  MIGRATION

def test_TODO_migrating_an_actor_to_another_host_suspends_serializes_and_deserializes_it_and_makes_it_look_as_if_it_had_been_deployed_remotely():
    pass


def test_TODO_migrating_an_actor_doesnt_break_existing_refs():
    pass


def test_TODO_migrating_an_actor_redirects_all_actorrefs_to_it():
    pass


def test_TODO_actor_is_allowed_to_fail_to_be_serialized():
    pass


##
## TYPED ACTORREFS

def test_TODO_typed_actorrefs():
    pass


##
## HELPFUL & ASSISTIVE TRACEBACKS

def test_TODO_correct_traceback_is_always_reported():
    pass


def test_TODO_assistive_traceback_with_send_and_spawn():
    pass


def test_TODO_assistive_traceback_with_recursive_send():
    pass


def test_TODO_assistive_traceback_with_async_interaction():
    pass


##
## DISPATCHING

def test_TODO_suspend_and_resume_doesnt_change_global_message_queue_ordering():
    pass


# SUPPORT

class MockException(Exception):
    pass


def TestNode():
    return Node(hub=HubWithNoRemoting())


def wrap_globals():
    """Ensures that errors in actors during tests don't go unnoticed."""

    def wrap(fn):
        if inspect.isgeneratorfunction(fn):
            fn = inlineCallbacks(fn)

        @functools.wraps(fn)
        @deferred(timeout=fn.timeout if hasattr(fn, 'timeout') else None)
        @inlineCallbacks
        def ret():
            # dbg("\n============================================\n")

            import spinoff.actor._actor
            spinoff.actor._actor.TESTING = True

            Actor.reset_flags(debug=True)

            # TODO: once the above TODO (fresh Node for each test fn) is complete, consider making Events non-global by
            # having each Node have its own Events instance.
            Events.reset()

            with ErrorCollector():
                try:
                    yield fn()
                finally:
                    # dbg("TESTWRAP: ------------------------- cleaning up after %s" % (fn.__name__,))

                    yield Node.stop_all()

                    if _idle_calls:
                        # dbg("TESTWRAP: processing all remaining scheduled calls...")
                        _process_idle_calls()
                        # dbg("TESTWRAP: ...scheduled calls done.")

                    if '__pypy__' not in sys.builtin_module_names:
                        gc.collect()
                        for trash in gc.garbage[:]:
                            if isinstance(trash, DebugInfo):
                                # dbg("DEBUGINFO: __del__")
                                if trash.failResult is not None:
                                    exc = Unclean(trash._getDebugTracebacks())
                                    trash.__dict__.clear()
                                    raise exc
                                gc.garbage.remove(trash)

                        assert not gc.garbage, "Memory leak detected"

                        # if gc.garbage:
                        #     dbg("GARGABE: detected after %s:" % (fn.__name__,), len(gc.garbage))
                        #     import objgraph as ob
                        #     import os

                        #     def dump_chain(g_):
                        #         def calling_test(x):
                        #             if not isframework(x):
                        #                 return None
                        #         import spinoff
                        #         isframework = lambda x: type(x).__module__.startswith(spinoff.__name__)
                        #         ob.show_backrefs([g_], filename='backrefs.png', max_depth=100, highlight=isframework)

                        #     for gen in gc.garbage:
                        #         dump_chain(gen)
                        #         dbg("   TESTWRAP: mem-debuggin", gen)
                        #         import pdb; pdb.set_trace()
                        #         os.remove('backrefs.png')

                        # to avoid the above assertion from being included as part of any tracebacks from the test
                        # function--the last line of a context managed block is included in tracebacks originating
                        # from context managers.
                        pass

        return ret

    for name, value in globals().items():
        if name.startswith('test_') and callable(value):
            globals()[name] = wrap(value)
wrap_globals()
del wrap_globals
