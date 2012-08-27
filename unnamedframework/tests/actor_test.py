from __future__ import print_function

import functools
import weakref
import random
import sys
from nose.twistedtools import deferred
from nose.tools import eq_

from twisted.internet.defer import Deferred, inlineCallbacks, DeferredQueue, fail, CancelledError

from unnamedframework.util.testing import assert_raises, assert_one_warning, assert_no_warnings, swallow_one_warning
from unnamedframework.util.pattern_matching import ANY, IS_INSTANCE

from unnamedframework.actor import (
    spawn, Actor, Props, Guardian, Unhandled, NameConflict, UnhandledTermination, CreateFailed,
    BadSupervision
)
from unnamedframework.util.testing import (
    MockMessages, assert_one_event, ErrorCollector, EvSeq, EVENT, NEXT, Latch, Trigger, Counter, expect_failure, Slot,
)
from unnamedframework.actor.events import Events, UnhandledMessage, DeadLetter, ErrorIgnored, HighWaterMarkReached
from unnamedframework.actor.supervision import Resume, Restart, Stop, Escalate, Default
from unnamedframework.actor.process import Process


def dbg(*args):
    print(file=sys.stderr, *args)


class MockException(Exception):
    pass


##
## SENDING & RECEIVING

def test_sent_message_is_received():
    """Trivial send and receive.

    By default, when sending a message to an actor, it is delivered to and handled by the receiver immediately for
    performance reasons.

    """
    messages = MockMessages()

    class MyActor(Actor):
        def receive(self, message):
            messages.append(message)

    a = spawn(MyActor)
    a << 'foo'
    assert messages.clear() == ['foo']


def test_sending_operator_is_chainable():
    messages = MockMessages()

    class MyActor(Actor):
        def receive(self, message):
            messages.append(message)

    a = spawn(MyActor)
    a << 'foo' << 'bar'
    assert messages.clear() == ['foo', 'bar']


@deferred(timeout=0.01)
@inlineCallbacks
def test_async_sending():
    """Asynchronous sending by setting SENDING_IS_ASYNC or force_async=True.

    Optionally, it is possible to guarantee delayed deliveries by setting SENDING_IS_ASYNC or passing
    `force_async=True` to `send`.

    Caveat: if you do an async send, but the message is delayed because the actor is suspended or hasn't started yet,
    the message will be received immediately when the actor is resumed or started, respectively.

    """
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
        msg1 = yield messages.get()
        assert msg1 == 'foo'
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
    receive_generator_started = Latch()

    class MyActor(Actor):
        def receive(self, message):
            receive_generator_started()
            yield

    spawn(MyActor) << None
    assert receive_generator_started


def test_receive_of_the_same_actor_never_executes_concurrently_even_with_deferred_receives():
    """Also deferred receive of the same actor is never concurrent.

    This allows for conveniently writing delayed completion of handling messages.

    An actor's `receive` method is allowed do delayed (non-blocking) processing by yielding or returning Deferreds.
    This test rules out concurrent receives on the same actor in case of a deferred-receive.

    See the comment about yielding vs returning Deferreds in
    test_receive_is_auto_wrapped_with_txcoroutine_if_its_a_generator_function.

    """
    receive_called = Counter()
    triggers = []

    def _do_test(actor_cls):
        # initialize/reset shared inspection/control variables
        receive_called.reset()

        release = Trigger()
        triggers[:] = [Trigger(), release]

        a = spawn(actor_cls)
        a << None << None
        assert receive_called == 1

        release()
        assert receive_called == 2

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

    class MyActor(Actor):
        def receive(self, message):
            raise Unhandled

    a = spawn(MyActor)
    with assert_one_event(UnhandledMessage(a, 'foo')):
        a << 'foo'


def test_with_no_receive_method_all_messages_are_unhandled():
    a = spawn(Actor)
    with assert_one_event(UnhandledMessage(a, 'dummy')):
        a << 'dummy'


##
## SPAWNING

@deferred(timeout=0.01)
@inlineCallbacks
def test_spawning_is_async():
    """Spawning child actors by default is delayed and the spawn call returns immediately."""

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
    Actor.SPAWNING_IS_ASYNC = False

    actor_spawned = Latch()

    class MyActor(Actor):
        def pre_start(self):
            actor_spawned()

    spawn(MyActor)
    assert actor_spawned


def test_spawning_a_toplevel_actor_assigns_guardian_as_its_parent():
    """Top-level actor's parent is the Guardian"""
    pre_start_called = Latch()

    class MyActor(Actor):
        def pre_start(self):
            assert self._parent is Guardian
            pre_start_called()

    spawn(MyActor)
    assert pre_start_called


def test_spawning_child_actor_assigns_the_spawner_as_parent():
    """Child actor's parent is the spawner"""
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

    spawn(MyActor) << 'init'
    assert message_received


def test_pre_start_can_return_a_Deferred():
    """Pre start can return a Deferred."""

    class MyActor(Actor):
        def pre_start(self):
            return Deferred()

    spawn(MyActor)


def test_pre_start_can_be_a_coroutine():
    """Pre start can return a Deferred."""
    pre_start_called = Latch()
    release = Trigger()
    pre_start_continued = Latch()

    class MyActor(Actor):
        def pre_start(self):
            pre_start_called()
            yield release
            pre_start_continued()

    spawn(MyActor)

    assert pre_start_called
    assert not pre_start_continued

    release()

    assert pre_start_continued


def test_actor_is_not_started_until_deferred_pre_start_completes():
    """Pre start can return a Deferred or be a coroutine.

    The actor is not considered to be started (i.e. ready to receive messages) until `pre_start` completes.

    """

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

    class MyActor(Actor):
        def pre_start(self):
            return fail(MockException())

    with expect_failure(CreateFailed) as basket:
        spawn(MyActor)
    with assert_raises(MockException):
        basket[0].raise_original()


def test_sending_to_self_does_not_deliver_the_message_until_after_the_actor_is_started():
    message_received = Latch()

    class MyActor(Actor):
        def pre_start(self):
            self.ref << 'dummy'
            assert not message_received

        def receive(self, message):
            message_received()

    spawn(MyActor)

    assert message_received


##

def test_TODO_remote_spawning():
    pass


def test_TODO_remote_actorref_determinism():
    pass


##
## LIFECYCLE

def test_suspending():
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


def test_suspending_with_nonempty_inbox_while_receive_is_blocked():
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


def test_TODO_stopping():
    pass


def test_TODO_force_stopping_does_not_wait_for_a_deferred_post_stop_to_complete():
    pass


def test_resuming():
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
    actor_started = Counter()

    class MyActor(Actor):
        def pre_start(self):
            actor_started()

    a = spawn(MyActor)
    a._stop_noevent()
    a << '_restart'

    assert actor_started == 1


def test_restarting_doesnt_destroy_the_inbox():
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
    started = Latch()
    child = Slot()

    class Parent(Actor):
        def pre_start(self):
            child << self.spawn(Child)

        def supervise(self, exc):
            assert (isinstance(exc, MockException) or
                    isinstance(exc, CreateFailed) and
                    isinstance(exc.cause, MockException))
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
    stopped = Counter()
    deferred_cancelled = []

    mock_d = Deferred(deferred_cancelled.append)

    class MyActor(Actor):
        def receive(self, message):
            return mock_d

        def post_stop(self):
            stopped()

    a = spawn(MyActor) << 'foo'
    a._stop_noevent()

    assert not deferred_cancelled
    assert not stopped

    mock_d.callback(None)
    assert stopped


def test_messages_sent_by_child_post_stop_to_restarting_parent_are_processed_after_restart():
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
    class MyActor(Actor):
        def receive(self, _):
            received()

    received = Counter()
    a = spawn(MyActor)
    a << None

    assert received == 1

    received.reset()
    a._stop_noevent()
    assert not received, "the '_stop' message should not be receivable in the actor"
    with assert_one_event(DeadLetter(a, None)):
        a << None


def test_stopping_calls_post_stop():
    post_stop_called = Latch()

    class MyActor(Actor):
        def post_stop(self):
            post_stop_called()

    spawn(MyActor)._stop_noevent()
    assert post_stop_called


@deferred(timeout=0.01)
@inlineCallbacks
def test_stopping_waits_for_post_stop():
    yield

    stop_complete = Trigger()
    parent_received = Slot()

    class Parent(Actor):
        def pre_start(self):
            self.child = self.watch(self.spawn(Child))

        def receive(self, message):
            if message == 'stop-child':
                self.child.stop()
            else:
                parent_received << message

    class Child(Actor):
        def post_stop(self):
            return stop_complete

    a = spawn(Parent)
    a << 'stop-child'
    assert not parent_received
    stop_complete()
    assert parent_received


@deferred(timeout=0.01)
@inlineCallbacks
def test_actor_is_not_stopped_until_its_children_are_stopped():
    yield

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
    a._stop_noevent()
    assert not parent_stopped
    stop_complete()
    assert parent_stopped


@deferred(timeout=0.01)
@inlineCallbacks
def test_actor_is_not_restarted_until_its_children_are_stopped():
    yield

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
    p._stop_noevent()
    assert not post_stop_called
    with expect_failure(MockException):
        release_child()


def test_stopping_an_actor_with_a_pending_deferred_receive_doesnt_cancel_the_deferred():
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
    a._stop_noevent()
    assert not canceller_called
    assert not stopped
    mock_d.callback(None)
    assert stopped


def test_stopping_in_pre_start_directs_any_refs_to_deadletters():
    message_received = Latch()

    class MyActor(Actor):
        def pre_start(self):
            self.ref._stop_noevent()

        def receive(self, message):
            message_received()

    a = spawn(MyActor)

    with assert_one_event(DeadLetter(a, 'dummy')):
        a << 'dummy'

    assert not message_received


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
## ACTORREFS

# def test_TODO_actorrefs_with_equal_paths_are_equal():
#     assert ActorRef(None, path='123') == ActorRef(None, path='123')


def test_actors_are_garbage_collected_on_termination():
    del_called = Latch()

    class MyActor(Actor):
        def __del__(self):
            del_called()

    ac = Guardian.spawn(MyActor)
    assert not del_called

    ac._stop_noevent()
    assert del_called


def test_cells_are_garbage_collected_on_termination():
    ac = Guardian.spawn(Actor)

    cell = weakref.ref(ac.target)
    assert cell()
    ac._stop_noevent()
    assert not cell()


def test_messages_to_dead_actors_are_sent_to_dead_letters():
    a = Guardian.spawn(Actor)
    a._stop_noevent()

    with assert_one_event(DeadLetter(a, 'should-end-up-as-letter')):
        a << 'should-end-up-as-letter'


def test_guardians_path_is_slash():
    assert Guardian.path == '/'


def test_toplevel_actorrefs_paths_are_prefixed_with_guardians_path():
    a = spawn(Actor, name='a')
    assert a.path == '/a'

    b = spawn(Actor, name='b')
    assert b.path == '/b'


def test_non_toplevel_actorrefs_are_prefixed_with_their_parents_path():
    child_ref = Slot()

    class MyActor(Actor):
        def pre_start(self):
            child_ref << self.spawn(Actor, name='child')

    a = spawn(MyActor, name='parent')
    assert child_ref and child_ref().path == a.path + '/child'


def test_toplevel_actor_paths_must_be_unique():
    spawn(Actor, name='a')
    with assert_raises(NameConflict):
        spawn(Actor, name='a')


def test_non_toplevel_actor_paths_must_be_unique():
    class MyActor(Actor):
        def pre_start(self):
            self.spawn(Actor, name='a')
            with assert_raises(NameConflict):
                self.spawn(Actor, name='a')

    spawn(MyActor, name='a')


def test_spawning_toplevel_actors_without_name_assigns_autogenerated_names():
    a = spawn(Actor)
    assert a.path[1:]

    assert a.path.startswith('/') and '/' not in a.path[1:]

    b = spawn(Actor)
    assert b.path[1:]

    assert b.path != a.path


def test_spawning_non_toplevel_actors_without_name_assigns_autogenerated_names_with_prefixed_parent_path():
    class MyActor(Actor):
        def pre_start(self):
            l = len(self.ref.path)

            a = self.spawn(Actor)
            assert a.path[l:]

            assert a.path.startswith(self.ref.path + '/'), a.path

            b = self.spawn(Actor)
            assert b.path[l:]

            assert a.path != b.path

    spawn(MyActor)


def test_spawning_with_autogenerated_looking_name_raises_an_exception():
    with assert_raises(ValueError):
        spawn(Actor, name='$1')


def test_TODO_looking_up_an_actor_by_its_absolute_path_returns_a_reference_to_it():
    pass


def test_TODO_looking_up_an_actor_by_a_relative_path_returns_a_reference_to_it():
    pass


def test_TODO_looking_up_an_actor_by_a_parent_traversing_relative_path_returns_a_reference_to_it():
    pass


def test_TODO_looking_up_a_non_existent_path_returns_an_empty_reference():
    pass


def test_TODO_messages_to_nonexistent_actors_are_sent_to_dead_letters():
    # TODO: this requires actor lookups by path
    # (absolute lookups are trivial but not relative lookups)
    pass


def test_TODO_stopped_actors_remote_refs_are_optimised():
    pass


##
## SUPERVISION & ERROR HANDLING

def test_child_is_resumed_if_supervise_returns_resume():
    """Child is resumed if `supervise` returns `Resume`"""
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


@deferred(timeout=0.01)
@inlineCallbacks
def test_child_error_suspends_child():
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
    class MyActor(Actor):
        def receive(self, _):
            return fail(MockException())

    a = spawn(MyActor)

    with expect_failure(MockException):
        a << 'dummy'


def test_exception_after_stop_is_ignored_and_does_not_report_to_parent():
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


@deferred(timeout=0.01)
@inlineCallbacks
def test_default_supervision_is_applied_if_supervision_returns_default():
    yield

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


@deferred(timeout=None)
@inlineCallbacks
def test_bad_supervision_is_raised_if_supervision_returns_an_illegal_value():
    yield

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
    class MyActor(Actor):
        def pre_start(self):
            assert not self.children

            child1 = self.spawn(Actor)
            assert child1 in self.children

            child2 = self.spawn(Actor)
            assert child2 in self.children

    spawn(MyActor)


def test_stopped_child_is_removed_from_its_parents_list_of_children():
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


@deferred(timeout=0.01)
@inlineCallbacks
def test_suspending_suspends_and_resuming_resumes_all_children():
    # Actor.SENDING_IS_ASYNC = True  # so that we could use EvSeq

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
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

    class Child(Actor):
        def post_stop(self):
            child_stopped()

    child_stopped = Latch()

    p = spawn(Parent)
    p._stop_noevent()

    assert child_stopped


def test_restarting_stops_children():
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
        p._stop_noevent()


@deferred(timeout=0.01)
@inlineCallbacks
def test_queued_messages_are_logged_as_deadletters_after_stop():
    Actor.SENDING_IS_ASYNC = True

    deadletter_event_emitted = Events.consume_one(DeadLetter)

    a = spawn(Actor)

    a._stop_noevent()

    a << 'dummy'

    assert (yield deadletter_event_emitted) == DeadLetter(a, 'dummy')


def test_termination_message_after_restart_is_ignored():
    child = Slot()
    release_child = Trigger()

    class Child(Actor):
        def receive(self, _):
            yield release_child
            self.stop()

    class Parent(Actor):
        def pre_start(self):
            if not child():  # make sure after restart, the child won't exist
                child << self.spawn(Child)

    parent = spawn(Parent)

    child() << 'dummy'
    parent << '_restart'
    release_child()


##
## DEATH WATCH

def test_watch_returns_the_actor_that_was_watched():
    """Actor.watch returns the actor that was passed to it"""
    class Parent(Actor):
        def pre_start(self):
            a = self.watch(self.spawn(Actor))
            assert a

    spawn(Parent)


@deferred(timeout=0.01)
def test_watching_running_actor():
    # when spawning is async, the actor is immediately spawn and thus we are watching an already running actor
    return _do_test_watching_actor(async=False)


@deferred(timeout=0.01)
def test_watching_new_actor():
    # when spawning is synchronous, the actor has not yet been spawn at the time we start watching it
    return _do_test_watching_actor(async=True)


@inlineCallbacks
def _do_test_watching_actor(async=False):
    # We could just set this after we have already spawned the Watcher to avoid needing 2 monitoring Deferreds, but for
    # safety, we set it for the entire duration of the test.
    Actor.SPAWNING_IS_ASYNC = async

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

    watchee._stop_noevent()
    assert (yield message_receieved) == ('terminated', watchee)


def test_watching_self_is_noop_or_warning():
    """Watching self warns by default and does nothing if explicitly told it's safe"""
    self_ok = False

    class MyActor(Actor):
        def pre_start(self):
            self.watch(self.ref, self_ok=self_ok)

        def receive(self, message):
            assert False, message

    with assert_one_warning():
        spawn(MyActor)._stop_noevent()

    self_ok = True
    with assert_no_warnings():
        a = spawn(MyActor)

    dead_letter_emitted_d = Events.consume_one(DeadLetter)
    a._stop_noevent()
    assert not dead_letter_emitted_d.called, dead_letter_emitted_d.result


def test_watching_self_still_returns_the_ref():
    """Calling self.watch(self.ref, self_ok=True) returns self.ref for consistency"""
    class MyActor(Actor):
        def pre_start(self):
            ref = self.watch(self.ref, self_ok=True)
            assert ref == self.ref

    spawn(MyActor)


def test_termination_message_contains_ref_that_forwards_to_deadletters():
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            _, sender = message
            with assert_one_event(DeadLetter(sender, 'dummy')):
                sender << 'dummy'

    watchee = spawn(Actor)

    spawn(Watcher)
    watchee._stop_noevent()


def test_watching_dead_actor():
    message_receieved = Latch()

    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            message_receieved()

    watchee = spawn(Actor)
    watchee._stop_noevent()

    spawn(Watcher)

    assert message_receieved


def test_watching_dying_actor():
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
    watchee._stop_noevent()

    spawn(Watcher)

    release_watchee()

    assert message_receieved


def test_unhandled_termination_message_causes_receiver_to_raise_unhandledtermination():
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            raise Unhandled

    watchee = spawn(Actor)
    watchee._stop_noevent()

    with expect_failure(UnhandledTermination):
        spawn(Watcher)


def test_termination_message_to_dead_actor_is_discarded():
    class Parent(Actor):
        def pre_start(self):
            child = self.watch(self.spawn(Actor))
            child.stop()
            self.ref._stop_noevent()

    d = Events.consume_one(DeadLetter)
    spawn(Parent)
    assert not d.called


def test_system_messages_to_dead_actorrefs_are_discarded():
    a = spawn(Actor)
    a._stop_noevent()

    for event in ['_stop', '_suspend', '_resume', '_restart']:
        d = Events.consume_one(DeadLetter)
        a << event
        assert not d.called, "message %r sent to a dead actor should be discarded" % event
        d.addErrback(lambda f: f.trap(CancelledError)).cancel()


def test_termination_message_to_dead_actorref_is_discarded():
    release_child = Trigger()

    class Child(Actor):
        def post_stop(self):
            return release_child

    class Parent(Actor):
        def pre_start(self):
            self.watch(self.spawn(Child))
            self.ref._stop_noevent()

    d = Events.consume_one(DeadLetter)
    spawn(Parent)
    release_child()
    assert not d.called, d.result

    d.addErrback(lambda f: f.trap(CancelledError)).cancel()  # just to be nice


def test_TODO_watching_nonexistent_actor():
    pass


def test_TODO_watching_running_remote_actor():
    pass


def test_TODO_watching_new_remote_actor():
    pass


def test_TODO_watching_dead_remote_actor():
    pass


def test_TODO_watching_nonexistent_remote_actor():
    pass


##
## REMOTING

def test_TODO_serializing_actorref_converts_it_to_addr_and_registers_it_with_remoting():
    pass


def test_TODO_sending_to_seemingly_remote_refs_that_are_local_bypasses_remoting():
    # TODO: eager or lazy conversion
    pass


def test_TODO_sending_to_remote_actorref_delivers_the_message():
    pass


def test_TODO_remotely_spawned_actors_ref_is_registered_eagerly():
    pass


##
## PROCESSES

def test_process_run_must_return_a_generator():
    with assert_raises(TypeError):
        class MyProc(Process):
            def run(self):
                pass


def test_processes_run_is_called_when_the_process_is_spawned():
    run_called = Latch()

    class MyProc(Process):
        def run(self):
            run_called()
            yield

    spawn(MyProc)

    assert run_called


def test_process_run_is_a_coroutine():
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


def test_process_run_is_paused_and_unpaused_if_the_actor_is_suspended_and_resumed():
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
    exited = Latch()

    class MyProc(Process):
        def run(self):
            try:
                yield self.get()
            except GeneratorExit:
                exited()

    p = spawn(MyProc)
    p._stop_noevent()
    assert exited


def test_sending_to_a_process_injects_the_message_into_its_coroutine():
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
    class MyProc(Process):
        def run(self):
            raise MockException
            yield self.get()

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


@deferred(timeout=0.01)
@inlineCallbacks
def test_restarting_a_process_reinvokes_its_run_method():
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


@deferred(timeout=0.01)
@inlineCallbacks
def test_error_in_process_suspends_and_taints_and_resuming_it_warns_and_restarts_it():
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
    class MyProc(Process):
        def run(self):
            try:
                yield self.get()
            finally:
                # this also covers the process trying to yield stuff, e.g. for getting another message
                raise MockException

    with assert_one_event(ErrorIgnored(ANY, IS_INSTANCE(MockException), ANY)):
        spawn(MyProc)._stop_noevent()


def test_all_queued_messages_are_reported_as_unhandled_on_flush():
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


@deferred(timeout=0.01)
@inlineCallbacks
def test_process_is_stopped_when_the_coroutine_exits():
    class MyProc(Process):
        def run(self):
            yield self.get()

    p = spawn(MyProc)
    p << 'dummy'
    yield p.join()


@deferred(timeout=0.01)
@inlineCallbacks
def test_process_is_stopped_when_the_coroutine_exits_during_startup():
    class MyProc(Process):
        def run(self):
            yield

    p = spawn(MyProc)
    yield p.join()


def test_process_can_get_messages_selectively():
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


@deferred(timeout=0.01)
@inlineCallbacks
def test_process_can_delegate_handling_of_caught_exceptions_to_parent():
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
            # yield self.get()  # put the process into receive mode (i.e. started)
            try:
                raise MockException()
            except MockException:
                yield self.escalate()
            process_continued()

    spawn(Parent)

    yield supervision_invoked
    assert not process_continued


def test_optional_process_high_water_mark_emits_an_event_for_every_multiple_of_that_nr_of_msgs_in_the_queue():
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


def wrap_globals():
    """Ensures that errors in actors during tests don't go unnoticed."""

    def wrap(fn):
        @functools.wraps(fn)
        def ret():
            Guardian.reset()  # nosetests reuses the same interpreter state for better performance
            assert not Guardian._children, Guardian._children

            Actor.reset_flags(debug=True)

            Events.reset()

            with ErrorCollector():
                fn()
        return ret

    for name, value in globals().items():
        if name.startswith('test_') and callable(value):
            globals()[name] = wrap(value)
wrap_globals()
del wrap_globals
