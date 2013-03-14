from __future__ import print_function

import gc
import random
import re
import weakref

from gevent import idle, sleep, GreenletExit, with_timeout, Timeout
from gevent.event import Event, AsyncResult
from gevent.queue import Channel
from nose.tools import eq_, ok_

from spinoff.actor import Actor, Props, Node, Uri
from spinoff.actor.ref import Ref
from spinoff.actor.events import Events, UnhandledMessage, DeadLetter
from spinoff.actor.exceptions import Unhandled, NameConflict, UnhandledTermination
from spinoff.util.pattern_matching import ANY, IS_INSTANCE
from spinoff.util.testing import assert_raises, expect_one_warning, expect_one_event, expect_failure, MockActor, expect_event_not_emitted
from spinoff.util.testing.actor import wrap_globals
from spinoff.util.python import deferred_cleanup


def wait(fn):
    while True:
        if fn():
            break
        else:
            idle()


class Observable(object):
    terminator = None

    def __init__(self):
        self.done = Channel()

    def wait_eq(self, terminator, message=None, timeout=None):
        if self.value == terminator:
            return self.value
        self.terminator = terminator
        try:
            with_timeout(timeout, self.done.get)
            return self.value
        except Timeout:
            eq_(self.value, terminator, message)

    def _check(self):
        if self.value == self.terminator:
            self.done.put(None)

    def __eq__(self, x):
        return self.value == x

    def __repr__(self):
        return "%s(%r)" % (type(self).__name__, self.value)


class obs_list(list, Observable):
    def __init__(self):
        list.__init__(self)
        Observable.__init__(self)

    value = property(lambda self: self)

    def do(x):
        def fn(self, *args, **kwargs):
            ret = getattr(list, x)(self, *args, **kwargs)
            self._check()
            return ret
        fn.__name__ = x
        return fn

    for x in ['append', 'extend', 'insert', 'pop', 'remove', 'reverse', 'sort']:
        locals()[x] = do(x)


class obs_count(Observable):
    value = 0

    def incr(self):
        self.value += 1
        self._check()


# ATTENTION: all tests functions are smartly auto-wrapped with wrap_globals at the bottom of this file.


##
## GC

def test_node_gc():
    node1 = Node(nid='localhost:20001', enable_remoting=False)
    ref = weakref.ref(node1)
    node1.stop()
    node1 = None
    gc.collect()
    ok_(not ref())

    node2 = Node(nid='localhost:20002', enable_remoting=True)
    ref = weakref.ref(node2)
    node2.stop()
    node2 = None
    gc.collect()
    ok_(not ref())


##
## SENDING & RECEIVING

@deferred_cleanup
def test_sent_message_is_received(defer):
    # Trivial send and receive.
    messages = obs_list()
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Props(MockActor, messages))
    a << 'foo'
    messages.wait_eq(['foo'])


@deferred_cleanup
def test_sending_operator_is_chainable(defer):
    # Trivial send and receive using the << operator.
    messages = obs_list()
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Props(MockActor, messages))
    a << 'foo' << 'bar'
    messages.wait_eq(['foo', 'bar'])


@deferred_cleanup
def test_receive_of_the_same_actor_never_executes_concurrently(defer):
    # Receive of the same actor is never concurrent.
    #
    # It is guaranteed that an actor that is processing
    # a message (i.e. is not idle), will not receive the next message before processing the previous one completes.
    #
    # This test case rules out only the basic recursive case with a non-switching receive method.
    class MyActor(Actor):
        def receive(self, message):
            if message == 'init':
                receive_called.incr()
                self.ref << None
                eq_(receive_called.value, 1)
            else:
                receive_called.incr()

    node = DummyNode()
    defer(node.stop)
    receive_called = obs_count()
    node.spawn(MyActor) << 'init'
    receive_called.wait_eq(2)

    #

    class MyActor2(Actor):
        def receive(self, message):
            receive_called.incr()
            unblocked.wait()

    receive_called = obs_count()
    unblocked = Event()
    node = DummyNode()
    node.spawn(MyActor2) << None << None
    receive_called.wait_eq(1)
    unblocked.set()
    receive_called.wait_eq(2)


@deferred_cleanup
def test_unhandled_message_is_reported(defer):
    # Unhandled messages are reported to Events
    class MyActor(Actor):
        def receive(self, _):
            raise Unhandled
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(MyActor)
    with expect_one_event(UnhandledMessage(a, 'foo', sender=None)):
        a << 'foo'


@deferred_cleanup
def test_unhandled_message_to_guardian_is_also_reported(defer):
    node = DummyNode()
    defer(node.stop)
    guardian = node.guardian
    with expect_one_event(UnhandledMessage(guardian, 'foo', sender=None)):
        guardian << 'foo'


@deferred_cleanup
def test_with_no_receive_method_all_messages_are_unhandled(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    with expect_one_event(UnhandledMessage(a, 'dummy', sender=None)):
        a << 'dummy'


##
## SPAWNING

@deferred_cleanup
def test_spawning(defer):
    node = DummyNode()
    defer(node.stop)
    init_called, actor_spawned = Event(), Event()

    class MyActor(Actor):
        def __init__(self):
            init_called.set()

        def pre_start(self):
            actor_spawned.set()

    node.spawn(MyActor)
    ok_(not init_called.is_set())
    ok_(not actor_spawned.is_set())
    actor_spawned.wait()
    ok_(init_called.is_set())


@deferred_cleanup
def test_spawning_a_toplevel_actor_assigns_guardian_as_its_parent(defer):
    node = DummyNode()
    defer(node.stop)
    spawn = node.spawn
    pre_start_called = Event()

    class MyActor(Actor):
        def pre_start(self):
            ok_(self._parent is node.guardian)
            pre_start_called.set()

    spawn(MyActor)
    pre_start_called.wait()


@deferred_cleanup
def test_spawning_child_actor_assigns_the_spawner_as_parent(defer):
    node = DummyNode()
    defer(node.stop)
    childs_parent = AsyncResult()

    class MyActor(Actor):
        def receive(self, _):
            self.spawn(Child) << None

    class Child(Actor):
        def receive(self, _):
            childs_parent.set(self._parent)

    a = node.spawn(MyActor)
    a << None
    ok_(childs_parent.get() is a)


@deferred_cleanup
def test_spawning_returns_an_immediately_usable_ref(defer):
    node = DummyNode()
    defer(node.stop)
    other_receive_called = Event()

    class MyActor(Actor):
        def receive(self, _):
            node.spawn(Other) << 'foo'

    class Other(Actor):
        def receive(self, _):
            other_receive_called.set()

    node.spawn(MyActor) << None
    other_receive_called.wait(), "Spawned actor should have received a message"


@deferred_cleanup
def test_pre_start_is_called_after_constructor_and_ref_and_parent_are_available(defer):
    node = DummyNode()
    defer(node.stop)
    message_received = Event()

    class MyActor(Actor):
        def receive(self, message):
            if message == 'init':
                self.child = self.spawn(Child)
            else:
                ok_(message is self.child)
                message_received.set()

    class Child(Actor):
        def pre_start(self):
            self._parent << self.ref

    node.spawn(MyActor) << 'init'
    message_received.wait()


@deferred_cleanup
def test_errors_in_pre_start_are_reported(defer):
    class MyActor(Actor):
        def pre_start(self):
            raise MockException()
    node = DummyNode()
    defer(node.stop)
    with expect_failure(MockException):
        node.spawn(MyActor)


@deferred_cleanup
def test_sending_to_self_does_not_deliver_the_message_until_after_the_actor_is_started(defer):
    class MyActor(Actor):
        def pre_start(self):
            self.ref << 'dummy'
            ok_(not message_received.is_set())

        def receive(self, message):
            message_received.set()

    node = DummyNode()
    defer(node.stop)
    message_received = Event()
    node.spawn(MyActor)
    message_received.wait()


##
## LIFECYCLE

@deferred_cleanup
def test_stopping_waits_till_the_ongoing_receive_is_complete(defer):
    class MyActor(Actor):
        def receive(self, message):
            released.wait()

        def post_stop(self):
            stopped.set()

    node = DummyNode()
    defer(node.stop)
    stopped, released = Event(), Event()
    a = node.spawn(MyActor) << 'foo'
    sleep(.001)
    a.stop()
    sleep(.001)
    ok_(not stopped.is_set())
    released.set()
    stopped.wait()


@deferred_cleanup
def test_killing_does_not_wait_till_the_ongoing_receive_is_complete(defer):
    class MyActor(Actor):
        def receive(self, message):
            released.wait()

        def post_stop(self):
            stopped.set()

    node = DummyNode()
    defer(node.stop)
    stopped, released = Event(), Event()
    a = node.spawn(MyActor) << 'foo'
    sleep(.001)
    a.kill()
    stopped.wait()


@deferred_cleanup
def test_stopping_an_actor_prevents_it_from_processing_any_more_messages(defer):
    class MyActor(Actor):
        def receive(self, _):
            received.set()
    node = DummyNode()
    defer(node.stop)
    received = Event()
    a = node.spawn(MyActor)
    a << None
    received.wait()
    received.clear()
    a.stop()
    sleep(.001)
    ok_(not received.is_set(), "the '_stop' message should not be receivable in the actor")
    with expect_one_event(DeadLetter(a, None, sender=None)):
        a << None


@deferred_cleanup
def test_stopping_calls_post_stop(defer):
    class MyActor(Actor):
        def post_stop(self):
            post_stop_called.set()
    node = DummyNode()
    defer(node.stop)
    post_stop_called = Event()
    node.spawn(MyActor).stop()
    post_stop_called.wait()


@deferred_cleanup
def test_stopping_waits_for_post_stop(defer):
    class ChildWithSlowPostStop(Actor):
        def post_stop(self):
            stop_complete.wait()

    class Parent(Actor):
        def pre_start(self):
            self.child = self.watch(self.spawn(ChildWithSlowPostStop))

        def receive(self, message):
            if message == 'stop-child':
                self.child.stop()
            else:
                parent_received.set()

    node = DummyNode()
    defer(node.stop)
    stop_complete, parent_received = Event(), Event()
    node.spawn(Parent) << 'stop-child'
    sleep(.001)
    ok_(not parent_received.is_set())
    stop_complete.set()
    parent_received.wait()


@deferred_cleanup
def test_parent_is_stopped_before_children(defer):
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

        def post_stop(self):
            parent_stopping.set()
            unblocked.wait()

    class Child(Actor):
        def post_stop(self):
            child_stopped.set()

    node = DummyNode()
    defer(node.stop)
    parent_stopping, unblocked, child_stopped = Event(), Event(), Event()
    a = node.spawn(Parent)
    a.stop()
    parent_stopping.wait()
    ok_(not child_stopped.is_set())
    unblocked.set()
    child_stopped.wait()


@deferred_cleanup
def test_stopping_in_pre_start_directs_any_refs_to_deadletters(defer):
    class MyActor(Actor):
        def pre_start(self):
            self.stop()

        def receive(self, message):
            message_received.set()

    node = DummyNode()
    defer(node.stop)
    message_received = Event()
    a = node.spawn(MyActor)
    with expect_one_event(DeadLetter(a, 'dummy', sender=None)):
        a << 'dummy'
    ok_(not message_received.is_set())


@deferred_cleanup
def test_stop_message_received_twice_is_silently_ignored(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    a.stop()
    a.stop()
    sleep(.001)


##
## ACTORREFS, URIS & LOOKUP

@deferred_cleanup
def test_actors_are_garbage_collected_on_termination(defer):
    class MyActor(Actor):
        def __del__(self):
            del_called.set()

    node = DummyNode()
    defer(node.stop)
    del_called = Event()
    node.spawn(MyActor).stop()
    idle()
    gc.collect()
    ok_(del_called.is_set())


@deferred_cleanup
def test_cells_are_garbage_collected_on_termination(defer):
    node = DummyNode()
    defer(node.stop)
    ac = node.spawn(Actor)
    cell = weakref.ref(ac._cell)
    ok_(cell())
    ac.stop()
    idle()
    gc.collect()
    ok_(not cell())


@deferred_cleanup
def test_messages_to_dead_actors_are_sent_to_dead_letters(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    a.stop()
    with expect_one_event(DeadLetter(a, 'should-end-up-as-letter', sender=None)):
        a << 'should-end-up-as-letter'
        idle()


@deferred_cleanup
def test_guardians_path_is_the_root_uri(defer):
    node = DummyNode()
    defer(node.stop)
    eq_(node.guardian.uri.path, '')


@deferred_cleanup
def test_toplevel_actorrefs_paths_are_prefixed_with_guardians_path(defer):
    node = DummyNode()
    defer(node.stop)
    eq_(node.spawn(Actor, name='a').uri.path, '/a')
    eq_(node.spawn(Actor, name='b').uri.path, '/b')


@deferred_cleanup
def test_non_toplevel_actorrefs_are_prefixed_with_their_parents_path(defer):
    class MyActor(Actor):
        def pre_start(self):
            child_ref.set(self.spawn(Actor, name='child'))
    node = DummyNode()
    defer(node.stop)
    child_ref = AsyncResult()
    a = node.spawn(MyActor, name='parent')
    eq_(child_ref.get().uri.path, a.uri.path + '/child')


@deferred_cleanup
def test_toplevel_actor_paths_must_be_unique(defer):
    node = DummyNode()
    defer(node.stop)
    node.spawn(Actor, name='a')
    with assert_raises(NameConflict):
        node.spawn(Actor, name='a')


def test_non_toplevel_actor_paths_must_be_unique():
    class MyActor(Actor):
        def pre_start(self):
            self.spawn(Actor, name='a')
            with assert_raises(NameConflict):
                self.spawn(Actor, name='a')
            spawned.set()
    node = DummyNode()
    spawned = Event()
    node.spawn(MyActor, name='a')
    spawned.wait()


@deferred_cleanup
def test_spawning_toplevel_actors_without_name_assigns_autogenerated_names(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    ok_(re.match(r'/[^/]+$', a.uri.path))
    b = node.spawn(Actor)
    ok_(re.match(r'/[^/]+$', b.uri.path))
    ok_(b.uri.path != a.uri.path)


@deferred_cleanup
def test_spawning_non_toplevel_actors_without_name_assigns_autogenerated_names_with_prefixed_parent_path(defer):
    class MyActor(Actor):
        def pre_start(self):
            l = len(self.ref.uri.path)
            a = self.spawn(Actor)
            ok_(a.uri.path[l:])
            ok_(a.uri.path.startswith(self.ref.uri.path + '/'), a.uri.path)
            b = self.spawn(Actor)
            ok_(b.uri.path[l:])
            ok_(a.uri.path != b.uri.path)
            started.set()
    node = DummyNode()
    defer(node.stop)
    started = Event()
    node.spawn(MyActor)
    started.wait()


@deferred_cleanup
def test_spawning_with_autogenerated_looking_name_raises_an_exception(defer):
    node = DummyNode()
    defer(node.stop)
    with assert_raises(ValueError):
        node.spawn(Actor, name='$1')


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

@deferred_cleanup
def test_looking_up_an_actor_by_its_absolute_path_returns_the_original_reference_to_it(defer):
    node = DummyNode()
    defer(node.stop)
    toplevel_actor = node.spawn(Actor, name='toplevel')
    ok_(node.lookup_str('/toplevel') is toplevel_actor)
    ok_(node.lookup(Uri.parse('/toplevel')) is toplevel_actor)

    child_actor = toplevel_actor._cell.spawn_actor(Actor, name='child')
    ok_(node.lookup_str('/toplevel/child') is child_actor)
    ok_(node.lookup(Uri.parse('/toplevel/child')) is child_actor)


@deferred_cleanup
def test_looking_up_an_actor_by_a_relative_path_returns_the_original_reference_to_it(defer):
    node = DummyNode()
    defer(node.stop)
    toplevel_actor = node.spawn(Actor, name='toplevel')
    ok_(node.lookup_str('toplevel') is toplevel_actor)

    child_actor = toplevel_actor._cell.spawn_actor(Actor, name='child')
    ok_(node.lookup_str('toplevel/child') is child_actor)
    ok_(toplevel_actor / 'child' is child_actor)


@deferred_cleanup
def test_looking_up_an_actor_by_a_parent_traversing_relative_path_returns_a_reference_to_it(defer):
    node = DummyNode()
    defer(node.stop)

    a = node.spawn(Actor, name='a')
    ok_(node.guardian / 'a' is a)

    b = a._cell.spawn_actor(Actor, name='b')
    ok_(a / 'b' is b)
    ok_(node.guardian / 'a/b' is b)


@deferred_cleanup
def test_looking_up_an_absolute_path_as_if_it_were_relative_just_does_an_absolute_lookup(defer):
    node = DummyNode()
    defer(node.stop)

    a = node.spawn(Actor, name='a')
    a._cell.spawn_actor(Actor, name='b')
    root_b = node.spawn(Actor, name='b')

    eq_(a / '/b', root_b)


@deferred_cleanup
def test_looking_up_a_non_existent_local_actor_raises_runtime_error(defer):
    node = DummyNode()
    defer(node.stop)
    with assert_raises(RuntimeError):
        node.guardian / 'noexist'


# def test_looking_up_a_non_existent_local_actor_returns_a_dead_ref_with_nevertheless_correct_uri():
#     network = MockNetwork(Clock())
#     node = network.node('local:123')
#     eq_(node.guardian, node.lookup('local:123'))

#     noexist = node.lookup('local:123/a/b/c')
#     eq_(noexist.uri, 'local:123/a/b/c')
#     ok_(noexist.is_local)
#     with expect_one_event(DeadLetter(noexist, 'foo')):
#         noexist << 'foo'

#     hypotheticalchild = noexist / 'hypotheticalchild'
#     ok_(hypotheticalchild.is_local)
#     eq_(hypotheticalchild.uri, 'local:123/a/b/c/hypotheticalchild')
#     with expect_one_event(DeadLetter(hypotheticalchild, 'foo')):
#         hypotheticalchild << 'foo'


# def test_manually_ceated_remote_looking_ref_to_a_non_existend_local_actor_is_converted_to_dead_ref_on_send():
#     network = MockNetwork(Clock())
#     node = network.node('local:123')

#     noexist = Ref(cell=None, is_local=False, uri=Uri.parse('local:123/a/b/c'), hub=node.hub)

#     with expect_one_event(DeadLetter(noexist, 'foo')):
#         noexist << 'foo'
#     ok_(noexist.is_local)


@deferred_cleanup
def test_error_in_post_stop_reports_error_and_termination_messages_are_sent_as_normal(defer):
    class Child(Actor):
        def post_stop(self):
            raise MockException

    class Parent(Actor):
        def pre_start(self):
            self.child = self.spawn(Child)
            self.watch(self.child)
            self.child.stop()

        def receive(self, message):
            eq_(message, ('terminated', self.child))
            termination_message_received.set()

    node = DummyNode()
    defer(node.stop)
    termination_message_received = Event()
    with expect_failure(MockException):
        node.spawn(Parent)
    termination_message_received.wait()


##
## HIERARCHY

@deferred_cleanup
def test_actors_remember_their_children(defer):
    class MyActor(Actor):
        def pre_start(self):
            ok_(not self.children)
            child1 = self.spawn(Actor)
            ok_(child1 in self.children)
            child2 = self.spawn(Actor)
            ok_(child2 in self.children)
    node = DummyNode()
    defer(node.stop)
    node.spawn(MyActor)


@deferred_cleanup
def test_stopped_child_is_removed_from_its_parents_list_of_children(defer):
    class MyActor(Actor):
        def pre_start(self):
            child = self.spawn(Actor)
            ok_(child in self.children)
            self.watch(child)
            child.stop()
            # XXX: assert child in self._children

        def receive(self, message):
            receive_called.set()
            eq_(message, ('terminated', ANY))
            _, child = message
            ok_(child not in self.children)

    node = DummyNode()
    defer(node.stop)
    receive_called = Event()
    node.spawn(MyActor)
    receive_called.wait()


@deferred_cleanup
def test_stopping_stops_children(defer):
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

    class Child(Actor):
        def post_stop(self):
            child_stopped.set()

    node = DummyNode()
    defer(node.stop)
    child_stopped = Event()
    node.spawn(Parent).stop()
    child_stopped.wait()


@deferred_cleanup
def test_stopping_parent_from_child(defer):
    class PoorParent(Actor):
        def pre_start(self):
            child.set(self.spawn(EvilChild))

        def post_stop(self):
            parent_stopped.set()

    class EvilChild(Actor):
        def pre_start(self):
            self._parent.stop()  # this will queue a `_stop` in the parent's queue

    node = DummyNode()
    defer(node.stop)
    child = AsyncResult()
    parent_stopped = Event()
    node.spawn(PoorParent)
    child.get() << 'dummy'
    parent_stopped.wait()


@deferred_cleanup
def test_sending_message_to_stopping_parent_from_post_stop_should_deadletter_the_message(defer):
    class Parent(Actor):
        def pre_start(self):
            self.spawn(Child)

        def receive(self, message):
            ok_(False)

    class Child(Actor):
        def post_stop(self):
            self._parent << 'should-not-be-received'

    node = DummyNode()
    defer(node.stop)
    p = node.spawn(Parent)
    with expect_one_event(DeadLetter(ANY, ANY, sender=ANY)):
        p.stop()
        idle()


@deferred_cleanup
def test_queued_messages_are_logged_as_deadletters_after_stop(defer):
    node = DummyNode()
    defer(node.stop)
    deadletter_event_emitted = Events.consume_one(DeadLetter)
    a = node.spawn(Actor)
    a.stop()
    a << 'dummy'
    eq_(deadletter_event_emitted.get(), DeadLetter(a, 'dummy', sender=None))


@deferred_cleanup
def test_child_termination_message_from_an_actor_not_a_child_of_the_recipient_is_ignored(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    a << ('_child_terminated', node.spawn(Actor))
    idle()


##
## DEATH WATCH

@deferred_cleanup
def test_watch_returns_the_arg(defer):
    class Parent(Actor):
        def pre_start(self):
            a = self.spawn(Actor)
            ok_(self.watch(a) is a)
    node = DummyNode()
    defer(node.stop)
    node.spawn(Parent)


@deferred_cleanup
def test_watching_running_actor(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            message_receieved.set(message)

    node = DummyNode()
    defer(node.stop)
    message_receieved = AsyncResult()
    watchee = node.spawn(Actor)
    node.spawn(Watcher)
    watchee.stop()
    eq_(message_receieved.get(), ('terminated', watchee))


@deferred_cleanup
def test_watching_new_actor(defer):
    class Watcher(Actor):
        def pre_start(self):
            a = self.spawn(Actor)
            a.stop()
            watchee.set(a)
            self.watch(a)

        def receive(self, message):
            message_receieved.set(message)

    node = DummyNode()
    defer(node.stop)
    watchee, message_receieved = AsyncResult(), AsyncResult()
    node.spawn(Watcher)
    eq_(message_receieved.get(), ('terminated', watchee.get()))


@deferred_cleanup
def test_watching_dead_actor(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            message_receieved.set(message)

    node = DummyNode()
    defer(node.stop)
    message_receieved = AsyncResult()
    watchee = node.spawn(Actor)
    watchee.stop()
    idle()
    node.spawn(Watcher)
    eq_(message_receieved.get(), ('terminated', watchee))


@deferred_cleanup
def test_watching_self_is_noop_and_returns_self(defer):
    class MyActor(Actor):
        def pre_start(self):
            eq_(self.watch(self.ref), self.ref)

        def receive(self, message):
            ok_(False)

    node = DummyNode()
    defer(node.stop)
    a = node.spawn(MyActor)
    dead_letter_emitted = Events.consume_one(DeadLetter)
    a.stop()
    idle()
    ok_(not dead_letter_emitted.ready())


@deferred_cleanup
def test_termination_message_contains_ref_that_forwards_to_deadletters(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            eq_(message, ('terminated', watchee))
            _, sender = message
            with expect_one_event(DeadLetter(sender, 'dummy', sender=self.ref)):
                sender << 'dummy'
                idle()
            all_ok.set()

    all_ok = Event()
    node = DummyNode()
    defer(node.stop)
    watchee = node.spawn(Actor)
    node.spawn(Watcher)
    watchee.stop()
    all_ok.wait()


@deferred_cleanup
def test_watching_dying_actor(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            eq_(message, ('terminated', ANY))
            message_receieved.set()

    class Watchee(Actor):
        def post_stop(self):
            watchee_released.wait()

    node = DummyNode()
    defer(node.stop)
    watchee_released, message_receieved = Event(), Event()

    watchee = node.spawn(Watchee)
    watchee.stop()
    node.spawn(Watcher)
    watchee_released.set()
    message_receieved.wait()


@deferred_cleanup
def test_unhandled_termination_message_causes_receiver_to_raise_unhandledtermination(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, message):
            raise Unhandled

    node = DummyNode()
    defer(node.stop)
    watchee = node.spawn(Actor)
    watchee.stop()
    with expect_failure(UnhandledTermination):
        node.spawn(Watcher)


@deferred_cleanup
def test_stop_and_kill_messages_to_dead_actorrefs_are_discarded(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    a.stop()
    for event in ['_stop', '_kill']:
        d = Events.consume_one(DeadLetter)
        a << event
        ok_(not d.ready(), "message %r sent to a dead actor should be discarded" % (event,))


@deferred_cleanup
def test_termination_message_to_dead_actor_is_discarded(defer):
    class Parent(Actor):
        def pre_start(self):
            self.watch(self.spawn(Actor)).stop()
            self.stop()
    d = Events.consume_one(DeadLetter)
    node = DummyNode()
    defer(node.stop)
    node.spawn(Parent)
    idle()
    ok_(not d.ready())


@deferred_cleanup
def test_termination_message_is_ignored_when_sender_is_not_watched(defer):
    node = DummyNode()
    defer(node.stop)

    received = AsyncResult()

    class Watcher(Actor):
        def receive(self, msg):
            received.set(msg)

    dummy = node.spawn(Actor)
    w = node.spawn(Watcher)
    w << ('terminated', dummy)
    sleep(.01)
    ok_(not received.ready())


@deferred_cleanup
def test_watching_running_remote_actor_that_stops_causes_termination_message(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(self.root.node.lookup_str('localhost:20002/remote-watchee'))

        def receive(self, msg):
            received.set(msg)

    received = AsyncResult()
    node1, node2 = Node('localhost:20001', enable_remoting=True), Node('localhost:20002', enable_remoting=True)
    defer(node1.stop, node2.stop)

    remote_watchee = node2.spawn(Actor, name='remote-watchee')
    node1.spawn(Watcher)
    remote_watchee.stop()

    eq_(received.get(), ('terminated', remote_watchee))


@deferred_cleanup
def test_watching_nonexistent_remote_actor_causes_termination_message(defer):
    class Watcher(Actor):
        def pre_start(self):
            self.watch(watchee)

        def receive(self, msg):
            received.set(msg)

    received = AsyncResult()
    node1, node2 = (Node('localhost:20001', enable_remoting=True),
                    Node('localhost:20002', enable_remoting=True))
    defer(node1.stop, node2.stop)

    watchee = node1.lookup_str('localhost:20002/nonexistent-watchee')
    node1.spawn(Watcher)

    eq_(received.get(), ('terminated', watchee))


# @simtime
# def test_watching_an_actor_on_a_node_with_whom_connectivity_is_lost_or_limited(clock):
#     def test_it(packet_loss_src, packet_loss_dst):
#         network = MockNetwork(clock)
#         node1, node2 = network.node('watcher-host:123'), network.node('watchee-host:123')

#         received = Event()

#         class Watcher(Actor):
#             def pre_start(self):
#                 self.watchee = self.watch(self.root.node.lookup('watchee-host:123/remote-watchee'))

#             def receive(self, msg):
#                 eq_(msg, ('terminated', self.watchee))
#                 received()
#         node1.spawn(Watcher)

#         node2.spawn(Actor, name='remote-watchee')

#         network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE / 2.0)
#         ok_(not received)

#         network.packet_loss(100.0,
#                             src='tcp://' + packet_loss_src + '-host:123',
#                             dst='tcp://' + packet_loss_dst + '-host:123')
#         network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)
#         ok_(received)

#     test_it(packet_loss_src='watchee', packet_loss_dst='watcher')
#     test_it(packet_loss_src='watcher', packet_loss_dst='watchee')


##
## REMOTING

@deferred_cleanup
def test_actorref_remote_returns_a_ref_that_when_sent_a_message_delivers_it_on_another_node(defer):
    # This just tests the routing logic and not heartbeat or reliability or deadletters or anything.

    # emulate a scenario in which a single node sends many messages to other nodes;
    # a total of NUM_NODES * NUM_ACTORS messages will be sent out.
    NUM_NODES = 1
    NUM_ACTORS_PER_NODE = 2

    sender_node = Node(nid='localhost:20000', enable_remoting=True)
    defer(sender_node.stop)

    recipient_nodes = []

    for node_ix in range(NUM_NODES):
        nid = 'localhost:2000%d' % (node_ix + 1,)
        remote_node = Node(nid=nid, enable_remoting=True)
        defer(remote_node.stop)

        receive_boxes = []
        sent_msgs = []

        for actor_num in range(1, NUM_ACTORS_PER_NODE + 1):
            actor_box = obs_list()  # collects whatever the MockActor receives
            actor = remote_node.spawn(Props(MockActor, actor_box), name='actor%d' % actor_num)
            # we only care about the messages received, not the ref itself
            receive_boxes.append(actor_box)

            # format: dummy-<nodename>-<actorname>-<random-stuff-for-good-measure> (just for debuggability)
            msg = 'dummy-%s-%s-%s' % (nid, actor.uri.name, random.randint(1, 10000000))
            sender_node.lookup(actor.uri) << msg
            sent_msgs.append(msg)

        recipient_nodes.append((sent_msgs, receive_boxes))

    for sent_msgs, receive_boxes in recipient_nodes:
        for sent_msg, receive_box in zip(sent_msgs, receive_boxes):
            receive_box.wait_eq(terminator=[sent_msg], timeout=None)


def test_transmitting_refs_and_sending_to_received_refs():
    # This just tests the Ref serialisation and deserialization logic.

    @deferred_cleanup
    def test_it(defer, make_actor1):
        node1 = Node(nid='localhost:20001', enable_remoting=True)
        defer(node1.stop)

        actor1_msgs = obs_list()
        actor1 = make_actor1(node1, Props(MockActor, actor1_msgs))

        #
        node2 = Node(nid='localhost:20002', enable_remoting=True)
        defer(node2.stop)

        actor2_msgs = obs_list()
        node2.spawn(Props(MockActor, actor2_msgs), name='actor2')

        # send: node1 -> node2:
        node1.lookup_str('localhost:20002/actor2') << ('msg-with-ref', actor1)

        # reply: node2 -> node1:
        actor2_msgs.wait_eq([ANY], "should be able to send messages to explicitly constructed remote refs")
        _, received_ref = actor2_msgs[0]
        received_ref << ('hello', received_ref)

        actor1_msgs.wait_eq([('hello', received_ref)], "should be able to send messages to received remote refs")

        # send to self without knowing it
        (_, re_received_ref), = actor1_msgs
        del actor1_msgs[:]
        re_received_ref << 'to-myself'

        actor1_msgs.wait_eq(['to-myself'])

    @test_it
    def make_toplevel(node, factory):
        return node.spawn(factory, name='actor1')

    @test_it
    def make_non_toplevel(node, factory):
        class Parent(Actor):
            def pre_start(self):
                child.set(self.spawn(factory, name='actor1'))

        child = AsyncResult()
        node.spawn(Parent, name='parent')
        return child.get()
test_transmitting_refs_and_sending_to_received_refs.timeout = 10


@deferred_cleanup
def test_sending_remote_refs(defer):
    """Sending remote refs.

    The sender acquires a remote ref to an actor on the target and sends it to the sender, who then sends a message.
    to the target. It forms a triangle where 1) M obtains a reference to T, 2) sends it over to S, and then 3) S uses it
    to start communication with T.

    T ---- S
     \   /
      \ /
       M

    """
    target_node = Node('localhost:20003', enable_remoting=True)
    defer(target_node.stop)

    target_msgs = obs_list()
    target_node.spawn(Props(MockActor, target_msgs), name='T')

    #
    sender_node = Node('localhost:20001', enable_remoting=True)
    defer(sender_node.stop)

    class SenderActor(Actor):
        def receive(self, msg):
            eq_(msg, ('send-msg-to', ANY))
            _, target = msg
            target << 'helo'
    sender_node.spawn(SenderActor, name='S')

    #
    middle_node = Node('localhost:20002', enable_remoting=True)
    defer(middle_node.stop)

    ref_to_sender = middle_node.lookup_str('localhost:20001/S')
    ref_to_target = middle_node.lookup_str('localhost:20003/T')
    ref_to_sender << ('send-msg-to', ref_to_target)

    target_msgs.wait_eq(['helo'])


@deferred_cleanup
def test_messages_sent_to_nonexistent_remote_actors_are_deadlettered(defer):
    sender_node, receiver_node = (Node('localhost:20001', enable_remoting=True),
                                  Node('localhost:20002', enable_remoting=True))
    defer(sender_node.stop, receiver_node.stop)

    noexist = sender_node.lookup_str('localhost:20002/non-existent-actor')
    with expect_one_event(DeadLetter):
        noexist << 'straight-down-the-drain'
test_messages_sent_to_nonexistent_remote_actors_are_deadlettered.timeout = 3.0


## HEARTBEAT

@deferred_cleanup
def test_sending_to_an_unknown_node_doesnt_start_if_the_node_doesnt_become_visible_and_the_message_is_later_dropped(defer):
    sender_node = Node('localhost:20001', enable_remoting=True, hub_kwargs={'heartbeat_interval': 0.05, 'heartbeat_max_silence': 0.1})
    defer(sender_node.stop)
    ref = sender_node.lookup_str('localhost:23456/actor2')
    with expect_one_event(DeadLetter(ref, 'bar', sender=None)):
        ref << 'bar'
test_sending_to_an_unknown_node_doesnt_start_if_the_node_doesnt_become_visible_and_the_message_is_later_dropped.timeout = 40.0


@deferred_cleanup
def test_sending_to_an_unknown_host_that_becomes_visible_in_time(defer):
    node1 = Node('localhost:20001', enable_remoting=True, hub_kwargs={'heartbeat_interval': 0.05, 'heartbeat_max_silence': 0.5})
    defer(node1.stop)

    ref = node1.lookup_str('localhost:20002/actor1')
    with expect_event_not_emitted(DeadLetter):
        ref << 'foo'

    sleep(0.1)

    node2 = Node('localhost:20002', enable_remoting=True)
    defer(node2.stop)

    actor2_msgs = obs_list()
    node2.spawn(Props(MockActor, actor2_msgs), name='actor1')

    actor2_msgs.wait_eq(['foo'])


# @deferred_cleanup
# def test_sending_stops_if_visibility_is_lost(defer):
#     node1 = Node('host1:123')
#     node2 = Node('host2:123')
#     defer(node1.stop, node2.stop)

#     # set up a normal sending state first

#     ref = node1.lookup_str('host2:123/actor2')
#     ref << 'foo'  # causes host2 to also start the heartbeat

#     sleep(1.0)

#     # ok, now they both know/have seen each other; let's change that:
#     network.packet_loss(percent=100.0, src='tcp://host2:123', dst='tcp://host1:123')
#     network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)

#     # the next message should fail after 5 seconds
#     ref << 'bar'

#     with expect_one_event(DeadLetter(ref, 'bar')):
#         network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)


# @simtime
# def test_sending_resumes_if_visibility_is_restored(clock):
#     network = MockNetwork(clock)

#     node1 = network.node('host1:123')

#     network.node('host2:123')

#     ref = node1.lookup('host2:123/actor2')
#     ref << 'foo'
#     network.simulate(duration=1.0)
#     # now both visible to the other

#     network.packet_loss(percent=100.0, src='tcp://host2:123', dst='tcp://host1:123')
#     network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE + 1.0)
#     # now host2 is not visible to host1

#     ref << 'bar'
#     network.simulate(duration=node1.hub.HEARTBEAT_MAX_SILENCE - 1.0)
#     # host1 still hasn't lost faith

#     network.packet_loss(percent=0, src='tcp://host2:123', dst='tcp://host1:123')
#     # and it is rewarded for its patience
#     with expect_event_not_emitted(DeadLetter):
#         network.simulate(duration=2.0)


## OPTIMIZATIONS

@deferred_cleanup
def test_incoming_refs_pointing_to_local_actors_are_converted_to_local_refs(defer):
    # node1:
    node1 = Node('localhost:20001', enable_remoting=True)
    defer(node1.stop)

    actor1_msgs = obs_list()
    actor1 = node1.spawn(Props(MockActor, actor1_msgs), name='actor1')

    # node2:
    node2 = Node('localhost:20002', enable_remoting=True)
    defer(node2.stop)

    actor2_msgs = obs_list()
    node2.spawn(Props(MockActor, actor2_msgs), name='actor2')

    # send from node1 -> node2:
    node1.lookup_str('localhost:20002/actor2') << ('msg-with-ref', actor1)

    # reply from node2 -> node1:
    _, received_ref = actor2_msgs.wait_eq([ANY])[0]
    received_ref << ('msg-with-ref', received_ref)

    (_, remote_local_ref), = actor1_msgs.wait_eq([ANY])
    ok_(remote_local_ref.is_local)


@deferred_cleanup
def test_looking_up_addresses_that_actually_point_to_the_local_node_return_a_local_ref(defer):
    node = Node('localhost:20000', enable_remoting=True)
    defer(node.stop)
    node.spawn(Actor, name='localactor')
    ref = node.lookup_str('localhost:20000/localactor')
    ok_(ref.is_local)


@deferred_cleanup
def test_sending_to_a_remote_ref_that_points_to_a_local_ref_is_redirected(defer):
    node = Node('localhost:20000', enable_remoting=True)
    defer(node.stop)

    msgs = obs_list()
    node.spawn(Props(MockActor, msgs), name='localactor')

    ref = Ref(cell=None, uri=Uri.parse('localhost:20000/localactor'), is_local=False, node=node)
    ref << 'foo'

    msgs.wait_eq(['foo'])
    ok_(ref.is_local)

    ref << 'bar'
    msgs.wait_eq(['foo', 'bar'])


##
## PROCESSES

@deferred_cleanup
def test_processes_run_is_called_when_the_process_is_spawned(defer):
    class MyProc(Actor):
        def run(self):
            run_called.set()
    node = DummyNode()
    defer(node.stop)
    run_called = Event()
    node.spawn(MyProc)
    run_called.wait()


@deferred_cleanup
def test_warning_is_emitted_if_process_run_returns_a_value(defer):
    class ProcThatReturnsValue(Actor):
        def run(self):
            return 'foo'
    node = DummyNode()
    defer(node.stop)
    with expect_one_warning():
        node.spawn(ProcThatReturnsValue)


@deferred_cleanup
def test_greenletexit_is_raised_in_run_if_the_actor_is_stopped(defer):
    class MyProc(Actor):
        def run(self):
            try:
                self.get()
            except GreenletExit:
                exited.set()
    node = DummyNode()
    defer(node.stop)
    exited = Event()
    r = node.spawn(MyProc)
    idle()
    r.stop()
    exited.wait()


@deferred_cleanup
def test_stopping_waits_till_process_is_done_handling_a_message(defer):
    class MyProc(Actor):
        def run(self):
            self.get()
            try:
                released.wait()
                self.get()
            except GreenletExit:
                exited.set()
    node = DummyNode()
    defer(node.stop)
    exited, released = Event(), Event()
    r = node.spawn(MyProc)
    r << 'foo'
    sleep(.001)
    r.stop()
    sleep(.001)
    ok_(not exited.is_set())
    released.set()
    exited.wait()


@deferred_cleanup
def test_error_in_stopping_proc_is_reported_nevertheless(defer):
    class MyProc(Actor):
        def run(self):
            self.get()
            released.wait()
            raise MockException
    released = Event()
    node = node = DummyNode()
    defer(node.stop)
    a = node.spawn(MyProc)
    a << 'dummy'
    sleep(.001)
    a.stop()
    with expect_failure(MockException):
        released.set()


@deferred_cleanup
def test_sending_a_message_to_a_process(defer):
    class MyProc(Actor):
        def run(self):
            received_message.set(self.get())
    node = DummyNode()
    defer(node.stop)
    random_message = random.random()
    received_message = AsyncResult()
    node.spawn(MyProc) << random_message
    eq_(received_message.get(), random_message)


@deferred_cleanup
def test_sending_2_messages_to_a_process(defer):
    class MyProc(Actor):
        def run(self):
            first_message.set(self.get())
            second_message.set(self.get())

    node = DummyNode()
    defer(node.stop)
    second_message, first_message = AsyncResult(), AsyncResult()
    msg1, msg2 = random.random(), random.random()
    p = node.spawn(MyProc)
    p << msg1
    eq_(first_message.get(), msg1)
    p << msg2
    eq_(second_message.get(), msg2)


@deferred_cleanup
def test_errors_in_process_while_processing_a_message_are_reported(defer):
    class MyProc(Actor):
        def run(self):
            self.get()
            raise MockException
    node = DummyNode()
    defer(node.stop)
    p = node.spawn(MyProc)
    with expect_failure(MockException):
        p << 'dummy'


@deferred_cleanup
def test_errors_while_stopping_are_reported(defer):
    class MyProc(Actor):
        def run(self):
            try:
                self.get()
            finally:
                raise MockException
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(MyProc)
    sleep(.001)
    with expect_failure(MockException):
        a.stop()


@deferred_cleanup
def test_all_stashed_messages_are_reported_as_unhandled_on_flush_and_discarded(defer):
    class MyProc(Actor):
        def run(self):
            self.get('dummy')
            self.flush()
            self.get('dummy')
            self.flush()
    node = DummyNode()
    defer(node.stop)
    p = node.spawn(MyProc)
    p << 'should-be-reported-as-unhandled'
    with expect_event_not_emitted(DeadLetter(p, 'should-be-reported-as-unhandled', sender=None)):
        with expect_one_event(UnhandledMessage(p, 'should-be-reported-as-unhandled', sender=None)):
            p << 'dummy'
    with expect_event_not_emitted(DeadLetter(p, 'should-be-reported-as-unhandled', sender=None)):
        with expect_event_not_emitted(UnhandledMessage(p, 'should-be-reported-as-unhandled', sender=None)):
            p << 'dummy'


@deferred_cleanup
def test_getting_stashed_message(defer):
    class MyProc(Actor):
        def run(self):
            self.get('dummy1')
            self.get('dummy2')
            got_both_messages.set()
            self.get('dummy2')
            ok_(False, "should not reach here")
    node = DummyNode()
    defer(node.stop)
    got_both_messages = Event()
    p = node.spawn(MyProc)
    p << 'dummy2'
    p << 'dummy1'
    got_both_messages.wait()


@deferred_cleanup
def test_dead_letters_are_emitted_in_the_order_the_messages_were_sent(defer):
    node = DummyNode()
    defer(node.stop)
    a = node.spawn(Actor)
    with expect_one_event(DeadLetter(a, 'dummy1', sender=None)):
        with expect_one_event(DeadLetter(a, 'dummy2', sender=None)):
            a << 'dummy1' << 'dummy2'
            a.stop()


@deferred_cleanup
def test_stashed_messages_are_received_in_the_order_they_were_sent(defer):
    class MyProc(Actor):
        def run(self):
            msgs_received.append(self.get(1))
            msgs_received.append(self.get())
            msgs_received.append(self.get())
            done.set()

    node = DummyNode()
    defer(node.stop)
    msgs_received = []
    done = Event()
    a = node.spawn(MyProc)
    a << 3 << 2 << 1
    done.wait()
    eq_(msgs_received, [1, 3, 2])


@deferred_cleanup
def test_process_is_stopped_when_run_returns(defer):
    class MyProc(Actor):
        def run(self):
            self.get()

        def post_stop(self):
            stopped.set()

    node = DummyNode()
    defer(node.stop)
    stopped = Event()
    p = node.spawn(MyProc)
    p << 'dummy'
    stopped.wait()


@deferred_cleanup
def test_process_is_stopped_when_the_coroutine_exits_during_startup(defer):
    class MyProc(Actor):
        def run(self):
            pass

        def post_stop(self):
            stopped.set()

    node = DummyNode()
    defer(node.stop)
    stopped = Event()
    node.spawn(MyProc)
    stopped.wait()


@deferred_cleanup
def test_process_can_get_messages_selectively(defer):
    class MyProc(Actor):
        def run(self):
            messages.append(self.get(ANY))
            messages.append(self.get('msg3'))
            messages.append(self.get(ANY))
            release1.wait()
            messages.append(self.get(IS_INSTANCE(int)))
            release2.wait()
            messages.append(self.get(IS_INSTANCE(float)))
    node = DummyNode()
    defer(node.stop)
    messages = obs_list()
    release1, release2 = Event(), Event()
    p = node.spawn(MyProc)
    p << 'msg1'
    messages.wait_eq(['msg1'])
    p << 'msg2'
    messages.wait_eq(['msg1'])
    p << 'msg3'
    messages.wait_eq(['msg1', 'msg3', 'msg2'])
    # (process blocked here)
    p << 'not-an-int'
    release1.set()
    sleep(.001)
    ok_('not-an-int' not in messages)
    p << 123
    sleep(.001)
    ok_(123 in messages)
    # (process blocked here)
    p << 321
    p << 32.1
    release2.set()
    sleep(.001)
    ok_(321 not in messages)
    ok_(32.1 in messages)


# SUPPORT

class MockException(Exception):
    pass


def DummyNode():
    return Node()


wrap_globals(globals())
