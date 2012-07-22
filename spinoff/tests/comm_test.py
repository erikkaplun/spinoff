import pickle
import random

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks

from spinoff.actor.comm import Comm, ActorRef
from spinoff.util.testing import MockActor, assert_raises, Container, deferred_result
from spinoff.actor import Actor
from spinoff.util.async import sleep
from spinoff.util.testing import assert_not_raises, deref


def _get_actor_id(actorref):
    return actorref.addr.rsplit('/', 1)[1]


class CommTestCase(unittest.TestCase):

    def test_comm_init(self):
        Comm(addr='tcp://127.0.0.1:11000', sock=MockActor())
        Comm(addr='tcp://127.0.0.1:11001', sock=MockActor)

    def test_get_addr(self):
        comm_a = Comm(addr='tcp://127.0.0.1:11000', sock=MockActor)

        assert isinstance(comm_a._get_addr(object()), str)

        actor1 = object()
        actor2 = object()

        assert comm_a._get_addr(actor1) == comm_a._get_addr(actor1)
        assert comm_a._get_addr(actor2) == comm_a._get_addr(actor2)
        assert comm_a._get_addr(actor1) != comm_a._get_addr(actor2)

    def test_ref_addr(self):
        with Comm(addr='tcp://127.0.0.1:11000', sock=MockActor):
            actor1 = Actor()
            assert ActorRef(actor1).addr == ActorRef(actor1).addr
            assert ActorRef(Actor()).addr != ActorRef(Actor()).addr

    def test_ref_equality(self):
        with Comm(addr='tcp://127.0.0.1', sock=MockActor) as comm:
            assert ActorRef('foo') == ActorRef('foo')
            assert not (ActorRef('foo') != ActorRef('foo'))

            assert not (ActorRef('foo2') == ActorRef('foo'))
            assert ActorRef('foo2') != ActorRef('foo')

            actor1 = Actor()
            assert ActorRef(actor1) == ActorRef(comm._get_addr(actor1))

    def test_actorref_comm_interaction(self):
        for _ in range(3):
            with Comm(addr='tcp://127.0.0.1', sock=MockActor) as comm:
                assert isinstance(comm, Comm)

                ref = ActorRef('whatever')
                assert ref._get_comm() == comm

    def test_send_locally(self):
        with Container() as container:
            comm = container._spawn(Comm(addr='tcp://127.0.0.1', sock=MockActor))

            actor1 = container._spawn(MockActor)
            with comm:
                actor1_ref = ActorRef(actor1)
            actor1_ref.send('foo')
            assert deferred_result(actor1.wait()) == 'foo'

    def test_send_remotely(self):
        with Container() as container:
            mock_sock = MockActor()
            comm = container._spawn(Comm(addr='tcp://127.0.0.1', sock=mock_sock))

            random_port, random_actor_id = random.randint(8000, 10000), str(random.randint(0, 10000))
            with comm:
                remote_ref = ActorRef('tcp://whatever:%d/%s' % (random_port, random_actor_id))
            random_message = 'message-content-%s' % random.random()
            remote_ref.send(random_message)
            msg = deferred_result(mock_sock.wait())
            assert msg == ('tcp://whatever:%d' % random_port, pickle.dumps((random_actor_id, random_message)))

    def test_invalid_send_remotely(self):
        with Container() as container:
            mock_sock = MockActor()
            comm = container._spawn(Comm(addr='tcp://127.0.0.1', sock=mock_sock))

            with assert_raises(ValueError):
                comm.send_msg('whatever:8765/765123', 'whatev')
            with assert_raises(ValueError):
                comm.send_msg('tcp://whatever:8765', 'whatev')
            with assert_raises(ValueError):
                comm.send_msg('', 'whatev')

    def test_send_actorref_and_use_it_remotely(self):
        with Container() as container:
            mock_sock = MockActor()
            comm = container._spawn(Comm(addr='tcp://127.0.0.1:11000', sock=mock_sock))

            remote_mock_sock = MockActor()
            remote_comm = container._spawn(Comm(addr='tcp://127.0.0.1:11001', sock=remote_mock_sock))

            # serialize/send actorref locally
            actor1 = container._spawn(MockActor)
            with comm:
                actor1_ref = ActorRef(actor1)
            comm.send_msg('tcp://127.0.0.1:11001/1234', actor1_ref)
            outgoing_msg = deferred_result(mock_sock.wait())
            assert outgoing_msg == ('tcp://127.0.0.1:11001', pickle.dumps(('1234', actor1_ref)))

            # deserialize it remotely
            with remote_comm:
                msg = pickle.loads(outgoing_msg[1])
            actor1_ref2 = msg[1]
            assert actor1_ref2.addr == actor1_ref.addr

            # and use it to send a message back
            actor1_ref2.send('something')
            remote_outgoing_msg = deferred_result(remote_mock_sock.wait())
            assert remote_outgoing_msg == ('tcp://127.0.0.1:11000', pickle.dumps((_get_actor_id(actor1_ref), 'something')))

    def test_comm_receives_message(self):
        with Container() as container:
            mock_sock = MockActor()
            comm = container._spawn(Comm(addr='tcp://127.0.0.1', sock=mock_sock))

            actor1 = container._spawn(MockActor)
            with comm:
                actor_id = _get_actor_id(ActorRef(actor1))

            comm._send(pickle.dumps((actor_id, 'something')))

            assert 'something' == deferred_result(actor1.wait())

    @inlineCallbacks
    def test_send_receive_with_zmq(self):
        with Container() as container:
            comm1 = container._spawn(Comm(addr='tcp://127.0.0.1:11000'))
            comm2 = container._spawn(Comm(addr='tcp://127.0.0.1:11001'))

            actor1 = container._spawn(MockActor)
            actor2 = container._spawn(MockActor)

            with comm1:
                actor1_ref = ActorRef(actor1)
                actor1_addr = actor1_ref.addr
            with comm2:
                actor2_ref = ActorRef(actor2)
                yield comm2.ensure_connected('tcp://127.0.0.1:11000')
                comm2.send_msg(actor1_addr, actor2_ref)

            yield sleep(0.1)

            received_ref = deferred_result(actor1.wait())
            assert actor2_ref == received_ref

            with comm1:
                yield comm1.ensure_connected('tcp://127.0.0.1:11001')
                received_ref.send('something-else')

            yield sleep(0.005)
            assert 'something-else' == deferred_result(actor2.wait())

    def test_receive_actorref_to_a_local_actor(self):
        with Container() as container:
            assert container.is_running
            comm1 = container._spawn(Comm(addr='tcp://127.0.0.1:11000'))

            actor1 = container._spawn(MockActor)
            with comm1:
                actor1_ref = ActorRef(comm1._get_addr(actor1))

            actor1_ref.send('foobar')
            assert 'foobar' == deferred_result(actor1.wait())

    def test_optimize_refs_to_local_addrs(self):
        with Container() as container:
            comm = container._spawn(Comm(addr='tcp://127.0.0.1:11000'))
            actor1 = container._spawn(MockActor)

            with comm:
                ref = ActorRef(comm._get_addr(actor1))

            ref.send('foo')
            comm.send_msg = None  # the second send should not invoke comm.send_msg
            with assert_not_raises(TypeError):
                ref.send('foo')

    def test_manual_actor_id(self):
        with Container() as container:
            comm = container._spawn(Comm(addr='tcp://127.0.0.1:11000', sock=MockActor))
            actor1 = container.spawn(MockActor)

            comm.set_id(actor1, 'actor1')
            with comm:
                ref = ActorRef('tcp://127.0.0.1:11000/actor1')
            ref.send('foobar')
            assert 'foobar' == deferred_result(deref(actor1).wait())

            # can't set again
            with assert_raises(RuntimeError):
                comm.set_id(actor1, 'whatev')

            actor2 = container.spawn(MockActor)
            comm._get_addr(deref(actor2))
            # can't set again even if the previous one was implicitly set
            with assert_raises(RuntimeError):
                comm.set_id(actor2, 'whatev')

    @inlineCallbacks
    def test_manual_actor_id_with_zmq(self):
        with Container() as container:
            comm1 = container._spawn(Comm(addr='tcp://127.0.0.1:11000'))
            comm2 = container._spawn(Comm(addr='tcp://127.0.0.1:11001'))

            some_actor = container.spawn(MockActor)
            comm2.set_id(some_actor, 'some-actor')

            with comm1:
                ref = ActorRef('tcp://127.0.0.1:11001/some-actor')
                yield comm1.ensure_connected('tcp://127.0.0.1:11001')
                ref.send('foobar')

            yield sleep(0.005)
            assert 'foobar' == deferred_result(deref(some_actor).wait())
