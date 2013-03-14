import os
import random
import tempfile

from gevent import sleep
from gevent.event import Event, AsyncResult
from gevent.queue import Queue
from nose.tools import eq_, ok_

from spinoff.actor import Actor
from spinoff.actor.node import Node
from spinoff.contrib.filetransfer import serve_file, TransferInterrupted
from spinoff.util.python import deferred_cleanup
from spinoff.util.logging import dbg
from spinoff.util.testing.actor import wrap_globals


@deferred_cleanup
def test_with_no_remoting(defer):
    random_data = str(random.randint(0, 10000000000))

    node1 = Node('localhost:20001', enable_remoting=False)
    defer(node1.stop)

    f_src = tempfile.NamedTemporaryFile()
    f_src.write(random_data)
    f_src.flush()

    class Sender(Actor):
        def run(self, receiver):
            ref = serve_file(f_src.name)
            receiver << ref

    class Receiver(Actor):
        def receive(self, fref):
            for _ in range(2):
                fetched_path = fref.fetch()
                with open(fetched_path) as f_dst:
                    eq_(random_data, f_dst.read())
            received.set()

    received = Event()
    node1.spawn(Sender.using(receiver=node1.spawn(Receiver)))
    received.wait()


@deferred_cleanup
def test_with_remoting(defer):
    random_data = str(random.randint(0, 10000000000))

    node1 = Node('localhost:20001', enable_remoting=True)
    defer(node1.stop)
    node2 = Node('localhost:20002', enable_remoting=True)
    defer(node2.stop)

    f_src = tempfile.NamedTemporaryFile()
    f_src.write(random_data)
    f_src.flush()

    class Sender(Actor):
        def run(self, receiver):
            ref = serve_file(f_src.name)
            receiver << ref

    class Receiver(Actor):
        def receive(self, fref):
            for _ in range(2):
                fetched_path = fref.fetch()
                with open(fetched_path) as f_dst:
                    eq_(random_data, f_dst.read())
            received.set()

    received = Event()
    node2.spawn(Receiver, name='receiver')
    receiver_ref = node1.lookup_str('localhost:20002/receiver')
    node1.spawn(Sender.using(receiver=receiver_ref))
    received.wait()
test_with_remoting.timeout = 10.0


if hasattr(os, 'mkfifo'):
    @deferred_cleanup
    def test_interrupted_transfer(defer):
        return  # XXX: nope, this scenario does not seem to be possible to conduct using pipes, instead, large files
                # might prove to be the only solution
        random_data = str(random.randint(0, 10000000000))

        node1 = Node('localhost:20001', enable_remoting=True)
        defer(node1.stop)
        node2 = Node('localhost:20002', enable_remoting=True)
        defer(node2.stop)

        fd, tmpname = tempfile.mkstemp()
        os.close(fd)
        with open(tmpname, 'wb') as f:
            f.write(b'_' * len(random_data))

        class Sender(Actor):
            def run(self, receiver):
                ref = serve_file(tmpname)
                served.set(ref)
                receiver << ref

        class Receiver(Actor):
            def receive(self, fref):
                try:
                    fref.fetch()
                except TransferInterrupted as e:
                    dbg(e)
                    failed.set()

        served = AsyncResult()
        failed = Event()

        node2.spawn(Receiver, name='receiver')
        receiver_ref = node1.lookup_str('localhost:20002/receiver')
        node1.spawn(Sender.using(receiver=receiver_ref))

        fileref = served.get()

        os.unlink(tmpname)
        os.mkfifo(tmpname)
        pipe = open(tmpname, 'w+')  # must open with w+ or rw, just w would block (because there are no readers)
        defer(pipe.close)

        pipe.write(random_data[:2])
        pipe.flush()

        sleep(.1)
        fileref.server.kill()

        failed.wait()
    test_interrupted_transfer.timeout = 1.0


wrap_globals(globals())
