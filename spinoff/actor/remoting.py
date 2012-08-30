from __future__ import print_function, absolute_import

import sys
import random
from cStringIO import StringIO
from cPickle import dumps
from pickle import Unpickler, BUILD

from spinoff.actor import ActorRef


dbg = lambda *args, **kwargs: print(file=sys.stderr, *args, **kwargs)


class Hub(object):
    """Handles traffic between actors on different nodes.

    The wire-transport implementation is specified/overridden by the `incoming` and `outgoing` parameters.

    """

    def __init__(self, incoming, outgoing, node):
        self.outgoing = outgoing
        incoming.gotMessage = self.got_message
        self.registry = {}
        self.node = node

    def make_proxy(self, path, node):
        return RemoteActor(path, node, bound_to=self)

    def register(self, actor):
        # TODO: use weakref
        self.registry[actor.path] = actor

    def send_message(self, path, node, msg, *_, **__):
        self.outgoing.sendMsg((node, dumps((path, msg))))

    def got_message(self, msg):
        path, msg = self._loads(msg)
        self.registry[path].send(msg)

    def _loads(self, data):
        return IncomingMessageUnpickler(self, StringIO(data)).load()


class RemoteActor(object):
    """A proxy that represents an actor on some other node.

    Internally, delegates all messages sent to it to `Hub`.

    """

    def __init__(self, path, node, bound_to):
        self.path = path
        self.node = node
        self.bound_to = bound_to

    def receive(self, message, force_async=None):
        """Delegates all messages sent to the `Hub` instance it is bound to, together with the address of the
        remote actor it represents.

        The `force_async` parameter is ignored because all remote messages are asynchronously delivered by default.
        However, the `Hub` instance can choose to emit the message into the underlying wire-transport immediately.

        """
        self.bound_to.send_message(self.path, self.node, message)


class IncomingMessageUnpickler(Unpickler):
    """Unpickler for attaching a `Hub` instance to all deserialized `ActorRef`s."""

    def __init__(self, dude, file):
        Unpickler.__init__(self, file)
        self.dude = dude

    # called by `Unpickler.load` before an uninitalized object is about to be filled with members;
    def _load_build(self):
        """See `pickle.py` in Python's source code."""
        # if the ctor. function (penultimate on the stack) is the `ActorRef` class...
        if isinstance(self.stack[-2], ActorRef):
            dict_ = self.stack[-1]
            proxy = self.dude.make_proxy(dict_['path'], dict_['node'])
            # ...set the `node` member of the object to be a `RemoteActor` proxy:
            dict_['target'] = proxy
        self.load_build()  # continue with the default implementation

    dispatch = dict(Unpickler.dispatch)  # make a copy of the original
    dispatch[BUILD] = _load_build  # override the handler of the `BUILD` instruction


class FakeNetwork(object):
    def __init__(self):
        self.nodes = {}
        self.queue = []

    def node(self, name):
        """Creates a new node with the specified name, with `FakeSocket` instances as incoming and outgoing sockets.

        Returns the `Hub` created for the node, and the sockets.

        """
        insock, outsock = FakeInSocket(), FakeOutSocket(sendMsg=lambda msg: self.enqueue(name, msg))
        hub = Hub(insock, outsock, node=name)
        self.nodes[name] = (hub, insock, outsock)
        return hub, insock, outsock

    def enqueue(self, from_node, (to_node, msg)):
        self.queue.append((from_node, to_node, msg))

    def transmit(self, randomize_order=False):
        """Puts all currently pending sent messages to the insock buffer of the recipient of the message.

        This is more useful than immediate "delivery" because it allows full flexibility of the order in which tests
        set up nodes and mock actors on those nodes, and of the order in which messages are sent out from a node.

        """
        if randomize_order:
            random.shuffle(self.queue)

        for from_node, to_node, message in self.queue:
            if to_node not in self.nodes:
                print("FakeNetwork: delivering message %r from %r to %r failed--no such node" %
                      (message, from_node, to_node), file=sys.stderr)
            else:
                to_sock = self.nodes[to_node][1]
                to_sock.gotMessage(message)
        del self.queue[:]


class FakeInSocket(object):
    """A fake (ZeroMQ-DEALER-like) socket.

    This will instead be a ZeroMQ DEALER connection object from the txzmq package under normal conditions.

    """
    def gotMessage(self, msg):
        assert False, "Hub should define gotMessage on the incoming transport"


class FakeOutSocket(object):
    """A fake (ZeroMQ-ROUTER-like) socket.

    This will instead be a ZeroMQ ROUTER connection object from the txzmq package under normal conditions.

    """
    def __init__(self, sendMsg):
        self.sendMsg = sendMsg
