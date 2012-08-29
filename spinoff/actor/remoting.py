from __future__ import print_function, absolute_import

import sys

from cStringIO import StringIO
from cPickle import dumps
from pickle import Unpickler, BUILD

from spinoff.actor import ActorRef


dbg = lambda *args, **kwargs: print(file=sys.stderr, *args, **kwargs)


class Hub(object):
    """Handles traffic between actors on different nodes.

    The wire-transport implementation is specified/overridden by the `incoming` and `outgoing` parameters.

    """

    def __init__(self, incoming, outgoing, nodename):
        self.outgoing = outgoing
        incoming.gotMessage = self.got_message
        self.registry = {}

    def make_proxy(self, path, node):
        return RemoteActor(path, node, bound_to=self)

    def register(self, actor):
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
