from __future__ import print_function, absolute_import

import sys

from cStringIO import StringIO
from cPickle import dumps
from pickle import Unpickler, BUILD

from unnamedframework.actor import ActorRef


dbg = lambda *args, **kwargs: print(file=sys.stderr, *args, **kwargs)


class RemotingUnpickler(Unpickler):
    def __init__(self, dude, *args, **kwargs):
        Unpickler.__init__(self, *args, **kwargs)
        self.dude = dude

    def _load_build(self):
        # dbg("REMOTING: load_build")
        if isinstance(self.stack[-2], ActorRef):
            dict_ = self.stack[-1]
            proxy = self.dude.make_proxy(dict_['path'], dict_['node'])
            dict_['target'] = proxy
        self.load_build()

    dispatch = dict(Unpickler.dispatch)
    dispatch[BUILD] = _load_build


class TheDude(object):
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
        return RemotingUnpickler(self, StringIO(data)).load()


class RemoteActor(object):
    def __init__(self, path, node, bound_to):
        self.path = path
        self.node = node
        self.bound_to = bound_to

    def receive(self, message, force_async=None):
        self.bound_to.send_message(self.path, self.node, message)
