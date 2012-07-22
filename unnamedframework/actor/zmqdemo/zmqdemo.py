import random

from twisted.internet.defer import inlineCallbacks
from txzmq import ZmqFactory

from unnamedframework.actor import Process, Application, Pipeline
from unnamedframework.actor.transport.zeromq import ZmqReq, ZmqRep
from unnamedframework.util.async import sleep


class ZmqTestClient(Process):

    @inlineCallbacks
    def start(self):
        while True:
            for i in range(3):
                self.put('elloo%d' % i)
            for i in range(3):
                msg = yield self.get()
                print 'got message: ', msg
            yield sleep(3.0 + random.random())


class ZmqTestServer(Process):

    @inlineCallbacks
    def start(self):
        while True:
            req = yield self.get()
            print 'got request: ', req
            yield sleep(5.0)
            self.put(req)


f = ZmqFactory()


client1 = ZmqTestClient()
client2 = ZmqTestClient()
client3 = ZmqTestClient()
server = ZmqTestServer()

application = Application(
    Pipeline(
        client1,
        ZmqReq(f, 'ipc://foobar', 'client1'),
        client1,
        ),

    Pipeline(
        client2,
        ZmqReq(f, 'ipc://foobar', 'client2'),
        client2,
        ),

    Pipeline(
        client3,
        ZmqReq(f, 'ipc://foobar', 'client3'),
        client3,
        ),

    Pipeline(
        server,
        ZmqRep(f, 'ipc://foobar'),
        server,
        ),
    )
