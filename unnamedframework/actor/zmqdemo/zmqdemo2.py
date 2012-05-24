from twisted.internet.defer import inlineCallbacks
from txzmq import ZmqFactory, ZmqEndpoint

from unnamedframework.actor.actor import Actor, Application, make_cycle
from unnamedframework.actor.transport.zeromq import ZmqRep, ZmqDealer, ZmqRouter, ZmqReq
from unnamedframework.util.async import sleep


class TestBroker(Actor):

    def __repr__(self):
        return '<TestBroker>'

    @inlineCallbacks
    def start(self):

        @inlineCallbacks
        def handle(recipient_id, msg):
            print 'BROKER putting msg to backend'
            yield self.put(msg, outbox='backend')
            print 'BROKER msg delivered to backend'
            result = yield self.get(inbox='backend')
            print 'BROKER result received from backend'
            self.put((recipient_id, result), outbox='frontend')

        print 'BROKER listening on front end'
        while True:
            sender_id, msg = yield self.get(inbox='frontend')
            print 'BROKER got %s' % msg
            handle(sender_id, msg)


class TestProducer(Actor):

    def __repr__(self):
        return '<TestProducer>'

    def __init__(self, id):
        super(TestProducer, self).__init__()
        self._id = id
        self._translated_ids = {}
        self._next_id = 1

    def _translate(self, uuid):
        if uuid in self._translated_ids:
            return self._translated_ids.pop(uuid)
        else:
            ret = self._next_id
            self._next_id += 1
            self._translated_ids[uuid] = ret
            return ret

    @inlineCallbacks
    def start(self):
        while True:
            message_id, msg = yield self.get()
            print 'PRODUCER %s got request with ID %s' % (self._id, self._translate(message_id))
            yield sleep(1.0)
            self.put((message_id, 'PRODUCER %s echoing back "%s"' % (self._id, msg)))
            print 'PRODUCER %s sent response to request with ID %s' % (self._id, self._translate(message_id))


class TestConsumer(Actor):

    def __repr__(self):
        return '<TestConsumer>'

    def __init__(self, id):
        super(TestConsumer, self).__init__()
        self.id = id

    @inlineCallbacks
    def start(self):
        yield sleep(.1)
        while True:
            print 'CONSUMER %s sending' % self.id
            self.put(message='msg-from-%s' % self.id)
            msg = yield self.get()
            print 'CONSUMER %s received %s' % (self.id, msg)


f = ZmqFactory()

BROKER_BE_ADDR = 'ipc://broker-be'
BROKER_FE_ADDR = 'ipc://broker-fe'


broker = TestBroker()

make_cycle(ZmqRouter(f, ZmqEndpoint('bind', BROKER_FE_ADDR)),
           ('frontend', broker, 'frontend'))

make_cycle(ZmqReq(f, ZmqEndpoint('bind', BROKER_BE_ADDR)),
           ('backend', broker, 'backend'))



consumers = [TestConsumer(i + 1) for i in range(3)]
for consumer in consumers:
    dealer = ZmqDealer(f, BROKER_FE_ADDR)
    make_cycle(consumer, dealer)

producers = [TestProducer(i + 1) for i in range(3)]
for producer in producers:
    rep = ZmqRep(f, ZmqEndpoint('connect', BROKER_BE_ADDR))
    make_cycle(producer, rep)


application = Application(producers, [broker], consumers)


# client1 = ZmqTestClient()
# client2 = ZmqTestClient()
# client3 = ZmqTestClient()
# server = ZmqTestServer()

# application = Application(
#     # Pipeline(
#     #     client1,
#     #     ZmqReq(f, 'ipc://foobar', 'client1'),
#     #     client1,
#     #     ),

#     # Pipeline(
#     #     client2,
#     #     ZmqReq(f, 'ipc://foobar', 'client2'),
#     #     client2,
#     #     ),

#     # Pipeline(
#     #     client3,
#     #     ZmqReq(f, 'ipc://foobar', 'client3'),
#     #     client3,
#     #     ),

#     # Pipeline(
#     #     server,
#     #     ZmqRep(f, 'ipc://foobar'),
#     #     server,
#     #     ),
#     )
