import uuid

from twisted.internet.defer import inlineCallbacks
from txzmq import ZmqFactory, ZmqEndpoint

from spinoff.actor.actor import Actor, Application, Pipeline, make_cycle
from spinoff.actor.transport.zeromq import ZmqRep, ZmqDealer, ZmqRouter, ZmqReq
from spinoff.util.async import sleep


class TestAgent(Actor):

    def __repr__(self):
        return '<TestAgent>'

    def __init__(self, id):
        super(TestAgent, self).__init__()
        self.id = id

    @inlineCallbacks
    def start(self):
        yield sleep(.1)

        print 'AGENT %s registering' % self.id
        self.put(message=('reg', self.id), outbox='ctrl')

        while True:
            msg = yield self.get(inbox='jobs')
            print 'AGENT %s received %s' % (self.id, msg)

    def stop(self):
        print "CLIENT: going down"
        self.put('going-down', outbox='ctrl')


f = ZmqFactory()

SERVER_ADDR = 'ipc://server'


agents = [(TestAgent(str(uuid.uuid4())),
           ZmqDealer(f, SERVER_ADDR))
          for i in range(10)]


application = Application(
    *[
        make_cycle(
            ('jobs', a, 'ctrl'),
            ('ctrl', t, 'jobs')
            )
        for a, t in agents
        ]
    )
