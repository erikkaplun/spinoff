import random

from twisted.internet.defer import inlineCallbacks
from txzmq import ZmqFactory, ZmqEndpoint

from spinoff.actor import Actor, Application, Pipeline, make_cycle
from spinoff.actor.transport.zeromq import ZmqRep, ZmqDealer, ZmqRouter, ZmqReq
from spinoff.util.async import sleep


class TestServer(Actor):

    def __repr__(self):
        return '<TestServer>'

    def __init__(self):
        super(TestServer, self).__init__()
        self._next_id = 1
        self._routing_info = {}

    @inlineCallbacks
    def start(self):

        @inlineCallbacks
        def handle_ctrl():
            while True:
                transport_id, (_, agent_id) = yield self.get(inbox='ctrl')
                print "SERVER: got reg from agent %s @ ZMQ identity %s" % (agent_id, repr(transport_id))
                self._routing_info[agent_id] = transport_id

        handle_ctrl()

        while True:
            if not self._routing_info:
                yield sleep(.1)
                continue
            agent_id, transport_id = random.choice(self._routing_info.items())
            print "SERVER: send job to agent %s @ ZMQ identity %s" % (agent_id, repr(transport_id))
            self.put((transport_id, 'job'), outbox='jobs')
            yield sleep(1.0)

    def stop(self):
        print "SERVER: going down"
        for _, transport_id in self._routing_info.items():
            self.put((transport_id, 'server-going-down'), outbox='ctrl')


f = ZmqFactory()

SERVER_ADDR = 'ipc://server'


server = TestServer()
server_transport = ZmqRouter(f, SERVER_ADDR)

server.connect(['ctrl', 'jobs'], server_transport)
server_transport.connect('ctrl', server)


application = Application(
    [server]
    )
