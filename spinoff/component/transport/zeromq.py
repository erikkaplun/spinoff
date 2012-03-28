import pickle

from twisted.internet.defer import succeed, inlineCallbacks
from txzmq.connection import ZmqEndpoint
from txzmq.req_rep import ZmqDealerConnection, ZmqRouterConnection, ZmqRequestConnection, ZmqReplyConnection

from spinoff.component.component import Component


class ZmqProxyBase(Component):

    CONNECTION_CLASS = None
    DEFAULT_ENDPOINT_TYPE = 'connect'

    def __init__(self, factory, endpoint=None, identity=None):
        super(ZmqProxyBase, self).__init__()
        self._identity = identity
        self._endpoints = []
        self._conn = self.CONNECTION_CLASS(factory, identity=identity)
        self._conn.gotMessage = self._zmq_msg_received

        if endpoint:
            self.add_endpoints([endpoint])

    def _zmq_msg_received(self, message):
        message, inbox, routing_key = pickle.loads(message[0])
        self.put(message, outbox=inbox, routing_key=routing_key)

    def deliver(self, message, inbox, routing_key=None):
        msg_data_out = pickle.dumps((message, inbox, routing_key))
        self._conn.sendMsg(msg_data_out)
        return succeed(True)

    def add_endpoints(self, endpoints):
        endpoints = [
            (e if isinstance(e, ZmqEndpoint)
             else (ZmqEndpoint(*e) if isinstance(e, tuple)
                   else ZmqEndpoint(self.DEFAULT_ENDPOINT_TYPE, e)))
            for e in endpoints
            ]
        self._endpoints.extend(endpoints)
        self._conn.addEndpoints(endpoints)

    def __repr__(self):
        endpoints_repr = ';'.join('%s=>%s' % (endpoint.type, endpoint.address)
                                  for endpoint in self._endpoints)
        return '<%s%s%s>' % (type(self),
                             ' ' + self._identity if self._identity else '',
                             ' ' + endpoints_repr if endpoints_repr else '')

    def stop(self):
        self._conn.shutdown()


class ZmqRouter(ZmqProxyBase):
    CONNECTION_CLASS = staticmethod(ZmqRouterConnection)
    DEFAULT_ENDPOINT_TYPE = 'bind'

    def _zmq_msg_received(self, sender_id, message):
        message, inbox, routing_key = pickle.loads(message[0])
        self.put((sender_id, message), outbox=inbox, routing_key=routing_key)

    def deliver(self, message, inbox, routing_key=None):
        recipient_id, message = message
        msg_data_out = pickle.dumps((message, inbox, routing_key))
        self._conn.sendMsg(recipient_id, msg_data_out)
        return succeed(True)


class ZmqDealer(ZmqProxyBase):
    CONNECTION_CLASS = staticmethod(ZmqDealerConnection)


class ZmqRep(ZmqProxyBase):
    CONNECTION_CLASS = staticmethod(ZmqReplyConnection)
    DEFAULT_ENDPOINT_TYPE = 'bind'

    def _zmq_msg_received(self, message_id, message):
        message, inbox, routing_key = pickle.loads(message)
        self.put((message_id, message), outbox=inbox, routing_key=routing_key)

    def deliver(self, message, inbox, routing_key=None):
        try:
            message_id, message = message
        except ValueError:
            raise Exception("ZmqRouter requires messages of the form (request_id, response)")
        msg_data_out = pickle.dumps((message, inbox, routing_key))
        self._conn.sendMsg(message_id, msg_data_out)


class ZmqReq(ZmqProxyBase):
    CONNECTION_CLASS = staticmethod(ZmqRequestConnection)

    @inlineCallbacks
    def deliver(self, message, inbox, routing_key=None):
        msg_data_out = pickle.dumps((message, inbox, routing_key))
        msg_data_in = yield self._conn.sendMsg(msg_data_out)
        message, inbox, routing_key = pickle.loads(msg_data_in[0])
        self.put(message, outbox=inbox, routing_key=routing_key)
