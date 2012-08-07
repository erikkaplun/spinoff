from txzmq.connection import ZmqEndpoint
from txzmq.req_rep import ZmqDealerConnection, ZmqRouterConnection, ZmqRequestConnection, ZmqReplyConnection
from txzmq import ZmqFactory

from unnamedframework.actor import Actor, NOT_STARTED


class ZmqProxyBase(Actor):

    CONNECTION_CLASS = None
    DEFAULT_ENDPOINT_TYPE = 'connect'

    _FACTORY = None

    def __init__(self, endpoint=None, identity=None):
        super(ZmqProxyBase, self).__init__()
        self._identity = identity
        self._endpoints = []
        self._pending_endpoints = [endpoint] if endpoint else []

    def _after_start(self):
        if not ZmqProxyBase._FACTORY:
            ZmqProxyBase._FACTORY = ZmqFactory()

        self._conn = self.CONNECTION_CLASS(self._FACTORY, identity=self._identity)
        self._conn.gotMessage = self._zmq_msg_received
        self.add_endpoints(self._pending_endpoints)

    def _zmq_msg_received(self, message):
        message = message[0]
        self.parent.send(message)

    def receive(self, message):
        self._conn.sendMsg(message)

    def add_endpoints(self, endpoints):
        if self._state is NOT_STARTED:
            self._pending_endpoints += endpoints
            return

        endpoints = [
            (e if isinstance(e, ZmqEndpoint)
             else (ZmqEndpoint(*e) if isinstance(e, tuple)
                   else ZmqEndpoint(self.DEFAULT_ENDPOINT_TYPE, e)))
            for e in endpoints
            ]
        self._endpoints.extend(endpoints)
        self._conn.addEndpoints(endpoints)

    @property
    def identity(self):
        return self._identity

    def __repr__(self):
        endpoints_repr = ';'.join('%s=>%s' % (endpoint.type, endpoint.address)
                                  for endpoint in self._endpoints)
        return '<%s%s%s>' % (type(self),
                             ' ' + self._identity if self._identity else '',
                             ' ' + endpoints_repr if endpoints_repr else '')

    def _on_stop(self):
        self._conn.shutdown()
        super(ZmqProxyBase, self)._on_stop()


class ZmqRouter(ZmqProxyBase):
    CONNECTION_CLASS = staticmethod(ZmqRouterConnection)
    DEFAULT_ENDPOINT_TYPE = 'bind'

    def _zmq_msg_received(self, sender_id, message):
        message = message[0]
        self.parent.send((sender_id, message))

    def receive(self, message):
        if not isinstance(message, tuple) or len(message) != 2:
            self.parent.send(('error', self, ('unhandled-message', message), None))
            return
        recipient_id, message = message
        self._conn.sendMsg(recipient_id, message)


class ZmqDealer(ZmqProxyBase):
    CONNECTION_CLASS = staticmethod(ZmqDealerConnection)


class ZmqRep(ZmqProxyBase):
    CONNECTION_CLASS = staticmethod(ZmqReplyConnection)
    DEFAULT_ENDPOINT_TYPE = 'bind'

    def _zmq_msg_received(self, message_id, message):
        self.parent.send((message_id, message))

    def receive(self, message):
        try:
            message_id, message = message
        except ValueError:
            raise Exception("ZmqRouter requires messages of the form (request_id, response)")
        msg_data_out = message
        self._conn.sendMsg(message_id, msg_data_out)


class ZmqReq(ZmqProxyBase):
    CONNECTION_CLASS = staticmethod(ZmqRequestConnection)

    def receive(self, message):
        msg_data_in = yield self._conn.sendMsg(message)
        self.parent.send(msg_data_in[0])
