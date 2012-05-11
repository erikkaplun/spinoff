import base64

from zope.interface import implements

from twisted.internet import reactor, defer, protocol
from twisted.internet.defer import inlineCallbacks, returnValue, succeed
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer


class receive_data(defer.Deferred, protocol.Protocol):
    """A generic helper for asynchronously receiving data from connections.

    This simplifies cases in which only a complete response is of
    interested by avoiding a need to manually create response builder
    objects.

    Callbacks can be attached directly to this object as it also
    inherits from Deferred. Therefore, it can also be used trivially
    from @inlineCallbacks by doing `yield receive_data(whatever)`.

    Standard usage:
        deferred = received_data(http_response.deliverBody)
        def on_success(ret):
            print 'Received body:\n%s' % ret
        deferred.addCallback(on_success)

    inlineCallbacks usage:
        response_body = yield receive_data(http_response.deliverBody)

    """

    def __init__(self, start_fn):
        defer.Deferred.__init__(self)
        self._buffer = []
        start_fn(self)

    def dataReceived(self, bytes):
        self._buffer.append(bytes)

    def connectionLost(self, reason):
        self.callback(''.join(self._buffer))


class send_data(object):
    implements(IBodyProducer)

    def __init__(self, data):
        self._data = data
        self.length = len(data)

    def startProducing(self, consumer):
        consumer.write(self._data)
        return defer.succeed(None)

    def stopProducing(self):
        pass


@inlineCallbacks
def get_page(method, uri, basic_auth=None, headers=None, postdata=None):
    headers = headers or {}

    if basic_auth:
        assert isinstance(basic_auth, tuple)
        username, password = basic_auth
        headers['Authorization'] = [basic_auth_string(username, password)]

    headers = Headers(headers)

    agent = Agent(reactor)
    if postdata is not None:
        postdata = StringProducer(postdata)
    response = yield agent.request(method, uri, headers, postdata)
    body = yield receive_data(response.deliverBody)
    returnValue((response.code, response.headers, body))


def basic_auth_string(username, password):
    """
    Encode a username and password for use in an HTTP Basic Authentication
    header
    """
    b64 = base64.encodestring('%s:%s' % (username, password)).strip()
    return 'Basic %s' % b64


class StringProducer(object):
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass
