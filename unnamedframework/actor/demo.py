from twisted.internet.defer import inlineCallbacks, returnValue

from unnamedframework.actor.actor import Actor, Application, Pipeline
from unnamedframework.util.async import sleep


__all__ = ['application']


TIME_UNIT = 1.0


class Base(Actor):

    def __init__(self, id, speed=1.0, *args, **kwargs):
        super(Base, self).__init__(*args, **kwargs)
        self.id = id
        self.interval = TIME_UNIT / speed


class Sender(Base):

    @inlineCallbacks
    def start(self):
        yield sleep(.5)

        while True:
            yield self._do_send()
            yield sleep(self.interval)

    def _do_send(self, message=None):
        print '(%s) SEND' % self.id
        self.put(message or ('message-from-%s' % self.id))


class Receiver(Base):

    @inlineCallbacks
    def start(self):
        while True:
            yield self._do_recv()
            yield sleep(self.interval)

    @inlineCallbacks
    def _do_recv(self):
        print '(%s) RECV' % self.id
        message = yield self.get()
        print '(%s) GOT.' % self.id
        self.debug_state(self.id)
        returnValue(message)


class SenderReceiver(Sender, Receiver):

    @inlineCallbacks
    def start(self):
        yield sleep(self.interval / 2)
        while True:
            self._do_send()
            yield self._do_recv()
            yield sleep(self.interval)


class ReceiverSender(Sender, Receiver):

    @inlineCallbacks
    def start(self):
        while True:
            yield self._do_recv()
            yield sleep(self.interval)
            self._do_send()


class Repeater(Receiver, Sender):
    @inlineCallbacks
    def start(self):
        while True:
            message = yield self._do_recv()
            self._do_send(message)


sra, srb = SenderReceiver('party-b'), SenderReceiver('party-a')
srb.connect('default', sra)

application = Application(
    # Pipeline(
    #     Sender('sender-1'),
    #     Receiver('receiver-1'),
    #     ),

    # Pipeline(
    #     Sender('sender-3'),
    #     ReceiverSender('sender-receiver-3'),
    #     Receiver('receiver-3'),
    #     ),

    # Pipeline(
    #     Sender('sender-4'),
    #     # LoadBalancer(
    #     Publisher(
    #         ReceiverSender('sender-receiver-4-ONE'),
    #         ReceiverSender('sender-receiver-4-TWO'),
    #         ),
    #     Receiver('receiver-4', speed=10),
    #     ),

    # cyclic
    Pipeline(sra, srb),

    # Pipeline(
    #     Sender('sender-6'),
    #     Repeater('repeater'),
    #     Receiver('receiver-6'),
    #     ),
    )
