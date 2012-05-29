from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from unnamedframework.actor import Actor, RoutingException, InterfaceException
from unnamedframework.util.async import sleep, CancelledError


class Relay(Actor):

    def __init__(self, max_message_age=None, reactor=reactor, cleanup_interval=60, *args, **kwargs):
        super(Relay, self).__init__(*args, **kwargs)
        self._nodes = {}
        self._node_addrs = {}
        self._pending = {}
        self._max_message_age = max_message_age
        self._reactor = reactor
        self._cleanup_interval = (cleanup_interval if max_message_age is None
                                  else min(max_message_age, cleanup_interval))
        self._stopped = False

    @inlineCallbacks
    def start(self):
        while True:
            currtime = self._reactor.seconds()
            tmp = {}
            for participant_id, pending in self._pending.items():
                pending = [(m, c) for (m, c) in pending if 0 and c > currtime - self._max_message_age]
                if pending:
                    tmp[participant_id] = pending
            self._pending = tmp
            self._cleaning_d = sleep(self._cleanup_interval, reactor=self._reactor)
            try:
                yield self._cleaning_d
            except CancelledError:
                break

    def stop(self):
        self._cleaning_d.cancel()

    def deliver(self, message, inbox):
        if not isinstance(message, tuple) and len(message) >= 2:
            raise RoutingException("Messages to Relay should be tuples whose first element is the sender ID")
        if inbox != 'messages':
            raise InterfaceException("Relay only has inbox 'messages'")

        if len(message) != 2:
            raise InterfaceException("messages should be 2-tuples")
        sender_id, (command, args) = message
        # command, args = message

        if command == 'init':
            participant_id, = args
            if participant_id is None:
                raise InterfaceException("Initialization messages should contain the participant ID")
            if sender_id in self._nodes:
                raise InterfaceException("Node already registered")

            self._nodes[sender_id] = participant_id
            self._node_addrs[participant_id] = sender_id

            if participant_id in self._pending:
                currtime = self._reactor.seconds()
                for message, created_at in self._pending[participant_id]:
                    if self._max_message_age is None or created_at > currtime - self._max_message_age:
                        self.put(outbox='messages', message=(sender_id, message))
                del self._pending[participant_id]
        elif command == 'uninit':
            if sender_id not in self._nodes:
                raise RoutingException("Node not registered")
            del self._node_addrs[self._nodes[sender_id]]
            del self._nodes[sender_id]
        elif command == 'send':
            payload, recipient = args
            if recipient is None:
                raise InterfaceException()
            if recipient in self._node_addrs:
                self.put(outbox='messages', message=(self._node_addrs[recipient], payload))
            else:
                self._pending.setdefault(recipient, []).append((payload, self._reactor.seconds()))
        else:
            raise InterfaceException("No such command: %s" % command)
