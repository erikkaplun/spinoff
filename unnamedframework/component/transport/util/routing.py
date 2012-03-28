from collections import namedtuple

from unnamedframework.component.component import Component


PendingMessage = namedtuple('PendingMessage', ['to', 'message', 'outbox'])


class MsgDispatcher(Component):

    def __init__(self):
        super(MsgDispatcher, self).__init__()
        self.routing_table = {}
        self.pending_messages = []

    def deliver(self, message, inbox, routing_key):
        if routing_key == 'init':
            return self.handle_init(message, inbox)
        else:
            return self.handle_msg(message, inbox, routing_key)

    def handle_init(self, message, inbox):
        # TODO: what to do with inbox?
        sender_node_id, message = message
        sender_actor_id = message
        self.routing_table[sender_actor_id] = sender_node_id
        for pending_message in self.pending_messages:
            if pending_message.to == sender_actor_id:
                self.put((sender_node_id, pending_message.message),
                         outbox=pending_message.outbox,
                         routing_key=sender_actor_id)

    def handle_msg(self, message, inbox, routing_key):
        recipient_actor_id = routing_key
        # tmp_sender_actor_id = [actor_id for actor_id, node_id in self.routing_table.items() if node_id == sender_node_id][0]
        print "MSG-DISPATCHER: dispatching %s   ==> %s" % (repr(message), recipient_actor_id)

        if recipient_actor_id in self.routing_table:
            print "MSG: sending message immediately"
            recipient_node_id = self.routing_table[recipient_actor_id]
            self.put((recipient_node_id, message), outbox=inbox, routing_key=recipient_actor_id)
        else:
            print "MSG-DISPATCHING: pending message"
            self.pending_messages.append(
                PendingMessage(to=recipient_actor_id,
                               message=message,
                               outbox=inbox))


class MsgRouter(MsgDispatcher):

    def handle_msg(self, message, inbox, routing_key):
        sender_node_id, message = message
        print "MSG-ROUTER: routing message from %s" % sender_node_id
        return super(MsgRouter, self).handle_msg(message, inbox, routing_key)
