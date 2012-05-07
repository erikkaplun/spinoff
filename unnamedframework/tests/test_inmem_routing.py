from unnamedframework.component.component import IProducer, IConsumer, Component
from unnamedframework.component.transport.inmem import InMemoryRouting, RoutingException
from unnamedframework.util.testing import assert_raises, assert_not_raises


def _all_neq(*args):
    for i, el in enumerate(args):
        if el in args[:i]:
            return False
    else:
        return True


def test_interface():
    routing = InMemoryRouting()
    assert isinstance(routing, InMemoryRouting)

    router = routing.make_router_endpoint()
    assert IProducer.providedBy(router)
    assert IConsumer.providedBy(router)

    with assert_raises(message="it is only possible to create one router endpoint"):
        routing.make_router_endpoint()

    dealer1 = routing.make_dealer_endpoint()
    assert IProducer.providedBy(dealer1)
    assert IConsumer.providedBy(dealer1)

    with assert_not_raises(message="it is possible to create many dealer endpoints"):
        dealer2 = routing.make_dealer_endpoint()

    assert dealer1 != dealer2

    assert IProducer.providedBy(dealer2)
    assert IConsumer.providedBy(dealer2)

    dealer3 = routing.make_dealer_endpoint()
    with assert_raises(AttributeError, "a dealer's identity should be read-only"):
        dealer3.identity = 123

    dealer4 = routing.make_dealer_endpoint(identity='123')
    assert dealer4.identity == '123'

    with assert_raises(message="routing should ensure that dealers have unique identities"):
        routing.make_dealer_endpoint(identity='123')


def test_client_server_interface():
    routing = InMemoryRouting()

    server = Component()
    client = Component()
    client2 = Component()

    routing.assign_server(server, inbox='in', outbox='out')
    routing.add_client(client, inbox='in', outbox='out')

    with assert_raises(RoutingException, "should not be able to assign server twice"):
        routing.assign_server(server, inbox='whatev', outbox='whatev')

    with assert_not_raises(RoutingException):
        routing.add_client(client2, inbox='in', outbox='out')

    with assert_raises(RoutingException, "should not be able to add the same client twice"):
        routing.add_client(client, inbox='in', outbox='out')

    client.put(outbox='out', message='msg-1')
    sender1, msg1 = _get_deferred_result(server.get(inbox='in'))
    assert msg1 == 'msg-1'

    client2.put(outbox='out', message='msg-2')
    sender2, msg2 = _get_deferred_result(server.get(inbox='in'))
    assert msg2 == 'msg-2'

    assert sender1 != sender2

    server.put(outbox='out', message='msg-3', routing_key=sender1)
    msg = _get_deferred_result(client.get(inbox='in'))
    assert msg == 'msg-3'

    routing.remove_client(client)
    with assert_raises(RoutingException, "should not be able to remove the same client twice"):
        routing.remove_client(client)
    with assert_raises(RoutingException, "should not be able to remove a non-existent client"):
        routing.remove_client(Component())

    with assert_raises(RoutingException, "should not be able to send from a client that has been removed"):
        client.put(outbox='out', message='whatev')
    d = server.get(inbox='in')
    assert not d.called


def test_dealer_to_router_communication():
    routing = InMemoryRouting()

    router = routing.make_router_endpoint()
    mock = Component()
    router.connect('default', ('default', mock))

    dealer1 = routing.make_dealer_endpoint()
    dealer1.deliver(message='msg1', inbox='default', routing_key=None)

    msg = _get_deferred_result(mock.get())
    assert isinstance(msg, tuple), "messages should be delivered on the other end as tuples"
    sender1_id, payload1 = msg
    assert payload1 == 'msg1', "message should be delivered on the other end as sent"

    dealer2 = routing.make_dealer_endpoint()
    dealer2.deliver(message='msg2', inbox='default', routing_key=None)

    msg2 = _get_deferred_result(mock.get())
    sender_id2, payload2 = msg2
    assert payload2 == 'msg2'
    assert sender1_id != sender_id2, "messages sent by different dealers should have different sender IDs"

    dealer2.deliver(message='msg3', inbox='default', routing_key=None)
    msg = _get_deferred_result(mock.get())
    tmp_sender_id, payload3 = msg
    assert payload3 == 'msg3'
    assert tmp_sender_id == sender_id2, "messages sent by a single dealer should have the same sender ID"


def test_router_to_dealer_communication():
    routing = InMemoryRouting()

    router = routing.make_router_endpoint()

    dealer1 = routing.make_dealer_endpoint()
    mock1 = Component()
    dealer1.connect('default', ('default', mock1))

    router.deliver(inbox='default', message='msg1', routing_key=dealer1.identity)
    msg = _get_deferred_result(mock1.get())
    assert msg == 'msg1'

    dealer2 = routing.make_dealer_endpoint()
    mock2 = Component()
    dealer2.connect('default', ('default', mock2))

    router.deliver(inbox='default', message='msg2', routing_key=dealer2.identity)
    msg = _get_deferred_result(mock2.get())
    assert msg == 'msg2'

    dealer1_msg_d = mock1.get()
    assert not dealer1_msg_d.called, "messages are only delivered to a single dealer"

    router.deliver(inbox='default', message='msg3', routing_key=dealer1.identity)
    msg = _get_deferred_result(dealer1_msg_d)
    assert msg == 'msg3'


def test_remove_dealer():
    routing = InMemoryRouting()

    router = routing.make_router_endpoint()
    mock = Component()
    router.connect('default', ('default', mock))

    dealer = routing.make_dealer_endpoint()

    routing.dealer_gone(dealer)

    with assert_raises(ValueError, "cannot remove dealer from routing more than once"):
        routing.dealer_gone(dealer)

    with assert_raises(RoutingException, "sending to a dealer that has previously been removed should not be possible"):
        router.deliver(inbox='default', message='whatev', routing_key=dealer.identity)

    with assert_raises(RoutingException, "sending from a dealer that has previously been removed should not be possible"):
        dealer.deliver(inbox='default', message='whatev', routing_key=None)


def _get_deferred_result(d):
    assert d.called, "messages should be delivered immediately"
    result = [None]
    d.addCallback(lambda ret: result.__setitem__(0, ret))
    msg = result[0]
    return msg
