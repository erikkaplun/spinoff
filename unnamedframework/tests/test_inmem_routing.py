from unnamedframework.actor.actor import IProducer, IConsumer, Actor, NoRoute
from unnamedframework.actor.transport.inmem import InMemoryRouting, RoutingException
from unnamedframework.util.testing import assert_raises, assert_not_raises
from unnamedframework.util.testing import deferred_result


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

    server = Actor()
    client = Actor()
    client2 = Actor()

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

    server.put(outbox='out', message=(sender1, 'msg-3'))
    msg = _get_deferred_result(client.get(inbox='in'))
    assert msg == 'msg-3'

    routing.remove_client(client)
    with assert_raises(RoutingException, "should not be able to remove the same client twice"):
        routing.remove_client(client)
    with assert_raises(RoutingException, "should not be able to remove a non-existent client"):
        routing.remove_client(Actor())

    with assert_raises(RoutingException, "should not be able to send from a client that has been removed"):
        client.put(outbox='out', message='whatev')
    d = server.get(inbox='in')
    assert not d.called


def test_client_server_interface_with_default_boxes():
    routing = InMemoryRouting()

    server = Actor()
    client = Actor()

    routing.assign_server(server)
    routing.add_client(client, identity=1)

    client.put(message='msg-1')
    assert deferred_result(server.get()) == (1, 'msg-1')

    server.put((1, 'msg-2'))
    assert deferred_result(client.get()) == 'msg-2'


def test_client_server_interface_with_no_outbox_or_no_inbox():
    routing = InMemoryRouting()
    server = Actor()
    client = Actor()

    routing.assign_server(server, outbox=None)
    routing.add_client(client, outbox=None)

    with assert_raises(NoRoute):
        client.put('whatev')
    with assert_raises(NoRoute):
        server.put('whatev')

    ###
    routing = InMemoryRouting()
    server = Actor()
    client = Actor()

    routing.assign_server(server, inbox=None)
    routing.add_client(client)

    with assert_raises(NoRoute):
        client.put('whatev')

    ###
    routing = InMemoryRouting()
    server = Actor()
    client = Actor()

    routing.assign_server(server)
    routing.add_client(client, inbox=None, identity=123)

    with assert_raises(NoRoute):
        server.put((123, 'whatev'))


def test_manual_identity():
    routing = InMemoryRouting()

    server = Actor()
    client = Actor()

    routing.assign_server(server, inbox='in', outbox='out')
    routing.add_client(client, inbox='in', outbox='out', identity=987)

    client.put(outbox='out', message='whatev')

    sender, msg = deferred_result(server.get('in'))
    assert sender == 987


def test_dealer_to_router_communication():
    routing = InMemoryRouting()

    router = routing.make_router_endpoint()
    mock = Actor()
    router.connect('default', ('default', mock))

    dealer1 = routing.make_dealer_endpoint()
    dealer1.send(message='msg1', inbox='default')

    msg = _get_deferred_result(mock.get())
    assert isinstance(msg, tuple), "messages should be delivered on the other end as tuples"
    sender1_id, payload1 = msg
    assert payload1 == 'msg1', "message should be delivered on the other end as sent"

    dealer2 = routing.make_dealer_endpoint()
    dealer2.send(message='msg2', inbox='default')

    msg2 = _get_deferred_result(mock.get())
    sender_id2, payload2 = msg2
    assert payload2 == 'msg2'
    assert sender1_id != sender_id2, "messages sent by different dealers should have different sender IDs"

    dealer2.send(message='msg3', inbox='default')
    msg = _get_deferred_result(mock.get())
    tmp_sender_id, payload3 = msg
    assert payload3 == 'msg3'
    assert tmp_sender_id == sender_id2, "messages sent by a single dealer should have the same sender ID"


def test_router_to_dealer_communication():
    routing = InMemoryRouting()

    router = routing.make_router_endpoint()

    dealer1 = routing.make_dealer_endpoint()
    mock1 = Actor()
    dealer1.connect('default', ('default', mock1))

    router.send(inbox='default', message=(dealer1.identity, 'msg1'))
    msg = _get_deferred_result(mock1.get())
    assert msg == 'msg1'

    dealer2 = routing.make_dealer_endpoint()
    mock2 = Actor()
    dealer2.connect('default', ('default', mock2))

    router.send(inbox='default', message=(dealer2.identity, 'msg2'))
    msg = _get_deferred_result(mock2.get())
    assert msg == 'msg2'

    dealer1_msg_d = mock1.get()
    assert not dealer1_msg_d.called, "messages are only delivered to a single dealer"

    router.send(inbox='default', message=(dealer1.identity, 'msg3'))
    msg = _get_deferred_result(dealer1_msg_d)
    assert msg == 'msg3'


def test_remove_dealer():
    routing = InMemoryRouting()

    router = routing.make_router_endpoint()
    mock = Actor()
    router.connect('default', ('default', mock))

    dealer = routing.make_dealer_endpoint()

    routing.dealer_gone(dealer)

    with assert_raises(ValueError, "cannot remove dealer from routing more than once"):
        routing.dealer_gone(dealer)

    with assert_raises(RoutingException, "sending to a dealer that has previously been removed should not be possible"):
        router.send(inbox='default', message=(dealer.identity, 'whatev'))

    with assert_raises(RoutingException, "sending from a dealer that has previously been removed should not be possible"):
        dealer.send(inbox='default', message='whatev')


def _get_deferred_result(d):
    return deferred_result(d)
