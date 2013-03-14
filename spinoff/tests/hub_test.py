import uuid
import random

from nose.tools import ok_

from spinoff.util.testing.actor import wrap_globals
from spinoff.remoting.hublogic import (
    HubLogic, Connect, Disconnect, NodeDown, Ping, Send, Receive, SendFailed, SigDisconnect,
    RelayConnect, RelaySend, RelaySigNodeDown, RelaySigConnected, RelayForward, RelaySigNew, RelayNvm,
    NextBeat, IN, OUT, flatten, nid2addr)
from spinoff.util.pattern_matching import ANY


class Time(object):
    t = 0.0

    def advance(self, by):
        self.t += by
        return self.t

    @property
    def current(self):
        return self.t

    def __repr__(self):
        return str(self.t)


def random_bytes():
    return 'random' + str(random.randint(0, 100000))


DEFAULT_LOGIC = lambda: HubLogic('me:123', 1.0, 3.0)
RELAY_LOGIC = lambda: HubLogic('me:123', 1.0, 3.0, is_relay=True)
NID = lambda addr: addr
UNIQUE_NID = lambda addr: addr + '|' + uuid.uudi4().bytes[:8]


def test_empty(t=Time, logic=DEFAULT_LOGIC):
    t, logic = t(), logic()
    # empty
    emits_(logic.heartbeat(t=t.advance(0)), [(NextBeat, 1.0)])


def test_successful_connect(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic = t(), logic()
    # connect
    emits_(logic.ensure_connected(nid, t=t.current), [(Connect, nid2addr(nid)), (Ping, OUT, nid, ANY)])
    # ...successfully
    emits_(logic.ping_received(OUT, nid, 0, t=t.advance(by=2.5)), [(Ping, OUT, nid, ANY)])
    return t, logic


def test_failed_connect(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic = t(), logic()
    # connect
    emits_(logic.ensure_connected(nid, 0), [(Connect, nid2addr(nid)), (Ping, OUT, nid, ANY)])
    # heartbeat not needed
    emits_(logic.heartbeat(t=t.current), [(NextBeat, 1.0)])
    # ping emitted
    emits_(logic.heartbeat(t=t.advance(by=1.0)), [(Ping, OUT, nid, ANY), (NextBeat, 1.0)])
    # ping emitted
    emits_(logic.heartbeat(t=t.advance(by=1.0)), [(Ping, OUT, nid, ANY), (NextBeat, 1.0)])
    # disconnect occurs
    emits_(logic.heartbeat(t=t.advance(by=1.0)), [(Disconnect, nid2addr(nid)), (NodeDown, nid), (NextBeat, 1.0)])
    # silence
    emits_(logic.heartbeat(t=t.advance(by=1.0)), [(NextBeat, 1.0)])
    return t, logic


#

def test_sig_disconnect_while_connected(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic = test_successful_connect(t, logic)
    emits_(logic.sig_disconnect_received(nid), [(Disconnect, nid2addr(nid)), (NodeDown, nid)])


def test_sig_disconnect_while_still_connecting(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic = test_successful_connect(t, logic)
    emits_(logic.sig_disconnect_received(nid), [(Disconnect, nid2addr(nid)), (NodeDown, nid)])


#

def test_sending_heartbeat(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic = test_successful_connect(t, logic)
    # unndeeded heartbeat not emitted
    emits_(logic.heartbeat(t=t.advance(by=.2)), [(NextBeat, 1.0)])
    # heartbeat received
    emits_(logic.ping_received(OUT, nid, 1, t=t.advance(by=.5)), [(Ping, OUT, nid, ANY)])
    # heartbeat not needed
    emits_(logic.heartbeat(t=t.advance(by=.2)), [(NextBeat, 1.0)])
    # heartbeat received
    emits_(logic.ping_received(OUT, nid, 2, t=t.advance(by=.5)), [(Ping, OUT, nid, ANY)])
    # heartbeat not needed
    emits_(logic.heartbeat(t=t.advance(by=.2)), [(NextBeat, 1.0)])
    # heartbeat needed
    emits_(logic.heartbeat(t=t.advance(by=1.0)), [(Ping, OUT, nid, ANY), (NextBeat, 1.0)])
    return t, logic


def test_fail_after_successful_connection(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic = test_successful_connect(t, logic)
    emits_(logic.heartbeat(t=t.advance(logic.heartbeat_max_silence)), [(NextBeat, 1.0), (Disconnect, nid2addr(nid)), (NodeDown, nid)])


def test_failed_connect_and_then_successful_connect(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic = test_failed_connect(t, logic, nid=nid)
    t, logic = test_successful_connect(lambda: t, lambda: logic, nid=nid)


#


def test_send_message_with_no_previous_connection(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic, msg = t(), logic(), object()
    emits_(logic.send_message(nid, msg, t=t.current), [(Connect, nid2addr(nid)), (Ping, OUT, nid, ANY)])
    return t, logic, msg


def test_send_message_with_no_previous_connection_and_no_response(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic, msg = test_send_message_with_no_previous_connection(t, logic, nid=nid)
    emits_(logic.heartbeat(t=t.advance(logic.heartbeat_max_silence)), [(Disconnect, nid2addr(nid)), (SendFailed, msg), (NodeDown, nid), (NextBeat, 1.0)])


def test_send_message_with_no_previous_connection_and_successful_response(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic, msg = test_send_message_with_no_previous_connection(t, logic, nid=nid)
    emits_(logic.ping_received(OUT, nid, 0, t=t.current), [(Send, OUT, nid, ANY, msg)])


def test_send_message_with_no_previous_connection_and_sigdisconnect_response(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic, msg = test_send_message_with_no_previous_connection(t, logic, nid=nid)
    emits_(logic.sig_disconnect_received(nid), [(Disconnect, nid2addr(nid)), (NodeDown, nid), (SendFailed, msg)])


def test_send_message_with_an_existing_connection(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    (t, logic), msg = test_successful_connect(t, logic, nid=nid), object()
    emits_(logic.send_message(nid, msg, t=t.current), [(Send, OUT, nid, ANY, msg)])
    # and SendFailed not emitted
    emits_(logic.sig_disconnect_received(nid), [(Disconnect, nid2addr(nid)), (NodeDown, nid)])


def test_receive_ping_with_no_prior_connection(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic = t(), logic()
    emits_(logic.ping_received(IN, nid, 1, t=t.current), [(Ping, IN, nid, 0)])
    return t, logic


def test_receive_message_with_no_prior_connection(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic, msg = t(), logic(), object()
    emits_(logic.message_received(IN, nid, 1, msg, t=t.current), [(Ping, IN, nid, 0), (Receive, nid, msg)])
    return t, logic


def test_receiving_ping_from_nil_means_the_connection_will_be_reused(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    (t, logic), msg = test_receive_ping_with_no_prior_connection(t, logic, nid=nid), object()
    emits_(logic.send_message(nid, msg, t=t.current), [(Send, IN, nid, ANY, msg)])


def test_receiving_message_from_nil_means_the_connection_will_be_reused(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    (t, logic), msg = test_receive_message_with_no_prior_connection(t, logic, nid=nid), object()
    emits_(logic.send_message(nid, msg, t=t.current), [(Send, IN, nid, ANY, msg)])


def test_sig_disconnect_while_not_connected_does_nothing(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic = t(), logic()
    emits_(logic.sig_disconnect_received(nid), [])


def test_shutdown_sigdisconnects(t=Time, logic=DEFAULT_LOGIC, nid=NID('kaamel:123')):
    t, logic = test_successful_connect(t, logic, nid=nid)
    emits_(logic.shutdown(), [(SigDisconnect, OUT, nid)])


# relay

def test_relay_failed_connect_request(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = t(), logic()
    just_(logic.ping_received(IN, cat, 1, t.current))
    emits_(logic.relay_connect_received(IN, cat, mouse), [(RelaySigNodeDown, IN, cat, mouse)])


def test_relay_failed_connect_request_if_target_is_on_out(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = test_successful_connect(t, logic, nid=mouse)
    just_(logic.ping_received(IN, cat, 1, t.current))
    emits_(logic.relay_connect_received(IN, cat, mouse), [(RelaySigNodeDown, IN, cat, mouse)])


def test_relay_successful_connect_request(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = t(), logic()
    just_(logic.ping_received(IN, mouse, 1, t.current))
    just_(logic.ping_received(IN, cat, 1, t.current))
    emits_(logic.relay_connect_received(IN, cat, mouse), [(RelaySigConnected, IN, cat, mouse)])
    return t, logic


def test_relay_send_message(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    (t, logic), msg_bytes = test_relay_successful_connect_request(t, logic, cat=cat, mouse=mouse), random_bytes()
    emits_(logic.relay_send_received(cat, mouse, msg_bytes), [(RelayForward, IN, mouse, cat, msg_bytes)])


def test_relayee_disconnects(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = test_relay_successful_connect_request(t, logic, cat=cat, mouse=mouse)
    just_(logic.ping_received(IN, cat, 2, t.current + logic.heartbeat_max_silence / 2.0))
    emits_(logic.sig_disconnect_received(mouse), [(RelaySigNodeDown, IN, cat, mouse), (NodeDown, mouse)])


def test_relayee_disappears(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = test_relay_successful_connect_request(t, logic, cat=cat, mouse=mouse)
    just_(logic.ping_received(IN, cat, 2, t.current + logic.heartbeat_max_silence / 2.0))
    emits_(logic.heartbeat(t=t.advance(logic.heartbeat_max_silence)), [(RelaySigNodeDown, IN, cat, mouse), (Disconnect, mouse), (NodeDown, mouse), (Ping, IN, cat, 3), (NextBeat, 1.0)])


def test_after_requestor_disconnects_relayee_disconnect_does_nothing(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = test_relay_successful_connect_request(t, logic, cat=cat, mouse=mouse)
    just_(logic.sig_disconnect_received(cat))
    emits_not_(logic.sig_disconnect_received(mouse), [(RelaySigNodeDown, IN, cat, mouse)])


def test_after_requestor_disappears_relayee_disconnect_does_nothing(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = test_relay_successful_connect_request(t, logic, cat=cat, mouse=mouse)
    just_(logic.ping_received(IN, mouse, 2, t.current + logic.heartbeat_max_silence / 2.0))
    just_(logic.heartbeat(t=t.advance(logic.heartbeat_max_silence)))
    emits_not_(logic.sig_disconnect_received(mouse), [(RelaySigNodeDown, IN, cat, mouse)])


def test_after_requestor_disconnects_relayee_disappearance_does_nothing(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = test_relay_successful_connect_request(t, logic, cat=cat, mouse=mouse)
    just_(logic.sig_disconnect_received(cat))
    emits_not_(logic.heartbeat(t=t.advance(logic.heartbeat_max_silence)), [(RelaySigNodeDown, IN, cat, mouse)])


def test_after_requestor_disappears_relayee_disappearance_does_nothing(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = test_relay_successful_connect_request(t, logic, cat=cat, mouse=mouse)
    just_(logic.ping_received(IN, mouse, 2, t.current + logic.heartbeat_max_silence / 2.0))
    just_(logic.heartbeat(t=t.advance(logic.heartbeat_max_silence)))
    emits_not_(logic.heartbeat(t=t.advance(logic.heartbeat_max_silence)), [(RelaySigNodeDown, IN, cat, mouse)])


# nvm

def test_after_nvm_relayee_disconnect_does_nothing(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = test_relay_successful_connect_request(t, logic, cat=cat)
    just_(logic.relay_nvm_received(cat, mouse))
    emits_not_(logic.sig_disconnect_received(mouse), [(RelaySigNodeDown, IN, cat, mouse)])


def test_after_nvm_relayee_disappearance_does_nothing(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321')):
    t, logic = test_relay_successful_connect_request(t, logic, cat=cat, mouse=mouse)
    just_(logic.relay_nvm_received(cat, mouse))
    emits_not_(logic.heartbeat(t=t.advance(logic.heartbeat_max_silence)), [(RelaySigNodeDown, IN, cat, mouse)])


#

def test_requestor_is_on_outsocket_also_works_for_relayed_connects(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321'), bigcat=NID('bigcat:321')):
    t, logic = test_successful_connect(t, logic, nid=bigcat)
    just_(logic.ping_received(IN, mouse, 1, t.current))
    emits_(logic.relay_connect_received(OUT, bigcat, mouse), [(RelaySigConnected, OUT, bigcat, mouse)])
    return t, logic


def test_requestor_is_on_outsocket_also_works_for_relayed_disconnects(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321'), bigcat=NID('bigcat:321')):
    t, logic = test_requestor_is_on_outsocket_also_works_for_relayed_connects(t, logic, cat=cat, mouse=mouse, bigcat=bigcat)
    emits_(logic.sig_disconnect_received(mouse), [(RelaySigNodeDown, IN, bigcat, mouse), (NodeDown, mouse)])


def test_requestor_is_on_outsocket_also_works_for_relayed_sends(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321'), bigcat=NID('bigcat:321')):
    (t, logic), msg_bytes = test_requestor_is_on_outsocket_also_works_for_relayed_connects(t, logic, cat=cat, mouse=mouse, bigcat=bigcat), random_bytes()
    emits_(logic.relay_send_received(bigcat, mouse, msg_bytes), [(RelayForward, IN, mouse, bigcat, msg_bytes)])


def test_requestor_is_also_relayee(t=Time, logic=DEFAULT_LOGIC, cat=NID('cat:321'), mouse=NID('mouse:321'), lion=NID('lion:321')):
    # checks whether clean ups are done properly if a single node is both a requestor as well as a relayee
    t, logic = t(), logic()
    for x in [mouse, cat, lion]:
        emits_(logic.ping_received(IN, x, 1, t.current), [(Ping, IN, x, ANY)])
    just_(logic.relay_connect_received(IN, cat, mouse))
    just_(logic.relay_connect_received(IN, lion, cat))
    just_(logic.sig_disconnect_received(cat))
    emits_not_(logic.sig_disconnect_received(mouse), [(RelaySigNodeDown, IN, cat, mouse)])
    just_(logic.ping_received(IN, cat, 1, t.current))  # cat reconnects
    emits_not_(logic.sig_disconnect_received(cat), [(RelaySigNodeDown, IN, lion, cat)])


# relay propagation

def test_relay_propagates_on_incoming_connection(t=Time, logic=RELAY_LOGIC, mouse=NID('mouse:123')):
    t, logic = t(), logic()
    emits_(logic.ping_received(IN, mouse, 1, t.current), [(Ping, IN, mouse, ANY), (RelaySigNew, IN, mouse)])


def test_relay_propagates_on_incoming_message(t=Time, logic=RELAY_LOGIC, mouse=NID('mouse:123')):
    t, logic, msg = t(), logic(), object()
    emits_(logic.message_received(IN, mouse, 1, msg, t.current), [(RelaySigNew, IN, mouse), (Ping, IN, mouse, ANY), (Receive, mouse, msg)])


def test_relay_propagates_on_outgoing_connection(t=Time, logic=RELAY_LOGIC, mouse=NID('mouse:123')):
    t, logic, msg = t(), logic(), object()
    emits_(logic.ensure_connected(mouse, t.current), [(RelaySigNew, OUT, mouse), (Connect, nid2addr(mouse)), (Ping, OUT, mouse, ANY)])


def test_relay_propagates_on_outgoing_message(t=Time, logic=RELAY_LOGIC, mouse=NID('mouse:123')):
    t, logic, msg = t(), logic(), object()
    emits_(logic.send_message(mouse, msg, t.current), [(RelaySigNew, OUT, mouse), (Connect, nid2addr(mouse)), (Ping, OUT, mouse, ANY)])


# relayers

def test_relay_is_tried_on_bad_connect(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    t, logic, msg = t(), logic(), object()
    just_(logic.new_relay_received(bear))
    emits_(logic.ensure_connected(mouse, t.current), [(Connect, nid2addr(mouse)), (Ping, OUT, mouse, ANY)])
    emits_(logic.heartbeat(t.advance(logic.heartbeat_max_silence)), [(RelayConnect, OUT, bear, mouse), (Disconnect, nid2addr(mouse)), (NextBeat, 1.0)])
    return t, logic


def test_relay_is_tried_on_bad_send(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    t, logic, msg = t(), logic(), object()
    just_(logic.new_relay_received(bear))
    emits_(logic.send_message(mouse, msg, t.current), [(Connect, nid2addr(mouse)), (Ping, OUT, mouse, ANY)])
    emits_(logic.heartbeat(t.advance(logic.heartbeat_max_silence)), [(RelayConnect, OUT, bear, mouse), (Disconnect, nid2addr(mouse)), (NextBeat, 1.0)])
    return t, logic, msg


def test_relay_is_tried_on_connects_immediately_after_connect(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    t, logic = test_relay_is_tried_on_bad_connect(t, logic, bear=bear, mouse=mouse)
    emits_(logic.ensure_connected(mouse, t.advance(1.0)), [])


def test_relay_is_tried_on_sends_immediately_after_connect(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    t, logic = test_relay_is_tried_on_bad_connect(t, logic, bear=bear, mouse=mouse)
    emits_(logic.send_message(mouse, object(), t.current), [])  # queued


def test_relay_is_tried_on_subsequent_connects_after_send(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    t, logic, _ = test_relay_is_tried_on_bad_send(t, logic, bear=bear, mouse=mouse)
    emits_(logic.ensure_connected(mouse, t.advance(1.0)), [])


def test_relay_is_tried_on_subsequent_sends_after_send(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    t, logic, _ = test_relay_is_tried_on_bad_send(t, logic, bear=bear, mouse=mouse)
    emits_(logic.send_message(mouse, object(), t.current), [])


def test_connect_fails_if_relayed_connect_fails(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    t, logic = test_relay_is_tried_on_bad_connect(t, logic, bear=bear, mouse=mouse)
    emits_(logic.relay_nodedown_received(bear, mouse), [(NodeDown, mouse)])
    return t, logic


def test_send_fails_if_relayed_connect_fails(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    t, logic, msg = test_relay_is_tried_on_bad_send(t, logic, bear=bear, mouse=mouse)
    emits_(logic.relay_nodedown_received(bear, mouse), [(SendFailed, msg), (NodeDown, mouse)])


# heuristically, we won't test all combinations of send|connect->send|connect here
def test_relay_connect_is_retried_after_failed_connects(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    t, logic = test_connect_fails_if_relayed_connect_fails(t, logic, bear=bear, mouse=mouse)
    emits_(logic.ensure_connected(mouse, t.current), [(Connect, nid2addr(mouse)), (Ping, OUT, mouse, ANY)])
    emits_(logic.heartbeat(t.advance(logic.heartbeat_max_silence)), [(RelayConnect, OUT, bear, mouse), (Disconnect, nid2addr(mouse)), (NextBeat, 1.0)])


def test_after_successful_relayed_connect_subsequent_connects_do_nothing(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    t, logic = test_relay_is_tried_on_bad_connect(t, logic, bear=bear, mouse=mouse)
    emits_(logic.relay_connected_received(mouse), [])
    emits_(logic.ensure_connected(mouse, t.current), [])


def test_after_successful_relayed_connect_queue_is_sent_and_subsequent_sends_do_a_relayed_send(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    (t, logic, msg), msg2 = test_relay_is_tried_on_bad_send(t, logic, bear=bear, mouse=mouse), object()
    emits_(logic.relay_connected_received(mouse), [(RelaySend, OUT, bear, mouse, msg)])
    emits_(logic.send_message(mouse, msg2, t.current), [(RelaySend, OUT, bear, mouse, msg2)])


# relayees

def test_relayed_message_received(t=Time, logic=DEFAULT_LOGIC, mouse=NID('mouse:456')):
    t, logic, msg = t(), logic(), object()
    emits_(logic.relay_forwarded_received(mouse, msg), [(Receive, mouse, msg)])
    return t, logic


# relay abandonment

def test_direct_connection_is_tried_even_for_messages_arriving_thru_relay(t=Time, logic=DEFAULT_LOGIC, mouse=NID('mouse:123'), cat=NID('cat:123')):
    # this is kind of trivial to determine by looking at the code, but for completeness' sake, and to build up the
    # demonstration of some more advanced features
    (t, logic), msg = test_relayed_message_received(t, logic, mouse=mouse), object()
    emits_(logic.send_message(cat, msg, t.current), [(Connect, nid2addr(cat)), (Ping, OUT, cat, ANY)])


def test_relay_is_abandoned_if_other_side_connects_directly(t=Time, logic=DEFAULT_LOGIC, bear=NID('bear:987'), mouse=NID('mouse:123')):
    (t, logic, msg), msg2 = test_relay_is_tried_on_bad_send(t, logic, bear=bear, mouse=mouse), object()
    emits_(logic.relay_connected_received(mouse), [(RelaySend, OUT, bear, mouse, msg)])
    emits_(logic.ping_received(IN, mouse, 1, t.current), [(RelayNvm, OUT, bear, mouse), (Ping, IN, mouse, 1)])


# relay merging

def test_node_chooses_the_same_relay_for_outgoing_as_for_incoming():
    pass


def test_2_nodes_talking_over_different_relays_choose_one_and_the_same_relay_and_abandon_the_other():
    pass


#

def emits_(gen, expected_actions):
    assert isinstance(expected_actions, list)
    actions = list(flatten(gen))
    if sorted(actions) != sorted(expected_actions):
        raise AssertionError("actions did not match:\n\tgot: %r\n\texp: %r" % (actions, expected_actions))


def emits_not_(gen, unwanted_actions):
    assert isinstance(unwanted_actions, list)
    actions = list(flatten(gen))
    ok_(not any(any(x == y for y in actions) for x in unwanted_actions), "actions contained the unwanted %r" % (list(set(unwanted_actions).intersection(set(actions))),))


def just_(gen):
    if gen is None:
        return
    for x in flatten(gen):
        pass


wrap_globals(globals())
