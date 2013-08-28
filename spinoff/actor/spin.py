import argparse
import sys

from gevent.event import Event

from spinoff.actor import Actor, Node
from spinoff.actor.exceptions import Unhandled
from spinoff.util.logging import err


_EMPTY = object()


class Wrapper(Actor):
    def __init__(self, actor_factory, spawn_at, keep_running, initial_messages, stop_event):
        self.actor_factory = actor_factory
        self.spawn_at = spawn_at
        self.keep_running = keep_running
        self.initial_messages = initial_messages
        self.stop_event = stop_event

    def _do_spawn(self):
        self.actor = self.watch(self.node.spawn(self.actor_factory, name=self.spawn_at))

    def pre_start(self):
        self._do_spawn()
        for msg in self.initial_messages:
            self.actor << msg

    def receive(self, message):
        if message == ('terminated', self.actor):
            _, actor = message
            if self.keep_running:
                self.send_later(1.0, '-spawn')
            else:
                self.stop()
        elif message == '-spawn':
            self._do_spawn()
        else:
            raise Unhandled

    def post_stop(self):
        self.stop_event.set()

    def __repr__(self):
        return '#wrapper#'


def spin(actor_cls, name=None, init_params={}, node_id=None, initial_messages=[], keep_running=False, enable_relay=False, wrapper_cls=Wrapper):
    node = Node(nid=node_id, enable_remoting=True if node_id else False, enable_relay=enable_relay)
    stop_event = Event()
    node.spawn(wrapper_cls.using(
        actor_cls.using(**init_params),
        spawn_at=name, keep_running=keep_running,
        initial_messages=initial_messages,
        stop_event=stop_event
    ), name='-runner')
    try:
        stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()


def console():
    class _ImportFailed(Exception):
        pass

    class _InitError(Exception):
        pass

    argparser = argparse.ArgumentParser(
        description="Runs the specified actor",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    argparser.add_argument('actor', metavar='ACTOR',
                           help="The actor to run")
    argparser.add_argument('-init', '--init-params', metavar='INIT', default={},
                           help="Parameters to pass to the actor")
    argparser.add_argument('-msg', '--initial-message', metavar='MSG', nargs='*', default=[],
                           help="Messages to send to the actor. Specify multiple times to send multiple messages. These must be valid Python expressions")
    argparser.add_argument('-keep', '--keep-running', action='store_true',
                           help="Whether to respawn the actor when it terminates")
    argparser.add_argument('-name', '--name', metavar='NAME',
                           help="What to name the actor")
    argparser.add_argument('-nid', '--node-id', metavar='NODEID',
                           help="What to name the actor")
    argparser.add_argument('-relay', '--enable-relay', metavar='RELAY',
                           help="What to name the actor")

    args = argparser.parse_args()

    try:
        if '.' not in args.actor:
            raise _InitError("Failed to import %s" % (args.actor,))
            sys.exit(1)
        else:
            mod_name, cls_name = args.actor.rsplit('.', 1)
            mod = __import__(mod_name, fromlist=[cls_name])
            actor_cls = getattr(mod, cls_name)

        eval_str = 'dict(%s)' % (args.init_params,)
        try:
            init_params = eval(eval_str)
        except Exception:
            raise _InitError("Failed to parse value: -init/--init-params")

        initial_messages = []
        for msg_raw in args.initial_message:
            try:
                msg = eval(msg_raw)
            except Exception:
                quote = '\'' if '"' in msg_raw else '"'
                raise _InitError("Failde to parse value: -msg %s%s%s" % (quote, msg_raw, quote))
            else:
                initial_messages.append(msg)
    except _InitError as exc:
        err(exc.args[0])
        sys.exit(exc.args[1] if len(exc.args) > 1 else 1)

    spin(actor_cls, name=args.name, init_params=init_params, node_id=args.node_id,
         initial_messages=initial_messages, keep_running=args.keep_running, enable_relay=args.enable_relay)
