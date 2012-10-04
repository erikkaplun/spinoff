from __future__ import print_function

import sys
import traceback
from collections import namedtuple

from twisted.internet.defer import Deferred
from spinoff.util.logging import err, log, fail


def fields(*args):
    return namedtuple('_', args)


class Event(object):
    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, self.repr_args())

    def repr_args(self):  # pragma: no cover
        return '%r' % (self.actor,)


class Message(Event, fields('message')):
    def __repr__(self):  # pragma: no cover
        return self.message


class MessageReceived(Event, fields('actor', 'message')):
    def repr_args(self):  # pragma: no cover
        return (super(MessageReceived, self).repr_args() +
                ', message=%r' % (self.message,))


class UnhandledMessage(MessageReceived, fields('actor', 'message')):
    pass


class DeadLetter(Event, fields('actor', 'message')):
    def repr_args(self):
        return (super(DeadLetter, self).repr_args() + (', %r' % (self.message, )))


class RemoteDeadLetter(Event, fields('actor', 'message', 'sender_addr')):
    def repr_args(self):
        return (super(RemoteDeadLetter, self).repr_args() + (', %r, from=%s' % (self.message, self.sender_addr)))


class LifecycleEvent(Event):
    pass


class Started(LifecycleEvent, fields('actor')):
    pass


class Error(LifecycleEvent, fields('actor', 'exc', 'tb')):
    """Logged by actors as they run into errors.

    This is done before the error is reported to the supervisor so even handled errors are logged this way.

    """
    def repr_args(self):  # pragma: no cover
        try:
            formatted_traceback = '\n' + traceback.format_exception(self.exc, None, self.tb)
        except Exception:
            formatted_traceback = ', %r, %r' % (self.exc, self.tb)  # to support passing pattern matchers as event args
        return super(Error, self).repr_args() + formatted_traceback


class UnhandledError(Error, fields('actor', 'exc', 'tb')):
    """Logged by the System actor in the case of errors coming from top-level actors.

    Logged in addition to a regular `Error` event.

    """


class ErrorIgnored(Error, fields('actor', 'exc', 'tb')):
    """Logged whenever there is an exception in an actor but this exception cannot be reported.

    The causes for this event is either an exception in Actor.post_stop or an exception when the actor is being
    stopped anyway.

    """


class ErrorReportingFailure(Error, fields('actor', 'exc', 'tb')):
    """Logged when reporting an exception fails (due to what might be a bug in the framework)."""


class _SupressedBase(LifecycleEvent):
    """Internal base class that implements the logic shared by the Suspended and Terminated events."""
    def __new__(cls, actor, reason=None):
        return super(_SupressedBase, cls).__new__(cls, actor, reason)

    def repr_args(self):
        return (super(_SupressedBase, self).repr_args() +
                (', reason=%r' % (self.reason, ) if self.reason else ''))


class Suspended(_SupressedBase, fields('actor', 'reason')):
    pass


class Resumed(LifecycleEvent, fields('actor')):
    pass


class Terminated(_SupressedBase, fields('actor', 'reason')):
    pass


class TopLevelActorTerminated(Terminated, fields('actor', 'reason')):
    pass


class HighWaterMarkReached(Event, fields('actor', 'count')):
    pass


class Events(object):
    # TODO: add {event type} + {actor / actor path} based subscriptions.

    subscriptions = {}
    consumers = {}

    def log(self, event, log_caller=False):
        try:
            (fail if isinstance(event, Error) else log)(event, caller=log_caller)

            consumers = self.consumers.get(type(event))
            if consumers:
                consumer_d = consumers.pop(0)
                consumer_d.callback(event)
                return

            subscriptions = self.subscriptions.get(type(event))
            if subscriptions:
                for fn in subscriptions:
                    try:
                        fn(event)
                    except Exception:  # pragma: no cover
                        err("Error in event handler:\n", traceback.format_exc())
        except Exception:  # pragma: no cover
            print("Events.log failed:\n", traceback.format_exc(), file=sys.stderr)

    def subscribe(self, event_type, fn):
        self.subscriptions.setdefault(event_type, []).append(fn)

    def unsubscribe(self, event_type, fn):
        subscribers = self.subscriptions.get(event_type, [])
        if fn in subscribers:
            subscribers.remove(fn)

    def consume_one(self, event_type):
        assert isinstance(event_type, type)
        c = self.consumers
        d = Deferred(lambda _: c[event_type].remove(d))
        c.setdefault(event_type, []).append(d)
        return d

    def reset(self):
        self.subscriptions = {}
        self.consumers = {}

    def __repr__(self):
        return "<Events>"
Events = Events()
