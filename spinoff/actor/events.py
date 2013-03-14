from __future__ import print_function

import sys
import traceback
from collections import namedtuple

from gevent.event import AsyncResult
from spinoff.util.logging import err, log, fail


def fields(*args):
    return namedtuple('_', args)


class Event(object):
    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, self.repr_args())

    def repr_args(self):  # pragma: no cover
        return '%r' % (self.actor,)


class UnhandledMessage(Event, fields('actor', 'message', 'sender')):
    def repr_args(self):  # pragma: no cover
        r = repr(self.message)
        if len(r) > 200:
            r = r[:200] + '...'
        return (super(UnhandledMessage, self).repr_args() +
                ', message=%s, sender=%r' % (r, self.sender))


class DeadLetter(Event, fields('actor', 'message', 'sender')):
    def repr_args(self):
        r = repr(self.message)
        if len(r) > 200:
            r = r[:200] + '...'
        return (super(DeadLetter, self).repr_args() + (', message=%s, sender=%r' % (r, self.sender)))


class Error(Event, fields('actor', 'exc', 'tb')):
    """Logged by actors as they run into errors."""
    def repr_args(self):  # pragma: no cover
        try:
            formatted_traceback = '\n' + traceback.format_exception(self.exc, None, self.tb)
        except Exception:
            formatted_traceback = ', %r, %r' % (self.exc, self.tb)  # to support passing pattern matchers as event args
        return super(Error, self).repr_args() + formatted_traceback


class Terminated(Event, fields('actor')):
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
                consumer_d.set(event)
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
        assert isinstance(event_type, type) or all(isinstance(x, type) for x in event_type)
        ret = AsyncResult()
        self.consumers.setdefault(event_type, []).append(ret)
        return ret

    def reset(self):
        self.subscriptions = {}
        self.consumers = {}

    def __repr__(self):
        return "<Events>"
Events = Events()
