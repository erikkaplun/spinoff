from __future__ import print_function

import traceback
import sys
from contextlib import contextmanager

from twisted.internet.defer import CancelledError

from spinoff.actor import Actor
from spinoff.actor.events import Events, ErrorIgnored, UnhandledError, ErrorReportingFailure

from .common import deferred_result, assert_raises
from spinoff.actor.exceptions import WrappingException


class MockMessages(list):
    def clear(self):
        ret = self[:]
        self[:] = []
        return ret


class MockActor(Actor):
    def __init__(self, messages):
        self.messages = messages

    def receive(self, message):
        self.messages.append(message)


_ERROR_EVENTS = [UnhandledError, ErrorIgnored, ErrorReportingFailure]


class ErrorCollector(object):
    stack = []

    def __init__(self):
        self.errors = []

    @classmethod
    def collect(cls, event):
        cls.stack[-1].on_event(event)

    @classmethod
    def subscribe(cls):
        for event_type in _ERROR_EVENTS:
            Events.subscribe(event_type, ErrorCollector.collect)

    @classmethod
    def unsubscribe(cls):
        for event_type in _ERROR_EVENTS:
            Events.unsubscribe(event_type, ErrorCollector.collect)

    def on_event(self, event):
        sender, exc, tb = event
        tb_formatted = ''.join(traceback.format_exception(exc, None, tb))
        error_report = 'ACTOR %s, EVENT %s:\n' % (sender, type(event).__name__)
        error_report += self.format_exc(exc, tb)
        self.errors.append((error_report, exc, tb_formatted))

    def format_exc(self, exc, tb):
        tb_formatted = ''.join(traceback.format_exception(exc, None, tb))
        error_report = tb_formatted + '\n' + type(exc).__name__ + (': ' + str(exc.args[0]) if exc.args else '')
        if isinstance(exc, WrappingException):
            fmted = exc.formatted_original_tb()
            indented = '\n'.join('    ' + line for line in fmted.split('\n') if line)
            error_report += '\n' + indented
        return error_report

    def __enter__(self):
        stack = ErrorCollector.stack
        if not stack:
            ErrorCollector.subscribe()
        stack.append(self)

    def __exit__(self, exc_cls, exc, tb):
        stack = ErrorCollector.stack
        assert stack[-1] == self
        stack.pop()
        if not stack:
            ErrorCollector.unsubscribe()

        clean = not exc

        def get_only_reports():
            for report, _, _ in self.errors:
                yield report

        if self.errors:
            # If there are at least 2 errors, or we are in the toplevel collector,
            # dump the errors and raise a general Unclean exception:
            if not stack or not clean or len(self.errors) > 1:
                if clean:
                    if len(self.errors) >= 2:
                        indented_error_reports = ('\n'.join('    ' + line for line in error_report.split('\n') if line)
                                                  for error_report in get_only_reports())
                        indented_entire_error_report = '\n\n'.join(indented_error_reports)
                        raise Unclean("There were errors in top-level actors:\n%s" % (indented_entire_error_report,))
                    else:
                        (_, exc, tb_formatted), = self.errors
                        print(tb_formatted, file=sys.stderr)

                        # XXX: copy-paste
                        if isinstance(exc, WrappingException):
                            fmted = exc.formatted_original_tb()
                            print('\n'.join('    ' + line for line in fmted.split('\n') if line), file=sys.stderr)

                        raise exc
                else:
                    print('\n'.join(get_only_reports()), file=sys.stderr)
            # ...otherwise just re-raise the exception to support assert_raises
            else:
                (_, exc, tb_formatted), = self.errors
                if not stack:
                    print(tb_formatted, file=sys.stderr)

                    # XXX: copy-paste
                    if isinstance(exc, WrappingException):
                        fmted = exc.formatted_original_tb()
                        print('\n'.join('    ' + line for line in fmted.split('\n') if line), file=sys.stderr)

                raise exc

        if not clean:
            # XXX: copy-paset
            if isinstance(exc, WrappingException):
                fmted = exc.formatted_original_tb()
                print('\n'.join('    ' + line for line in fmted.split('\n') if line), file=sys.stderr)

            raise exc_cls, exc, tb


class Unclean(Exception):
    pass


def test_errorcollector_can_be_used_with_assert_raises():
    from spinoff.actor import Node
    from spinoff.actor.remoting import HubWithNoRemoting

    spawn = Node(hub=HubWithNoRemoting()).spawn

    class MockException(Exception):
        pass

    message_received = [False]

    class MyActor(Actor):
        def receive(self, _):
            message_received[0] = True
            raise MockException

    with ErrorCollector():  # emulate a real actor test case
        with assert_raises(MockException):
            with ErrorCollector():
                spawn(MyActor) << None
                assert message_received[0]


@contextmanager
def expect_failure(exc, message=None):
    with assert_raises(exc, message) as basket:
        with ErrorCollector():
            yield basket


class DebugActor(object):
    def __init__(self, name):
        self.name = name

    def receive(self, message):
        print("%s: received %s" % (self.name, message))


@contextmanager
def assert_one_event(ev):
    d = Events.consume_one(type(ev) if not isinstance(ev, type) else ev)
    try:
        yield
    except:
        raise
    else:
        assert d.called, ("Event %r should have been emitted but was not" % (ev,)
                          if not isinstance(ev, type) else
                          "Event of type %s should have been emitted but was not" % (ev.__name__,))
        result = deferred_result(d)
        if isinstance(ev, type):
            assert isinstance(result, ev), "Event of type %s.%s should have been emitted but was not" % (ev.__module__, ev.__name__)
        else:
            assert result == ev, "Event %r should have been emitted but %s was" % (ev, result)
    finally:
        d.addErrback(lambda f: f.trap(CancelledError)).cancel()


@contextmanager
def assert_event_not_emitted(ev):
    d = Events.consume_one(type(ev) if not isinstance(ev, type) else ev)
    try:
        yield
    except:
        raise
    else:
        assert not d.called or deferred_result(d) != ev, \
            "Event %s should not have been emitted" % (
                (" of type %s" % (ev.__name__,)) if isinstance(ev, type) else ev,)
    finally:
        d.addErrback(lambda f: f.trap(CancelledError)).cancel()
