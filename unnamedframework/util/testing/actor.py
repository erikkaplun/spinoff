from __future__ import print_function

import traceback
from contextlib import contextmanager

from unnamedframework.actor import Actor, spawn
from unnamedframework.actor.events import Events, ErrorIgnored, UnhandledError, SupervisionFailure

from .common import deferred_result, assert_raises


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


def make_mock():
    messages = MockMessages()
    ret = MockActor.spawn(messages)
    return ret, messages


_ERROR_EVENTS = [UnhandledError, ErrorIgnored, SupervisionFailure]


class ErrorCollector(object):
    stack = []

    def __init__(self):
        self.errors = []

    def collect(self, event):
        sender, exc_, tb_ = event
        error_report = 'ACTOR %s, EVENT %s:\n%s' % (sender, type(event).__name__, ''.join(traceback.format_exception(exc_, None, tb_)) + '\n' + type(exc_).__name__ + (': ' + str(exc_.args[0]) if exc_.args else ''))
        self.errors.append(error_report)

    def __enter__(self):
        stack = ErrorCollector.stack

        if stack:
            for event_type in _ERROR_EVENTS:
                Events.unsubscribe(event_type, stack[-1].collect)

        stack.append(self)
        for event_type in _ERROR_EVENTS:
            Events.subscribe(event_type, self.collect)

    def __exit__(self, exc_cls, exc, tb):
        stack = ErrorCollector.stack

        assert stack[-1] == self
        for event_type in _ERROR_EVENTS:
            Events.unsubscribe(event_type, self.collect)
        stack.pop()

        if stack:
            for event_type in _ERROR_EVENTS:
                Events.subscribe(event_type, stack[-1].collect)

        have_primary_exception = exc is not None

        if self.errors:
            # If there are at least 2 errors, or we are in the toplevel collector,
            # dump the errors and raise a general Unclean exception:
            if not stack or have_primary_exception or len(self.errors) >= 2:
                error_reports = []
                for error in self.errors:
                    # sender, exc_, tb_ = error
                    error_reports.append(error)
                # print("error_reports: %r" % (error_reports,), file=sys.stderr)
                if not have_primary_exception:
                    indented_error_reports = ('\n'.join('    ' + line for line in error_report.split('\n') if line)
                                              for error_report in error_reports)
                    indented_entire_error_report = '\n\n'.join(indented_error_reports)
                    raise Unclean("There were errors in top-level actors:\n%s" % indented_entire_error_report)
                else:
                    print('\n'.join(error_reports))
            # ...otherwise just re-raise the exception to support assert_raises
            else:
                error, = self.errors
                raise error[1], None, error[2]
        if have_primary_exception:
            raise exc_cls, exc, tb


class Unclean(Exception):
    pass


def test_errorcollector_can_be_used_with_assert_raises():
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
    d = Events.consume_one(type(ev))
    try:
        yield
    except:
        raise
    else:
        assert d.called, "Event %r should have been emitted but was not" % (ev,)
        result = deferred_result(d)
        assert result == ev, "Event %r should have been emitted but %s was" % (ev, result)
