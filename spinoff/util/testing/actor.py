from __future__ import print_function

import functools
import gc
import inspect
import sys
import traceback
from contextlib import contextmanager

from gevent import idle, with_timeout, Timeout, sleep
from nose.tools import ok_, eq_
from twisted.internet.defer import DebugInfo

from spinoff.actor import Actor, Node
from spinoff.actor.events import Events, ErrorIgnored, UnhandledError
from spinoff.actor.exceptions import WrappingException
from spinoff.remoting import HubWithNoRemoting
from spinoff.util.async import _process_idle_calls

from .common import assert_raises


class MockActor(Actor):
    def __init__(self, messages):
        self.receive = messages.append


@contextmanager
def expect_failure(exc, message=None, timeout=None):
    with assert_raises(exc, message=message) as basket:
        err = Events.consume_one(UnhandledError)
        yield basket
        try:
            _, exc, tb = err.get(timeout=timeout)
        except Timeout:
            pass
        else:
            raise exc, None, tb


@contextmanager
def expect_one_event(ev, timeout=None):
    result = Events.consume_one(type(ev) if not isinstance(ev, type) else ev)
    yield
    try:
        result = result.get(timeout=timeout)
    except Timeout:
        ok_(False,
            "Event %r should have been emitted but was not" % (ev,)
            if not isinstance(ev, type) else
            "Event of type %s should have been emitted but was not" % (ev.__name__,))
    if isinstance(ev, type):
        ok_(isinstance(result, ev), "Event of type %s.%s should have been emitted but was not" % (ev.__module__, ev.__name__))
    else:
        eq_(result, ev, "Event %r should have been emitted but %s was" % (ev, result))


@contextmanager
def expect_event_not_emitted(ev, during=0.001):
    result = Events.consume_one(type(ev) if not isinstance(ev, type) else ev)
    yield
    sleep(during)
    ok_(not result.ready() or (not isinstance(result.get(), ev) if isinstance(ev, type) else result.get() != ev),
        "Event %s should not have been emitted" % (" of type %s" % (ev.__name__,) if isinstance(ev, type) else ev,))


class ErrorCollector(object):
    ERROR_EVENTS = [UnhandledError, ErrorIgnored]

    stack = []

    def __init__(self):
        self.errors = []

    @classmethod
    def collect(cls, event):
        cls.stack[-1].on_event(event)

    @classmethod
    def subscribe(cls):
        for event_type in ErrorCollector.ERROR_EVENTS:
            Events.subscribe(event_type, ErrorCollector.collect)

    @classmethod
    def unsubscribe(cls):
        for event_type in ErrorCollector.ERROR_EVENTS:
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
    node = Node('foo:123')

    class MockException(Exception):
        pass

    message_received = [False]

    class MyActor(Actor):
        def receive(self, _):
            message_received[0] = True
            raise MockException

    try:
        a = node.spawn(MyActor)
        with ErrorCollector():  # emulate a real actor test case
            with assert_raises(MockException):
                with ErrorCollector():
                    a << None
                    idle()
                    assert message_received[0]
    finally:
        node.stop()


def wrap_globals(globals):
    """Ensures that errors in actors during tests don't go unnoticed."""

    def wrap(fn):
        @functools.wraps(fn)
        def ret(*args, **kwargs):
            # dbg("\n============================================\n")

            import spinoff.actor._actor
            spinoff.actor._actor.TESTING = True

            # TODO: once the above TODO (fresh Node for each test fn) is complete, consider making Events non-global by
            # having each Node have its own Events instance.
            Events.reset()

            def check_memleaks():
                if '__pypy__' not in sys.builtin_module_names:
                    gc.collect()
                    for trash in gc.garbage[:]:
                        if isinstance(trash, DebugInfo):
                            # dbg("DEBUGINFO: __del__")
                            if trash.failResult is not None:
                                exc = Unclean(repr(trash.failResult.value) + '\n' + str(trash._getDebugTracebacks()))
                                trash.__dict__.clear()
                                raise exc
                            gc.garbage.remove(trash)

                    assert not gc.garbage, "Memory leak detected: %r" % (gc.garbage,)

                    # if gc.garbage:
                    #     dbg("GARGABE: detected after %s:" % (fn.__name__,), len(gc.garbage))
                    #     import objgraph as ob
                    #     import os

                    #     def dump_chain(g_):
                    #         isframework = lambda x: type(x).__module__.startswith(spinoff.__name__)
                    #         ob.show_backrefs([g_], filename='backrefs.png', max_depth=100, highlight=isframework)

                    #     for gen in gc.garbage:
                    #         dump_chain(gen)
                    #         dbg("   TESTWRAP: mem-debuggin", gen)
                    #         import pdb; pdb.set_trace()
                    #         os.remove('backrefs.png')

            try:
                with ErrorCollector():
                    ret = with_timeout(fn.timeout if hasattr(fn, 'timeout') else 0.5, fn, *args, **kwargs)
                    idle()
                return ret
            finally:
                _process_idle_calls()
                check_memleaks()
        return ret

    for name, value in globals.items():
        if name.startswith('test_') and callable(value) and not inspect.isgeneratorfunction(value):
            globals[name] = wrap(value)


def TestNode():
    return Node(hub=HubWithNoRemoting())
