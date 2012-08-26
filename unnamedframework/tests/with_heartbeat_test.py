# from twisted.internet.defer import Deferred
# from twisted.internet.task import Clock
# from twisted.trial import unittest

# from unnamedframework.actor import Process
# from unnamedframework.util.async import sleep
# from unnamedframework.util.async import with_heartbeat
# from unnamedframework.util.testing import MockFunction


# def make_mock(heartbeat_fn, reactor, methods={}, interval=1.0):
#     class Mock(Process):
#         send_heartbeat = staticmethod(heartbeat_fn)

#         for method_name, method_fn in methods.items():
#             locals()[method_name] = with_heartbeat(interval=interval, reactor=reactor)(method_fn)

#     return Mock()


# class WithHeartbeatTest(unittest.TestCase):

#     def test_basic(self):
#         clock = Clock()
#         heartbeat_fn = MockFunction()
#         sleep_time = 4.0

#         def meth(self):
#             yield sleep(sleep_time, reactor=clock)
#         mock = make_mock(heartbeat_fn, clock, {'method': meth})

#         d = mock.method()
#         assert isinstance(d, Deferred)

#         clock.advance(0.0)

#         heartbeat_fn.assert_called("heartbeat should be sent immediately after the first called to a with_heartbeat wrapped method")

#         clock.advance(1.0)
#         heartbeat_fn.assert_called("heartbeat should be sent again after `interval` has passed")

#         clock.advance(1.0)
#         heartbeat_fn.assert_called("heartbeat should be sent again the 2nd time after `interval` has passed")

#         clock.advance(.5)
#         assert not heartbeat_fn.called, \
#             "heartbeat should not be sent if `interval` has not passed yet"

#         clock.advance(.5)
#         heartbeat_fn.assert_called("heartbeat should be sent again the 3rd time after `interval` has passed")

#         # go to the point where the method exits
#         clock.advance(sleep_time - clock.seconds())
#         heartbeat_fn.reset()

#         clock.advance(1.0)
#         assert not heartbeat_fn.called, "heartbeat stops after the wrapped coroutine has exited"

#     def test_exception(self):
#         clock = Clock()
#         heartbeat_fn = MockFunction()

#         def meth(self):
#             yield sleep(1.5, clock)
#             raise Exception()
#         mock = make_mock(heartbeat_fn, clock, {'method': meth})

#         mock.method().addErrback(lambda _: None)

#         clock.advance(0.0)
#         heartbeat_fn.assert_called()

#         clock.advance(1.0)
#         heartbeat_fn.assert_called()

#         clock.pump([0.5, 0.1])
#         assert not heartbeat_fn.called

#     def test_two_code_flows(self):
#         clock = Clock()
#         heartbeat_fn = MockFunction()

#         def meth(self):
#             yield sleep(10.0, clock)
#         mock = make_mock(heartbeat_fn, clock,
#                          {'method1': meth, 'method2': meth})

#         mock.method1()
#         mock.method2()
#         heartbeat_fn.assert_called(1)

#         clock.pump([1.0, 1.0])
#         heartbeat_fn.assert_called(2)

#     def test_two_code_flows_with_exception(self):
#         clock = Clock()
#         heartbeat_fn = MockFunction()

#         def meth1(self):
#             yield sleep(10.0, clock)

#         def meth2(self):
#             yield sleep(2.9, clock)
#             raise Exception()

#         mock = make_mock(heartbeat_fn, clock,
#                          {'method1': meth1, 'method2': meth2})
#         mock.method1()
#         mock.method2().addErrback(lambda _: None)
#         heartbeat_fn.reset()

#         clock.pump([1.0, 1.0])
#         heartbeat_fn.assert_called(2)

#         clock.advance(0.9)
#         assert not heartbeat_fn.called

#         clock.advance(0.1)
#         assert not heartbeat_fn.called
