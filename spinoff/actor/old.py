    # def consume(self):
    #     return self.priority_inbox.popleft() if self.priority_inbox else self.inbox.popleft()

    # def peek(self):
    #     return self.priority_inbox[0] if self.priority_inbox else self.inbox[0] if self.inbox else None

    # def receive(self, message):
        # if self.shutting_down:
        #     # the shutting_down procedure is waiting for all children to terminate so we make an exception here
        #     # and handle the message directly, bypassing the standard message handling logic:
        #     # NB! DO NOT do this with a running actor--it changes the observable state of the actor
        #     if message == ('_child_terminated', ANY):
        #         _, child = message
        #         self._do_child_terminated(child)
        #     # don't care about any system message if we're already stopping:
        #     elif message not in STATE_CHANGING_MESSAGES:
        #         # so that it could be sent to dead letters when the stopping is complete:
        #         self.inbox.append(message)
        #     # XXX: untested
        #     elif self.priority_inbox:
        #         self.priority_inbox.append(message)
        #     return

        # # '_watched' is something that is safe to handle immediately as it doesn't change the visible state of the actor;
        # # NB: DO NOT do the same with '_suspend', '_resume' or any other message that changes the observable state of the actor!
        # if message == ('_watched', ANY):
        #     self._watched(message[1])
        # elif message == ('_unwatched', ANY):
        #     self._unwatched(message[1])
        # elif ('terminated', ANY) == message and not (self.watchees and message[1] in self.watchees):
        #     pass  # ignore unwanted termination message
        # elif message == ('_node_down', ANY):
        #     self._node_down(message[1])
        # # ...except in case of an ongoing receive in which case the suspend-resume event will be seem atomic to the actor
        # else:
        #     (self.priority_inbox if message in STATE_CHANGING_MESSAGES else self.inbox).append(message)

    # @logstring(u'↻')
    # def process_messages(self):
    #     while True:
    #         if not self.peek():
    #             self._msg_available.clear()
    #             self._msg_available.wait()
    #         next_message = self.peek()
    #         is_startstop = next_message in ('_start', '_stop')
    #         is_untaint = next_message in ('_resume', '_restart')
    #         if not self.processing_messages and (self.started or is_startstop or self.tainted and is_untaint):
    #             # XXX: this method can be called after the actor is stopped--add idle call cancelling
    #             while not self.stopped and (not self.shutting_down) and self.peek() and (not self.suspended or self.peek() in ('_stop', '_restart', '_resume', '_suspend')) or (self.shutting_down and ('_child_terminated', ANY) == self.peek()):
    #                 message = self.consume()
    #                 try:
    #                     self._process_one_message(message)
    #                 except Exception:
    #                     self.report()

    # @logstring(u"↻ ↻ ↻")
    # def _process_one_message(self, message):
    #     if '_start' == message:
    #         self._do_start()
    #     elif ('_error', ANY, ANY, ANY) == message:
    #         _, sender, exc, tb = message
    #         self.supervise(sender, exc, tb)
    #     elif '_stop' == message:
    #         self._do_stop()
    #     elif '_restart' == message:
    #         self._do_restart()
    #     elif '_resume' == message:
    #         self._do_resume()
    #     elif '_suspend' == message:
    #         self._do_suspend()
    #     elif ('_child_terminated', ANY) == message:
    #         _, child = message
    #         self._do_child_terminated(child)
    #     else:
    #         if ('terminated', ANY) == message:
    #             _, watchee = message
    #             # XXX: it's possible to have two termination messages in the queue; this will be solved when actors of
    #             # nodes going down do not send individual messages but instead the node sends a single 'going-down' message:
    #             if watchee not in self.watchees:
    #                 return
    #             self.watchees.remove(watchee)
    #             self._unwatch(watchee, silent=True)

    #         receive = self.impl.receive
    #         try:
    #             self.ongoing = gevent.spawn(receive, message)
    #             self.ongoing.join()
    #             del self.ongoing
    #         except Unhandled:
    #             self.unhandled(message)
    #         except Exception:
    #             raise

    # @logstring(u"||")
    # def _do_suspend(self):
    #     self.suspended = True
    #     for child in self.children:
    #         child.send('_suspend')

    # @logstring(u"||►")
    # def _do_resume(self):
    #     self.suspended = False
    #     for child in self.children:
    #         child.send('_resume')

    # @logstring("_stop:")
    # def _do_stop(self):
    #     if self.ongoing:
    #         self.ongoing.kill()
    #         del self.ongoing
    #     self._shutdown()
    #     ref = self.ref
    #     # TODO: test that system messages are not deadlettered
    #     for message in self.inbox:
    #         if ('_error', ANY, ANY, ANY) == message:
    #             _, sender, exc, tb = message
    #             Events.log(ErrorIgnored(sender, exc, tb))
    #         elif ('_watched', ANY) == message:
    #             _, watcher = message
    #             watcher.send(('terminated', ref))
    #         elif (IN(['terminated', '_unwatched']), ANY) != message:
    #             Events.log(DeadLetter(ref, message))
    #     assert not self.impl
    #     del self.inbox
    #     del self.priority_inbox  # don't want no more, just release the memory
    #     del ref._cell
    #     self.stopped = True
    #     # XXX: which order should the following two operations be done?
    #     self.parent.send(('_child_terminated', ref))
    #     if self.watchers:
    #         for watcher in self.watchers:
    #             watcher.send(('terminated', ref))

    # @logstring("shutdown:")
    # def _shutdown(self):
    #     self.shutting_down = True
    #     if self._children:  # we don't want to do the Deferred magic if there're no babies
    #         self._all_children_stopped = gevent.event.Event()
    #         for child in self.children:
    #             child._stop()
    #         self._all_children_stopped.wait()
    #     while self.watchees:
    #         self._unwatch(self.watchees.pop())
    #     if hasattr(self.impl, 'post_stop'):
    #         try:
    #             self.impl.post_stop()
    #         except Exception:
    #             _ignore_error(self.impl)
    #     if hasattr(self.impl, '_coroutine'):
    #         try:
    #             self.impl._Process__shutdown()
    #         except Exception:
    #             _ignore_error(self.impl)
    #     self.impl = None
    #     self.shutting_down = False
