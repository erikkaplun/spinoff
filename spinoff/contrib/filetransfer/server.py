import datetime
import os

from gevent.threadpool import ThreadPool

from spinoff.actor import Actor
from spinoff.actor.context import get_context
from spinoff.util.logging import dbg, err
from spinoff.util.pattern_matching import ANY, IN
from spinoff.contrib.filetransfer.response import Response
from spinoff.contrib.filetransfer import constants


class Server(Actor):
    _instances = {}

    def pre_start(self):
        self.threadpool = ThreadPool(maxsize=10)
        self.published = {}  # <file_id> => (<local file path>, <time added>)
        self.responses = {}  # <sender> => <file_id>
        self << 'purge-old'

    def receive(self, msg):
        if ('serve', ANY, ANY) == msg:
            _, file_path, file_id = msg
            if not os.path.exists(file_path) and not os.path.isdir(file_path):
                err("attempt to publish a file that does not exist")
            elif file_id in self.published:
                err("Attempt to publish %r with ID %r but a file already exists with that ID" % (file_path, file_id))
            else:
                self.published[file_id] = (file_path, datetime.datetime.now())
        elif msg == ('request', ANY) or msg == ('request-local', ANY):
            request, file_id = msg
            if file_id not in self.published:
                err("attempt to get a file with ID %r which has not been published or is not available anymore" % (file_id,))
            else:
                file_path, time_added = self.published[file_id]
                if request == 'request-local':
                    self._touch_file(file_id)
                    self.reply(('local-file', file_path))
                else:
                    response = self.spawn(Response.using(file=file_path, request=self.sender, threadpool=self.threadpool))
                    self.watch(response)
                    self.responses[response] = file_id
        elif 'purge-old' == msg:
            self.send_later(60, 'purge-old')
            t = datetime.datetime.now()
            for file_id, (file_path, time_added) in self.published.items():
                if (t - time_added).total_seconds() > constants.FILE_MAX_LIFETIME and file_id not in self.responses.values():
                    dbg("purging file %r at %r" % (file_id, file_path))
                    del self.published[file_id]
        elif ('terminated', IN(self.responses)) == msg:
            _, sender = msg
            self._touch_file(file_id=self.responses.pop(sender))

    def _touch_file(self, file_id):
        file_path, time_added = self.published[file_id]
        self.published[file_id] = (file_path, datetime.datetime.now())

    def post_stop(self):
        del self._instances[self.node]
