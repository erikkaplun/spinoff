Overview
========

Spinoff is a framework for writing distributed, fault tolerant and scalable applications in Python.
Spinoff is based on the `Actor Model <http://en.wikipedia.org/wiki/Actor_model>`_ and borrows from `Akka <http://akka.io>`_ (location transparency, actor references, etc)
and `Erlang <http://en.wikipedia.org/wiki/Erlang_(programming_language)>`_ (processes, ``nodename@hosthost`` style node references (not implemented yet)).

Spinoff has been built using `Twisted <twistedmatrix.com>`_ as the underlying framework and `ZeroMQ <http://www.zeromq.org/>`_ (via ``pyzmq`` and a fork of ``txzmq``) for remoting.

Spinoff is currently under continuous development but is nevertheless usable for writing real applications—its fault tolerance features also protect it against bugs in its own code.


Example
=======

The following is only a very small "peek preview" style example of what the framework can do. More examples and full documentation will follow soon.

::

    # spinoff/examples/example1.py

    from spinoff.actor import Actor
    from spinoff.actor.process import Process
    from spinoff.util.logging import dbg
    from spinoff.util.async import sleep, with_timeout


    class ExampleProcess(Process):
        def run(self):
            child = self.spawn(ExampleActor)

            while True:
                dbg("sending greeting to %r" % (child,))
                child << ('hello!', self.ref)

                dbg("waiting for ack from %r" % (child,))
                yield with_timeout(5.0, self.get('ack'))

                dbg("got 'ack' from %r; now sleeping a bit..." % (child,))
                yield sleep(1.0)


    class ExampleActor(Actor):
        def pre_start(self):
            dbg("starting")

        def receive(self, msg):
            content, sender = msg
            dbg("%r from %r" % (content, sender))
            sender << 'ack'

        def post_stop(self):
            dbg("stopping")


The example can be run using the following command:

::

    twistd --nodaemon startnode --actor spinoff.examples.example1.ExampleProcess
