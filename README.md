Overview
========

Spinoff is a framework for writing distributed, fault tolerant and scalable applications in Python. <a href="http://travis-ci.org/eallik/spinoff" title="Spinoff on Travis-CI.org"><img src="https://secure.travis-ci.org/eallik/spinoff.png?branch=master" alt="&lt;build status indicator on travis-ci.org/eallik/spinoff&gt;"/></a>

Spinoff is based on the [Actor Model](http://en.wikipedia.org/wiki/Actor_model) and borrows from [Akka](http://akka.io) (location transparency, actor references, etc) and [Erlang](http://en.wikipedia.org/wiki/Erlang_(programming_language)) (processes, `nodename@hosthost` style node references (not implemented yet)).

Spinoff has been built using [Twisted](http://twistedmatrix.com/) as the underlying framework and [ZeroMQ](http://www.zeromq.org/) (via `pyzmq` and a fork of `txzmq`) for remoting.

Spinoff is currently under continuous development but is nevertheless usable for writing real applications—its fault tolerance features also protect it against bugs in its own code.


Hype
====

> _"anyway, spinoff is really super nice to play with all this stuff
> because it's cheap to write code in it
> it really shines at making quick prototypes"_

...so let's hope one day there will be a testimonial here saying "it really shines at making production software".

Example
=======

The following is only a very small "peek preview" style example of what the framework can do. More examples and full documentation will follow soon.

```python
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
```

The example can be run using the following command:

```bash
$ twistd --nodaemon startnode --actor spinoff.examples.example1.ExampleProcess
```

or

```bash
$ twistd -n startnode -a spinoff.examples.example1.ExampleProcess
```

Distributed Example (with Remoting)
===================================

```python
# spinoff/examples/example2.py
from spinoff.actor import Actor
from spinoff.actor.process import Process
from spinoff.util.logging import dbg
from spinoff.util.async import sleep, with_timeout


class ExampleProcess(Process):
    def run(self, other_actor):
        other_actor = lookup(other_actor) if isinstance(other_actor, str) else other_actor
        while True:
            dbg("sending greeting to %r" % (other_actor,))
            other_actor << ('hello!', self.ref)

            dbg("waiting for ack from %r" % (other_actor,))
            yield with_timeout(5.0, self.get('ack'))

            dbg("got 'ack' from %r; now sleeping a bit..." % (other_actor,))
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
```

The example can be run using the following commands:

```bash
$ twistd --pidfile node1.pid --nodaemon startnode --remoting localhost:9700 --actor spinoff.examples.example2.ExampleActor --name other
$ twistd --pidfile node2.pid --nodaemon startnode --remoting localhost:9701 --actor spinoff.examples.example2.ExampleProcess --params "other_actor='localhost:9700/other'"
```

or

```bash
$ twistd --pidfile node1.pid -n startnode -r :9700 -a spinoff.examples.example2.ExampleActor -n other
$ twistd --pidfile node2.pid -n startnode -r :9701 -a spinoff.examples.example2.ExampleProcess -i "other_actor='localhost:9700/other'"
```

Same Distributed Code without Remoting
======================================

The following example demonstrates how it's possible to run the same code, unmodified, in a single thread with no network/remoting involved whatsoever.  It's an illustration of how actors (components) written using Spinoff are xagnostic of how they are wired to work with other actors and can thus be viewed as abstract components containing only pure domain logic and no low level transportation and topology details.

```python
# spinoff/examples/example2_local.py
from spinoff.actor.process import Process
from spinoff.util.logging import dbg

from .example2 import ExampleProcess, ExampleActor


class LocalApp(Process):
    def run(self):
        dbg("spawning ExampleActor")
        actor1 = self.spawn(ExampleActor)

        dbg("spawning ExampleProcess")
        self.spawn(ExampleProcess.using(other_actor=actor1))

        yield self.get()  # so that the entire app wouldn't exit immediately
```

The example can be run using the following commands:

```bash
$ twistd --nodaemon startnode --actor spinoff.examples.example2_local.LocalApp
```

or

```bash
$ twistd -n startnode -a spinoff.examples.example2_local.LocalApp
```

One might be tempted to ask, then, what is the difference between remoting frameworks such as CORBA and Spinoff.  The difference is that actors define clear boundaries where remoting could ever be used, as opposed to splitting a flow of tightly coupled logic into two nodes on the network, which, still providing valid output, can degrade in performance significantly.  This is not to say that actors with location transparency suffer none of such issues but the extent to which the problem exists is, arguably, an order of magnitude lower.
