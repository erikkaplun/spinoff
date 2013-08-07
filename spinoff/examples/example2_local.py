from spinoff.actor import Actor
from spinoff.util.logging import dbg

from .example2 import ExampleProcess, ExampleActor


class LocalApp(Actor):
    def run(self):
        dbg("spawning ExampleActor")
        actor1 = self.spawn(ExampleActor)

        dbg("spawning ExampleProcess")
        self.spawn(ExampleProcess.using(other_actor=actor1))

        self.get()  # so that the entire app wouldn't exit immediately
