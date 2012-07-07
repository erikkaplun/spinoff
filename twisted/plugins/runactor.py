import sys

from zope.interface import implements
from twisted.python import usage, failure
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from unnamedframework.actor import ActorRunner


class Options(usage.Options):

    optParameters = [
        ['actor', 'a', None, 'The actor to spawn.'],
        ]


class ActorRunnerMaker(object):
    implements(IServiceMaker, IPlugin)

    tapname = 'runactor'
    description = 'Runs an actor'
    options = Options

    def makeService(self, options):
        actor = options['actor']
        if not actor:
            print >> sys.stderr, "error: no actor specified"
            sys.exit(1)

        try:
            module_path, actor_cls_name = actor.rsplit('.', 1)
        except ValueError:
            print >> sys.stderr, "error: bad path to actor %s" % actor
            sys.exit(1)

        try:
            mod = __import__(module_path, globals(), locals(), [actor_cls_name], -1)
        except ImportError:
            print >> sys.stderr, "error: could not import %s:" % actor
            failure.Failure().printTraceback(file=sys.stderr)
            sys.exit(1)

        actor_cls = getattr(mod, actor_cls_name)

        return ActorRunner(actor_cls)


serviceMaker = ActorRunnerMaker()
