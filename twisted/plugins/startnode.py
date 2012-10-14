from __future__ import print_function

import sys
import traceback

from twisted.application.service import IServiceMaker, MultiService
from twisted.plugin import IPlugin
from twisted.python import usage
from zope.interface import implements


from spinoff.actor._actor import _validate_nodeid
from spinoff.actor.runner import ActorRunner
from spinoff.util.logging import fatal, log


class _EMPTY(object):
    def __repr__(self):
        return '<empty>'

    def __nonzero__(self):
        return False
_EMPTY = _EMPTY()


class Options(usage.Options):

    optParameters = [
        ['actor', 'a', None, "The [a]ctor to spawn."],
        ['params', 'i', _EMPTY, (u"Parameters to [i]nitialize the actor with.\n\n"
                                 u"Parsed as `dict(<params>)` and passed as **kwargs.\n\n"
                                 u"\b")],  # so that Twisted wouldn't strip off the line endings before [default: <empty>]
        ['message', 'm', _EMPTY, "[m]essage to send to the actor"],
        ['remoting', 'r', None, "Set up [r]emoting with"],
        ['name', 'n', None, "Set the [n]ame of the actor"],
        ['supervise', 's', 'stop', "Set how the spawned actor is [s]upervised in case of failures"],

        ['remotedebuggingport', 'p', 6022, "[p]rt on which to start the SSH remote debug console server"],
        ['remotedebuggingusername', 'u', 'debug', "[u]sername to log on to the SSH remote debug console"],
        ['remotedebuggingpassword', 'P', '', "[P]assword to log on to the SSH remote debug console"],
    ]
    optFlags = [
        ['keeprunning', 'k', "Whether the actor should be re-spawned on termination"],
        ['remotedebugging', 'd', "Whether to start a SSH remote [d]ebug console server"],
    ]


class ActorRunnerMaker(object):
    implements(IServiceMaker, IPlugin)

    tapname = 'startnode'
    description = 'Starts a node with the specified actor running in it'
    options = Options

    def makeService(self, options):
        actor = options['actor']
        if not actor:
            fatal("error: no actor specified")
            sys.exit(1)

        try:
            module_path, actor_cls_name = actor.rsplit('.', 1)
        except ValueError:
            fatal("error: invalid path to actor %s" % actor)
            sys.exit(1)

        try:
            mod = __import__(module_path, globals(), locals(), [actor_cls_name], -1)
        except ImportError:
            fatal("error: could not import %s:\n%s" % (actor, traceback.format_exc()))
            sys.exit(1)

        try:
            actor_cls = getattr(mod, actor_cls_name)
        except AttributeError:
            fatal("error: no such actor %s" % actor)
            sys.exit(1)

        kwargs = {}

        if options['params'] is not _EMPTY:
            params = 'dict(%s)' % (options['params'],)
            try:
                params = eval(params)
            except:
                fatal("error: could not parse parameters")
                sys.exit(1)
            else:
                kwargs['init_params'] = params

        if options['message'] is not _EMPTY:
            initial_message = options['message']
            try:
                initial_message = eval(initial_message)
            except:
                fatal("error: could not parse initial message")
                sys.exit(1)
            else:
                kwargs['initial_message'] = initial_message

        if options['name']:
            name = options['name']
            if '/' in name:
                fatal("invalid name: names cannot contain slashes")
                sys.exit(1)
            else:
                kwargs['name'] = name

        if options['remoting']:
            nodeid = options['remoting']
            try:
                _validate_nodeid(nodeid)
            except TypeError:
                fatal("invalid node ID")
                sys.exit(1)
            else:
                kwargs['nodeid'] = nodeid

        if options['supervise']:
            supervise_option = options['supervise']
            if supervise_option not in ('stop', 'restart', 'resume'):
                fatal("Supervision should be either 'stop', 'restart' or 'resume'")
                sys.exit(1)
            kwargs['supervise'] = supervise_option

        kwargs['keep_running'] = options['keeprunning']

        m = MultiService()
        actor_runner = ActorRunner(actor_cls, **kwargs)
        actor_runner.setServiceParent(m)

        # manhole

        if options['remotedebugging']:
            port = options['remotedebuggingport']
            try:
                port = int(port)
            except ValueError:
                fatal("Invalid port specified: %r" % port)
                sys.exit(1)

            username = options['remotedebuggingusername']
            password = options['remotedebuggingpassword']

            manhole = self.make_manhole_server(port, username, password)
            manhole.setServiceParent(m)

        return m

    def make_manhole_server(self, port, username, password):
        from twisted.application.internet import TCPServer
        from twisted.cred.portal import Portal
        from twisted.cred.checkers import InMemoryUsernamePasswordDatabaseDontUse
        from twisted.conch.manhole import ColoredManhole
        from twisted.conch.manhole_ssh import ConchFactory, TerminalRealm
        from twisted.conch.insults.insults import ServerProtocol

        rlm = TerminalRealm()
        rlm.chainedProtocolFactory = lambda: ServerProtocol(ColoredManhole, None)

        auth_checker = InMemoryUsernamePasswordDatabaseDontUse(**{username: password})

        return TCPServer(port, ConchFactory(Portal(rlm, [auth_checker])))

    def __repr__(self):
        return '<#bootstrap#>'


serviceMaker = ActorRunnerMaker()
