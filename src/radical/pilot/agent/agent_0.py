
__copyright__ = 'Copyright 2014-2016, http://radical.rutgers.edu'
__license__   = 'MIT'


import os
import sys
import copy
import stat
import time
import pprint
import signal
import subprocess         as sp

import radical.utils      as ru

from ..  import db
from ..  import utils     as rpu
from ..  import states    as rps
from ..  import constants as rpc
from ..  import Session   as rp_Session

from .  import rm         as rpa_rm
from .  import lm         as rpa_lm


# this needs git attribute 'ident' set for this file
git_ident = '$Id$'


# ==============================================================================
#
class Agent_0(rpu.Worker):

    # This is the base agent.  It does not do much apart from starting
    # sub-agents and watching them  If any of the sub-agents die, it will shut
    # down the other sub-agents and itself.
    #
    # This class inherits the rpu.Worker, so that it can use the communication
    # bridges and callback mechanisms.  It will own a session (which creates said
    # communication bridges (or at least some of them); and a controller, which
    # will control the sub-agents.

    # --------------------------------------------------------------------------
    #
    def __init__(self, agent_name):

        # synchronization timestamp
        t_zero = time.time()

        assert(agent_name == 'agent_0'), 'expect agent_0, not subagent'
        print 'startup agent %s' % agent_name

        # load config, create session, init rpu.Worker
        agent_cfg  = '%s/%s.cfg' % (os.getcwd(), agent_name)
        cfg        = ru.read_json_str(agent_cfg)

        cfg['agent_name'] = agent_name

        self._uid         = agent_name
        self._pid         = cfg['pilot_id']
        self._sid         = cfg['session_id']
        self._runtime     = cfg['runtime']
        self._starttime   = time.time()
        self._final_cause = None
        self._lrms        = None
        self._hb_last     = time.time()
        self._hb_timeout  = 40.0  # FIXME: make configurable / adaptive

        # this better be on a shared FS!
        cfg['uid']             = self._uid
        cfg['owner']           = self._sid
        cfg['workdir']         = os.getcwd()
        cfg['log_dir']         = cfg['pilot_sandbox']
        cfg['profile_dir']     = cfg['pilot_sandbox']
        cfg['session_sandbox'] = cfg['pilot_sandbox']

        # sanity check on config settings
        if 'cores'               not in cfg: raise ValueError('Missing number of cores')
        if 'lrms'                not in cfg: raise ValueError('Missing LRMS')
        if 'pilot_id'            not in cfg: raise ValueError('Missing pilot id')
        if 'runtime'             not in cfg: raise ValueError('Missing or zero agent runtime')
        if 'scheduler'           not in cfg: raise ValueError('Missing agent scheduler')
        if 'session_id'          not in cfg: raise ValueError('Missing session id')
        if 'spawner'             not in cfg: raise ValueError('Missing agent spawner')
        if 'task_launch_method'  not in cfg: raise ValueError('Missing unit launch method')

        # Check for the RADICAL_PILOT_DB_HOSTPORT env var, which will hold
        # the address of the tunnelized DB endpoint. If it exists, we
        # overrule the agent config with it.
        # FIXME: tunnel to client and agent queues
        hostport = os.environ.get('RADICAL_PILOT_DB_HOSTPORT')
        if hostport:
            pass

        # Create a session.
        #
        # This session will connect to MongoDB, and will also create any
        # communication channels and components/workers specified in the
        # config -- we merge that information into our own config.
        # We don't want the session to start components though, so remove them
        # from the config copy.
        scfg = {'log_dir'         : cfg['log_dir'],
                'profile_dir'     : cfg['profile_dir'],
                'session_sandbox' : cfg['session_sandbox']}
        self._session = rp_Session(uid=self._sid, _cfg=scfg)

        # Create LRMS which will give us the set of agent_nodes to use for
        # sub-agent startup.  Add the remaining LRMS information to the
        # config, for the benefit of the scheduler).
        self._lrms = rpa_rm.RM.create(cfg['lrms'], cfg, self._session)

        # add the resource manager information to our own config
        cfg['lrms_info'] = self._lrms.lrms_info
        cfg['owner']     = self._uid

        # only now, after the lrms is created, we can instantiate components, as
        # those need the LRMS info.
        self._cmgr = rpu.ComponentManager(self._session, cfg, self._uid)

        # at this point the session is up and connected, and we should have
        # brought up all communication bridges and the UpdateWorker.  We are
        # ready to rumble!
        rpu.Worker.__init__(self, cfg, self._session)

        # this is the point to sync bootstrapper and agent profiles
        self._prof.prof('sync_rel', msg='agent_0 start', uid=self._pid,
                        timestamp=t_zero)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # create the sub-agent configs
        self._write_sa_configs()

        # and start the sub agents
        self._start_sub_agents()

        # refresh heartbeat before checking it the first time
        self._hb_last = time.time()

        # register the command callback which pulls the DB for commands
        self.register_timed_cb(self._agent_command_cb,
                               timer=self._cfg['db_poll_sleeptime'])

        # registers the staging_input_queue as this is what we want to push
        # units to
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)

        # sub-agents are started, components are started, bridges are up: we are
        # ready to roll!
        pilot = {'type'             : 'pilot',
                 'uid'              : self._pid,
                 'state'            : rps.PMGR_ACTIVE,
                 'resource_details' : {
                     'lm_info'      : self._lrms.lm_info.get('version_info'),
                     'lm_detail'    : self._lrms.lm_info.get('lm_detail')},
                 '$set'             : ['resource_details']}
        self.advance(pilot, publish=True, push=False)

        # record hostname in profile to enable mapping of profile entries
        self._prof.prof(event='hostname', uid=self._pid, msg=ru.get_hostname())


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # tear things down in reverse order
        self._prof.flush()
        self._log.info('publish "terminate" cmd')
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'terminate',
                                          'arg' : None})

        if self._lrms:
            self._log.debug('stop    lrms %s', self._lrms)
            self._lrms.stop()
            self._log.debug('stopped lrms %s', self._lrms)

        if   self._final_cause == 'timeout'  : state = rps.DONE
        elif self._final_cause == 'cancel'   : state = rps.CANCELED
        elif self._final_cause == 'sys.exit' : state = rps.CANCELED
        else                                 : state = rps.FAILED

        self._log.debug('final state: %s (%s)', state, self._final_cause)


    # --------------------------------------------------------------------------
    #
    def wait_final(self):

        while self._final_cause is None:
            time.sleep(0.1)

        self._log.debug('final: %s', self._final_cause)


    # --------------------------------------------------------------------------
    #
    def _update_db(self, state, msg=None):

        # NOTE: we do not push the final pilot state, as that is done by the
        #       bootstrapper *after* this poilot *actually* finished.

        self._log.info('pilot state: %s', state)
        self._log.info('rusage: %s', rpu.get_rusage())
        self._log.info(msg)

        if state == rps.FAILED:
            self._log.info(ru.get_trace())

        out = None
        err = None
        log = None

        try    : out = open('./agent_0.out', 'r').read(1024)
        except Exception: pass
        try    : err = open('./agent_0.err', 'r').read(1024)
        except Exception: pass
        try    : log = open('./agent_0.log', 'r').read(1024)
        except Exception: pass

        # FIXME
        ## ret = self._db._c.update({'type'   : 'pilot',
        ##                           'uid'    : self._pid},
        ##                          {'$set'   : {'stdout'        : rpu.tail(out),
        ##                                       'stderr'        : rpu.tail(err),
        ##                                       'logfile'       : rpu.tail(log)}
        ##                          })
        ## self._log.debug('update ret: %s', ret)


    # --------------------------------------------------------------------------
    #
    def _write_sa_configs(self):

        # we have all information needed by the subagents -- write the
        # sub-agent config files.

        # write deep-copies of the config for each sub-agent (sans from agent_0)
        for sa in self._cfg.get('agents', {}):

            assert(sa != 'agent_0'), 'expect subagent, not agent_0'

            # use our own config sans agents/components as a basis for
            # the sub-agent config.
            tmp_cfg = copy.deepcopy(self._cfg)
            tmp_cfg['agents']     = dict()
            tmp_cfg['components'] = dict()

            # merge sub_agent layout into the config
            ru.dict_merge(tmp_cfg, self._cfg['agents'][sa], ru.OVERWRITE)

            tmp_cfg['agent_name'] = sa
            tmp_cfg['owner']      = 'agent_0'

            ru.write_json(tmp_cfg, './%s.cfg' % sa)


    # --------------------------------------------------------------------------
    #
    def _start_sub_agents(self):
        '''
        For the list of sub_agents, get a launch command and launch that
        agent instance on the respective node.  We pass it to the seconds
        bootstrap level, there is no need to pass the first one again.
        '''

        # FIXME: we need a watcher cb to watch sub-agent state

        self._log.debug('start_sub_agents')

        if not self._cfg.get('agents'):
            self._log.debug('start_sub_agents noop')
            return

        # the configs are written, and the sub-agents can be started.  To know
        # how to do that we create the agent launch method, have it creating
        # the respective command lines per agent instance, and run via
        # popen.
        #
        # actually, we only create the agent_lm once we really need it for
        # non-local sub_agents.
        agent_lm   = None
        for sa in self._cfg['agents']:

            target = self._cfg['agents'][sa]['target']

            if target == 'local':

                # start agent locally
                cmdline = '/bin/sh -l %s/bootstrap_2.sh %s' % (os.getcwd(), sa)

            elif target == 'node':

                if not agent_lm:
                    agent_lm = rpa_lm.LaunchMethod.create(
                        name    = self._cfg['agent_launch_method'],
                        cfg     = self._cfg,
                        session = self._session)

                node = self._cfg['lrms_info']['agent_nodes'][sa]
                # start agent remotely, use launch method
                # NOTE:  there is some implicit assumption that we can use
                #        the 'agent_node' string as 'agent_string:0' and
                #        obtain a well format slot...
                # FIXME: it is actually tricky to translate the agent_node
                #        into a viable 'slots' structure, as that is
                #        usually done by the schedulers.  So we leave that
                #        out for the moment, which will make this unable to
                #        work with a number of launch methods.  Can the
                #        offset computation be moved to the LRMS?
                ls_name = "%s/%s.sh" % (os.getcwd(), sa)
                slots = {
                  'cpu_processes' : 1,
                  'cpu_threads'   : 1,
                  'gpu_processes' : 0,
                  'gpu_threads'   : 0,
                # 'nodes'         : [[node[0], node[1], [[0]], []]],
                  'nodes'         : [{'name'    : node[0], 
                                     'uid'     : node[1],
                                     'core_map': [[0]],
                                     'gpu_map' : [],
                                     'lfs'     : {'path': '/tmp', 'size': 0}
                                    }],
                  'cores_per_node': self._cfg['lrms_info']['cores_per_node'],
                  'gpus_per_node' : self._cfg['lrms_info']['gpus_per_node'],
                  'lm_info'       : self._cfg['lrms_info']['lm_info']
                }
                agent_cmd = {
                        'uid'          : sa,
                        'slots'        : slots,
                        'description'  : {
                            'cpu_processes' : 1,
                            'executable'    : "/bin/sh",
                            'mpi'           : False,
                            'arguments'     : ["%s/bootstrap_2.sh" % os.getcwd(), sa]
                            }
                        }
                cmd, hop = agent_lm.construct_command(agent_cmd,
                        launch_script_hop='/usr/bin/env RP_SPAWNER_HOP=TRUE "%s"' % ls_name)

                with open (ls_name, 'w') as ls:
                    # note that 'exec' only makes sense if we don't add any
                    # commands (such as post-processing) after it.
                    ls.write('#!/bin/sh\n\n')
                    ls.write('exec %s\n' % cmd)
                    st = os.stat(ls_name)
                    os.chmod(ls_name, st.st_mode | stat.S_IEXEC)

                if hop : cmdline = hop
                else   : cmdline = ls_name

            # spawn the sub-agent
            self._log.info ('create sub-agent %s: %s' % (sa, cmdline))

            # ------------------------------------------------------------------
            # FIXME: use component manager?
            class _SA(ru.Process):
                def __init__(self, sa, cmd, log):
                    self._sa   = sa
                    self._cmd  = cmd.split()
                    self._log  = log
                    self._proc = None
                    super(_SA, self).__init__(name=sa, log=self._log)
                    self.start()

                def ru_initialize_child(self):
                    sys.stdout = open('%s.out' % self._ru_name, 'w')
                    sys.stderr = open('%s.err' % self._ru_name, 'w')
                    out = open('%s.out' % self._sa, 'w')
                    err = open('%s.err' % self._sa, 'w')
                    self._proc = sp.Popen(args=self._cmd, stdout=out, stderr=err)

                def work_cb(self):
                    time.sleep(0.1)
                    if self._proc.poll() is None:
                        return True   # all is well
                    else:
                        return False  # proc is gone - terminate

                def ru_finalize_child(self):
                    if self._proc:
                        try:
                            self._proc.terminate()
                        except Exception as e:
                            # we are likely racing on termination...
                            self._log.warn('%s term failed: %s', self._sa, e)
            # ------------------------------------------------------------------

            # the agent is up - let the watcher manage it from here
            self.register_watchable(_SA(sa, cmdline, log=self._log))

        self._log.debug('start_sub_agents done')


    # --------------------------------------------------------------------------
    #
    def _agent_command_cb(self):

        self.is_valid()

        if not self._check_commands(): return False
        if not self._check_state   (): return False

        return True


    # --------------------------------------------------------------------------
    #
    def _check_commands(self):

        # Check if there's a command waiting - if so retrieve and purge
        # FIXME: this pull should be done by the update worker, and commands
        #        should then be communicated over the command pubsub
        # FIXME: commands go to pmgr, umgr, session docs
        # FIXME: this is disabled right now
        # FIXME
        ## retdoc = self._db._c.find_and_modify(query ={'uid'  : self._pid},
        ##                                      update={'$set' : {'cmd': []}},
        ##                                      fields=['cmd'])
        ## if not retdoc:
        ##     return True  # this is not an error

        ## for spec in retdoc.get('cmd', []):

        ##     self._log.info('=== cmd %s', spec)
        ##     cmd = spec['cmd']
        ##     arg = spec['arg']

        ##     self._prof.prof('cmd', msg="%s : %s" % (cmd, arg), uid=self._pid)

        ##     if cmd == 'heartbeat':
        ##         self._log.info('=== heartbeat refresh')
        ##         self._hb_last = time.time()


        ##     elif cmd == 'cancel_pilot':
        ##         self._log.info('cancel pilot cmd')
        ##         self._log.info('publish "terminate" cmd')
        ##         self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'terminate',
        ##                                           'arg' : None})

        ##       # self.stop()
        ##         self._ru_term.set()

        ##         with open('./killme.signal', 'w+') as f:
        ##             f.write(rps.CANCELED)
        ##             f.flush()

        ##         self._final_cause = 'cancel'
        ##         return False  # we are done

        ##     elif cmd == 'cancel_units':

        ##         self._log.info('cancel_units cmd')
        ##         self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'cancel_units',
        ##                                           'arg' : arg})
        ##     else:
        ##         self._log.error('could not interpret cmd "%s" - ignore', cmd)

        return True


    # --------------------------------------------------------------------------
    #
    def _check_state(self):

        # Make sure that we haven't exceeded the runtime (if one is set). If
        # we have, terminate.
        if self._runtime:
            if time.time() >= self._starttime + (int(self._runtime) * 60):
                self._log.info('walltime limit (%ss).', self._runtime * 60)
                self._final_cause = 'timeout'
                self.stop()
                return False  # we are done

     ## # make sure we did not lose connection to client
     ## if time.time() - self._hb_last > self._hb_timeout:
     ##     self._log.error('=== heartbeat timeout - terminate')
     ##     os.kill(os.getpid(), signal.SIGTERM)

        return True


# ------------------------------------------------------------------------------

