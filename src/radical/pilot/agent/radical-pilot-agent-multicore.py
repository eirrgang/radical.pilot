#!/usr/bin/env python

"""
.. module:: radical.pilot.agent
   :platform: Unix
   :synopsis: The agent for RADICAL-Pilot.

   The agent gets CUs by means of the MongoDB.
   The execution of CUs by the Agent is (primarily) configured by the
   triplet (LRMS, LAUNCH_METHOD(s), SCHEDULER):
   - The LRMS detects and structures the information about the resources
     available to agent.
   - The Scheduler maps the execution requests of the LaunchMethods to a
     subset of the resources available to the Agent.
     It does not deal with the "presentation" of this subset.
   - The LaunchMethods configure how to execute (regular and MPI) tasks,
     and know about the specific format to specify the subset of resources.


   Structure:
   ----------
   This represents the planned architecture, which is not fully represented in
   code, yet.

     - class Agent
       - represents the whole thing
       - has a set of InputStagingWorkers  (threads or procs)
       - has a set of OutputStagingWorkers (threads or procs)
       - has a set of ExecWorkers          (threads or procs)
       - has a set of UpdateWorkers        (threads or procs)
       - has a inputstaging  queue
       - has a outputstaging queue
       - has a execution queue
       - has a update queue
       - loops forever
       - in each iteration
         - pulls CU bulks from DB
         - pushes CUs into inputstaging queue or execution queue (based on
           obvious metric)

     class InputeStagingWorker
       - competes for CU input staging requests from inputstaging queue
       - for each received CU 
         - performs staging
         - pushes CU into execution queue
         - pushes stage change notification request into update queue

     class OutputeStagingWorker
       - competes for CU output staging requests from outputstaging queue
       - for each received CU 
         - performs staging
         - pushes stage change notification request into update queue

     class ExecWorker
       - manages a partition of the allocated cores
         (partition size == max cu size)
       - has one spawner (operating on that partition)
       - competes for CU execution reqeusts from execute queue
       - for each CU
         - prepares execition command (LRMS, Launcher, ...)
         - pushes command to spawner
         - pushes stage change notification request into update queue

     class Spawner
       - executes CUs according to ExecWorker instruction
       - monitors CU execution (for completion)
       - gets CU execution reqeusts from ExecWorker
       - for each CU
         - executes CU command
         - monitors CU execution
         - on CU completion
           - pushes CU to outputstaging queue (if staging is needed)
           - pushes stage change notification request into update queue

     class Updater
       - competes for CU state update reqeusts from update queue
       - for each CU
         - pushes state update (collected into bulks if possible)
         - cleans CU workdir if CU is final and cleanup is requested

    NOTE:
    -----
      - each CU will be passed to the different threads via queues.
        cu state changes are pushed onto the update_queue.  As long as events in
        the update queue remain ordered, no invalid state transitions of the CU
        can occur.  TO ensure that, always *first* push state transition
        requests onto the update queue, *then* move the CU to the next thread
        (state)...

    TODO:
    -----

    - add option to scheduler to ignore node 0 (which hosts the agent process)
    - add LRMS.partition (n) to return a set of partitioned LRMS for partial
      ExecWorkers
"""

__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import copy
import math
import saga
import stat
import sys
import time
import errno
import Queue
import signal
import shutil
import pymongo
import optparse
import logging
import datetime
import hostlist
import tempfile
import traceback
import threading 
import subprocess
import multiprocessing

from bson.objectid import ObjectId
from operator import mul


# this needs git attribute 'ident' set for this file
git_ident = "$Id$"


# ------------------------------------------------------------------------------
# CONSTANTS
#
N_EXECUTION_WORKER          = 1
N_UPDATE_WORKER             = 1
N_STAGEIN_WORKER            = 1
N_STAGEOUT_WORKER           = 1

# 'enum' for unit launch method types
LAUNCH_METHOD_APRUN         = 'APRUN'
LAUNCH_METHOD_CCMRUN        = 'CCMRUN'
LAUNCH_METHOD_DPLACE        = 'DPLACE'
LAUNCH_METHOD_FORK          = 'FORK'
LAUNCH_METHOD_IBRUN         = 'IBRUN'
LAUNCH_METHOD_MPIEXEC       = 'MPIEXEC'
LAUNCH_METHOD_MPIRUN_CCMRUN = 'MPIRUN_CCMRUN'
LAUNCH_METHOD_MPIRUN_DPLACE = 'MPIRUN_DPLACE'
LAUNCH_METHOD_MPIRUN        = 'MPIRUN'
LAUNCH_METHOD_MPIRUN_RSH    = 'MPIRUN_RSH'
LAUNCH_METHOD_POE           = 'POE'
LAUNCH_METHOD_RUNJOB        = 'RUNJOB'
LAUNCH_METHOD_SSH           = 'SSH'

# 'enum' for local resource manager types
LRMS_NAME_FORK              = 'FORK'
LRMS_NAME_LOADLEVELER       = 'LOADL'
LRMS_NAME_LSF               = 'LSF'
LRMS_NAME_PBSPRO            = 'PBSPRO'
LRMS_NAME_SGE               = 'SGE'
LRMS_NAME_SLURM             = 'SLURM'
LRMS_NAME_TORQUE            = 'TORQUE'

# 'enum' for pilot's unit scheduler types
SCHEDULER_NAME_CONTINUOUS   = "CONTINUOUS"
SCHEDULER_NAME_SCATTERED    = "SCATTERED"
SCHEDULER_NAME_TORUS        = "TORUS"

# 'enum' for pilot's task spawner types
SPAWNER_NAME_POPEN          = "POPEN"
SPAWNER_NAME_PTY            = "PTY"

# defines for pilot commands
COMMAND_CANCEL_PILOT        = "Cancel_Pilot"
COMMAND_CANCEL_COMPUTE_UNIT = "Cancel_Compute_Unit"
COMMAND_KEEP_ALIVE          = "Keep_Alive"
COMMAND_FIELD               = "commands"
COMMAND_TYPE                = "type"
COMMAND_ARG                 = "arg"


# 'enum' for staging action operators
COPY     = 'Copy'     # local cp
LINK     = 'Link'     # local ln -s
MOVE     = 'Move'     # local mv
TRANSFER = 'Transfer' # saga remote transfer 
                      # TODO: This might just be a special case of copy

# tri-state for unit spawn retval
OK       = 'OK'
FAIL     = 'FAIL'
RETRY    = 'RETRY'

# two-state for slot occupation.
FREE     = 'Free'
BUSY     = 'Busy'

# directory for staging files inside the agent sandbox
STAGING_AREA         = 'staging_area'

# max number of cu out/err chars to push to db
MAX_IO_LOGLENGTH     = 64*1024

# max time period to collec db requests into bulks (seconds)
BULK_COLLECTION_TIME = 1.0   # FIXME: 2*latency

# time to sleep between queue polls (seconds)
QUEUE_POLL_SLEEPTIME = 1.0   # FIXME: 2*latency


# ------------------------------------------------------------------------------
#
# state enums (copied from radical/pilot/states.py
#
# Common States
NEW                         = 'New'
PENDING                     = 'Pending'
DONE                        = 'Done'
CANCELING                   = 'Canceling'
CANCELED                    = 'Canceled'
FAILED                      = 'Failed'

# ComputePilot States
PENDING_LAUNCH              = 'PendingLaunch'
LAUNCHING                   = 'Launching'
PENDING_ACTIVE              = 'PendingActive'
ACTIVE                      = 'Active'

# ComputeUnit States
PENDING_EXECUTION           = 'PendingExecution'
SCHEDULING                  = 'Scheduling'
ALLOCATING                  = 'Allocating'
EXECUTING                   = 'Executing'

# These last 4 are not really states, as there are distributed entities enacting
# on them.  They should probably just go, and be turned into logging events.

PENDING_INPUT_STAGING       = 'PendingInputStaging'
STAGING_INPUT               = 'StagingInput'
PENDING_OUTPUT_STAGING      = 'PendingOutputStaging'
STAGING_OUTPUT              = 'StagingOutput'

# ------------------------------------------------------------------------------
#
# time stamp for profiling etc.
#
def timestamp () :
    # human readable absolute UTC timestamp for log entries in database
    return datetime.datetime.utcnow ()

def timestamp_epoch () :
    # absolute timestamp as seconds since epoch
    return float(time.time())

# absolute timestamp in seconds since epocj pointing at start of
# bootstrapper (or 'now' as fallback)
timestamp_zero = float(os.environ.get ('TIME_ZERO', time.time()))

print "timestamp zero: %s" % timestamp_zero

def timestamp_now () :
    # relative timestamp seconds since TIME_ZERO (start)
    return float(time.time()) - timestamp_zero


# ------------------------------------------------------------------------------
#
# profiling support
#
# If 'RADICAL_PILOT_PROFILE' is set in environment, the agent logs timed events.
#
if 'RADICAL_PILOT_PROFILE' in os.environ:
    profile_agent  = True
    profile_handle = open('AGENT.prof', 'a')
else :
    profile_agent  = False
    profile_handle = sys.stdout


# ------------------------------------------------------------------------------
#
profile_tags  = dict ()
profile_freqs = dict ()

def prof (etype, uid="", msg="", tag="", logger=None) :

    if  logger :
        logger ("%s -- %s (%s): %s" % (etype, msg, uid, tag))


    # whenever a tag changes (to a non-None value), the time since the last tag
    # change is added
    #
    # times between the same tags (but different uids) are recorded, too, and
    # a frequency is derived (tagged events / second)

    if not profile_agent :
        return

    logged = False
    tid    = threading.current_thread().name
    now    = timestamp_now()
    print "timestamp now: %s" % now


    if uid and tag :

        if not uid in profile_tags :
            profile_tags[uid] = { 'tag'  : "",
                                  'time' : 0.0 }

        old_tag = profile_tags[uid]['tag']

        if tag != old_tag :

            tagged_time = now - profile_tags[uid]['time']

            profile_tags[uid]['tag' ] = tag
            profile_tags[uid]['time'] = timestamp_now()

            profile_handle.write ("> %12.4f : %-20s : %12.4f : %-15s : %-24s : %-40s : %s\n" \
                               % (tagged_time, tag, now, tid, uid, etype, msg))
            logged = True


            if not tag in profile_freqs :
                profile_freqs[tag] = {'last'  : now, 
                                      'diffs' : list()}
            else :
                diff = now - profile_freqs[tag]['last']
                profile_freqs[tag]['diffs'].append (diff)
                profile_freqs[tag]['last' ] = now

                freq = sum(profile_freqs[tag]['diffs']) / len (profile_freqs[tag]['diffs'])

                profile_handle.write ("> %12s : %-20.4f : %12s : %-15s : %-24s : %-40s : %s\n" \
                                   % ('frequency', freq, '', '', '', '', ''))



    if not logged :
        profile_handle.write ("  %12s : %-20s : %12.4f : %-15s : %-24s : %-40s : %s\n" \
                           % (' ' , ' ', now, tid, uid, etype, msg))

    # FIXME: disable flush on production runs
    profile_handle.flush ()


#---------------------------------------------------------------------------
#
def get_rusage () :

    import resource

    self_usage  = resource.getrusage (resource.RUSAGE_SELF)
    child_usage = resource.getrusage (resource.RUSAGE_CHILDREN)

    rtime = time.time () - timestamp_zero
    utime = self_usage.ru_utime  + child_usage.ru_utime
    stime = self_usage.ru_stime  + child_usage.ru_stime
    rss   = self_usage.ru_maxrss + child_usage.ru_maxrss

    return "real %3f sec | user %.3f sec | system %.3f sec | mem %.2f kB" \
         % (rtime, utime, stime, rss)


# ------------------------------------------------------------------------------
#
def pilot_FAILED(mongo_p, pilot_uid, logger, message):
    """Updates the state of one or more pilots.
    """
    logger.error(message)      

    ts  = timestamp()
    out = None
    err = None
    log = None

    try    : out = open ('./AGENT.STDOUT', 'r').read ()
    except : pass
    try    : err = open ('./AGENT.STDERR', 'r').read ()
    except : pass
    try    : log = open ('./AGENT.LOG',    'r').read ()
    except : pass

    msg = [{"message": message,      "timestamp": ts},
           {"message": get_rusage(), "timestamp": ts}]

    if  mongo_p :
        mongo_p.update({"_id": ObjectId(pilot_uid)}, 
            {"$pushAll": {"log"         : msg},
             "$push"   : {"statehistory": {"state"     : FAILED, 
                                           "timestamp" : ts}},
             "$set"    : {"state"       : FAILED,
                          "stdout"      : out,
                          "stderr"      : err,
                          "logfile"     : log,
                          "capability"  : 0,
                          "finished"    : ts}
            })

    else :
        logger.error ("cannot log error state in database!")


# ------------------------------------------------------------------------------
#
def pilot_CANCELED(mongo_p, pilot_uid, logger, message):
    """Updates the state of one or more pilots.
    """
    logger.warning(message)

    ts  = timestamp()
    out = None
    err = None
    log = None

    try    : out = open ('./AGENT.STDOUT', 'r').read ()
    except : pass
    try    : err = open ('./AGENT.STDERR', 'r').read ()
    except : pass
    try    : log = open ('./AGENT.LOG',    'r').read ()
    except : pass

    msg = [{"message": message,      "timestamp": ts},
           {"message": get_rusage(), "timestamp": ts}]

    mongo_p.update({"_id": ObjectId(pilot_uid)}, 
        {"$pushAll": {"log"         : msg},
         "$push"   : {"statehistory": {"state"     : CANCELED, 
                                       "timestamp" : ts}},
         "$set"    : {"state"       : CANCELED,
                      "stdout"      : out,
                      "stderr"      : err,
                      "logfile"     : log,
                      "capability"  : 0,
                      "finished"    : ts}
        })


# ------------------------------------------------------------------------------
#
def pilot_DONE(mongo_p, pilot_uid):
    """Updates the state of one or more pilots.
    """

    ts  = timestamp()
    out = None
    err = None
    log = None

    try    : out = open ('./AGENT.STDOUT', 'r').read ()
    except : pass
    try    : err = open ('./AGENT.STDERR', 'r').read ()
    except : pass
    try    : log = open ('./AGENT.LOG',    'r').read ()
    except : pass

    msg = [{"message": "pilot done", "timestamp": ts}, 
           {"message": get_rusage(), "timestamp": ts}]

    mongo_p.update({"_id": ObjectId(pilot_uid)}, 
        {"$pushAll": {"log"         : msg},
         "$push"   : {"statehistory": {"state"    : DONE, 
                                       "timestamp": ts}},
         "$set"    : {"state"       : DONE,
                      "stdout"      : out,
                      "stderr"      : err,
                      "logfile"     : log,
                      "capability"  : 0,
                      "finished"    : ts}
        })


# ------------------------------------------------------------------------------
#
class ExecutionEnvironment(object):
    """DOC
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, logger, lrms_name, requested_cores,
                 task_launch_method, mpi_launch_method, scheduler_name):

        # Derive the environment for the cu's from our own environment
        self.cu_environment = self._populate_cu_environment()

        # Configure nodes and number of cores available
        self.lrms = LRMS.create(lrms_name, requested_cores, logger)

        self.scheduler = Scheduler.create(scheduler_name, self.lrms, logger)

        self.task_launcher = LaunchMethod.create(task_launch_method,
                                                  self.scheduler, logger)
        self.mpi_launcher = LaunchMethod.create(mpi_launch_method,
                                                 self.scheduler, logger)

        self.spawner = Spawner.create (SPAWNER_NAME_POPEN, logger)

    # --------------------------------------------------------------------------
    #
    def _populate_cu_environment(self):
        """Derive the environment for the cu's from our own environment."""

        # Get the environment of the agent
        new_env = copy.deepcopy(os.environ)

        #
        # Mimic what virtualenv's "deactivate" would do
        #
        old_path = new_env.pop('_OLD_VIRTUAL_PATH', None)
        if old_path:
            new_env['PATH'] = old_path

        old_home = new_env.pop('_OLD_VIRTUAL_PYTHONHOME', None)
        if old_home:
            new_env['PYTHON_HOME'] = old_home

        old_ps = new_env.pop('_OLD_VIRTUAL_PS1', None)
        if old_ps:
            new_env['PS1'] = old_ps

        new_env.pop('VIRTUAL_ENV', None)

        return new_env


# ==============================================================================
#
# Schedulers
#
# ==============================================================================
class Scheduler(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lrms, logger):

        self.name = name
        self.lrms = lrms
        self.log = logger

        self.configure()

    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Launch Method.
    #
    @classmethod
    def create(cls, name, lrms, logger):

        # Make sure that we are the base-class!
        if cls != Scheduler:
            raise Exception("Scheduler Factory only available to base class!")

        try:
            implementation = {
                SCHEDULER_NAME_CONTINUOUS : SchedulerContinuous,
              # SCHEDULER_NAME_SCATTERED  : SchedulerScattered,
                SCHEDULER_NAME_TORUS      : SchedulerTorus
            }[name]
            return implementation(name, lrms, logger)
        except KeyError:
            raise Exception("Scheduler '%s' unknown!" % name)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        raise NotImplementedError("configure() not implemented for Scheduler '%s'." % self.name)

    # --------------------------------------------------------------------------
    #
    def slot_status(self, short=False):
        raise NotImplementedError("slot_status() not implemented for Scheduler '%s'." % self.name)

    # --------------------------------------------------------------------------
    #
    def allocate_slot(self, cores_requested):
        raise NotImplementedError("allocate_slot() not implemented for Scheduler '%s'." % self.name)

    # --------------------------------------------------------------------------
    #
    def release_slot(self, opaque_slot):
        raise NotImplementedError("release_slot() not implemented for Scheduler '%s'." % self.name)

# ------------------------------------------------------------------------------
#
class SchedulerContinuous(Scheduler):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lrms, logger):
        Scheduler.__init__(self, name, lrms, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        if not self.lrms.node_list:
            raise Exception("LRMS %s didn't configure node_list." % self.lrms.name)

        if not self.lrms.cores_per_node:
            raise Exception("LRMS %s didn't configure cores_per_node." % self.lrms.name)

        # Slots represents the internal process management structure.
        # The structure is as follows:
        # [
        #    {'node': 'node1', 'cores': [p_1, p_2, p_3, ... , p_cores_per_node]},
        #    {'node': 'node2', 'cores': [p_1, p_2, p_3. ... , p_cores_per_node]
        # ]
        #
        # We put it in a list because we care about (and make use of) the order.
        #
        self._slots = []
        for node in self.lrms.node_list:
            self._slots.append({
                'node': node,
                # TODO: Maybe use the real core numbers in the case of 
                # non-exclusive host reservations?
                'cores': [FREE for _ in range(0, self.lrms.cores_per_node)]
            })

        # keep a slot allocation history (short status), start with presumably
        # empty state now
        self._slot_history     = [self.slot_status(short=True)]
        self._slot_history_old = None

        #self._capability = self._slots2caps(self._slots)
        #self._capability     = self._slots2free(self._slots)
        #self._capability_old = None

    # --------------------------------------------------------------------------
    #
    # Convert a set of slots into an index into the global slots list
    #
    def slots2offset(self, task_slots):
        # TODO: This assumes all hosts have the same number of cores

        first_slot = task_slots[0]
        # Get the host and the core part
        [first_slot_host, first_slot_core] = first_slot.split(':')
        # Find the entry in the the all_slots list based on the host
        slot_entry = (slot for slot in self._slots if slot["node"] == first_slot_host).next()
        # Transform it into an index in to the all_slots list
        all_slots_slot_index = self._slots.index(slot_entry)

        return all_slots_slot_index * self.lrms.cores_per_node + int(first_slot_core)

    # --------------------------------------------------------------------------
    #
    def slot_status(self, short=False):
        """Returns a multi-line string corresponding to slot status.
        """

        print 'ss 1'
        if short:
            print 'ss 2'
            slot_matrix = ""
            for slot in self._slots:
                slot_matrix += "|"
                for core in slot['cores']:
                    if core == FREE:
                        slot_matrix += "-"
                    else:
                        slot_matrix += "+"
            slot_matrix += "|"
            print 'ss 3'
            return {'timestamp' : timestamp(), 
                    'slotstate' : slot_matrix}

        else :
            print 'ss 4'
            slot_matrix = ""
            for slot in self._slots:
                slot_vector  = ""
                for core in slot['cores']:
                    if core == FREE:
                        slot_vector += " - "
                    else:
                        slot_vector += " X "
                slot_matrix += "%-24s: %s\n" % (slot['node'], slot_vector)
            print 'ss 5'
            return slot_matrix


    # --------------------------------------------------------------------------
    #
    # (Temporary?) wrapper for acquire_slots
    #
    def allocate_slot(self, cores_requested):

        # TODO: single_node should be enforced for e.g. non-message passing 
        #       tasks, but we don't have that info here.
        # NOTE AM: why should non-messaging tasks be confined to one node?
        if cores_requested < self.lrms.cores_per_node:
            single_node = True
        else:
            single_node = False

        # Given that we are the continuous scheduler, this is fixed.
        # TODO: Argument can be removed altogether?
        continuous = True

        # TODO: Now we rely on "None", maybe throw an exception?
        return self._acquire_slots(cores_requested, single_node=single_node, 
                continuous=continuous)

    # --------------------------------------------------------------------------
    #
    def release_slot(self, (task_slots)):
        self._change_slot_states(task_slots, FREE)

    # --------------------------------------------------------------------------
    #
    def _acquire_slots(self, cores_requested, single_node, continuous):

        #
        # Switch between searching for continuous or scattered slots
        #
        # Switch between searching for single or multi-node
        if single_node:
            if continuous:
                task_slots = self._find_slots_single_cont(cores_requested)
            else:
                raise NotImplementedError('No scattered single node scheduler implemented yet.')
        else:
            if continuous:
                task_slots = self._find_slots_multi_cont(cores_requested)
            else:
                raise NotImplementedError('No scattered multi node scheduler implemented yet.')

        if task_slots is not None:
            self._change_slot_states(task_slots, BUSY)

        return task_slots


    # --------------------------------------------------------------------------
    #
    # Find a needle (continuous sub-list) in a haystack (list)
    #
    def _find_sublist(self, haystack, needle):
        n = len(needle)
        # Find all matches (returns list of False and True for every position)
        hits = [(needle == haystack[i:i+n]) for i in xrange(len(haystack)-n+1)]
        try:
            # Grab the first occurrence
            index = hits.index(True)
        except ValueError:
            index = None

        return index


    # --------------------------------------------------------------------------
    #
    # Transform the number of cores into a continuous list of "status"es,
    # and use that to find a sub-list.
    #
    def _find_cores_cont(self, slot_cores, cores_requested, status):
        return self._find_sublist(slot_cores, [status for _ in range(cores_requested)])


    # --------------------------------------------------------------------------
    #
    # Find an available continuous slot within node boundaries.
    #
    def _find_slots_single_cont(self, cores_requested):

        for slot in self._slots:
            slot_node = slot['node']
            slot_cores = slot['cores']

            slot_cores_offset = self._find_cores_cont(slot_cores, cores_requested, FREE)

            if slot_cores_offset is not None:
                self.log.info('Node %s satisfies %d cores at offset %d' %
                              (slot_node, cores_requested, slot_cores_offset))
                return ['%s:%d' % (slot_node, core) for core in
                        range(slot_cores_offset, slot_cores_offset + cores_requested)]

        return None

    # --------------------------------------------------------------------------
    #
    # Find an available continuous slot across node boundaries.
    #
    def _find_slots_multi_cont(self, cores_requested):

        # Convenience aliases
        cores_per_node = self.lrms.cores_per_node
        all_slots = self._slots

        # Glue all slot core lists together
        all_slot_cores = [core for node in [node['cores'] for node in all_slots] for core in node]
        # self._log.debug("all_slot_cores: %s" % all_slot_cores)

        # Find the start of the first available region
        all_slots_first_core_offset = self._find_cores_cont(all_slot_cores, cores_requested, FREE)
        self.log.debug("all_slots_first_core_offset: %s" % all_slots_first_core_offset)
        if all_slots_first_core_offset is None:
            return None

        # Determine the first slot in the slot list
        first_slot_index = all_slots_first_core_offset / cores_per_node
        self.log.debug("first_slot_index: %s" % first_slot_index)
        # And the core offset within that node
        first_slot_core_offset = all_slots_first_core_offset % cores_per_node
        self.log.debug("first_slot_core_offset: %s" % first_slot_core_offset)

        # Note: We subtract one here, because counting starts at zero;
        #       Imagine a zero offset and a count of 1, the only core used 
        #       would be core 0.
        #       TODO: Verify this claim :-)
        all_slots_last_core_offset = (first_slot_index * cores_per_node) +\
                                     first_slot_core_offset + cores_requested - 1
        self.log.debug("all_slots_last_core_offset: %s" % all_slots_last_core_offset)
        last_slot_index = (all_slots_last_core_offset) / cores_per_node
        self.log.debug("last_slot_index: %s" % last_slot_index)
        last_slot_core_offset = all_slots_last_core_offset % cores_per_node
        self.log.debug("last_slot_core_offset: %s" % last_slot_core_offset)

        # Convenience aliases
        last_slot = self._slots[last_slot_index]
        self.log.debug("last_slot: %s" % last_slot)
        last_node = last_slot['node']
        self.log.debug("last_node: %s" % last_node)
        first_slot = self._slots[first_slot_index]
        self.log.debug("first_slot: %s" % first_slot)
        first_node = first_slot['node']
        self.log.debug("first_node: %s" % first_node)

        # Collect all node:core slots here
        task_slots = []

        # Add cores from first slot for this task
        # As this is a multi-node search, we can safely assume that we go
        # from the offset all the way to the last core.
        task_slots.extend(['%s:%d' % (first_node, core) for core in
                           range(first_slot_core_offset, cores_per_node)])

        # Add all cores from "middle" slots
        for slot_index in range(first_slot_index+1, last_slot_index):
            slot_node = all_slots[slot_index]['node']
            task_slots.extend(['%s:%d' % (slot_node, core) for core in range(0, cores_per_node)])

        # Add the cores of the last slot
        task_slots.extend(['%s:%d' % (last_node, core) for core in range(0, last_slot_core_offset+1)])

        return task_slots


    # --------------------------------------------------------------------------
    #
    # Change the reserved state of slots (FREE or BUSY)
    #
    def _change_slot_states(self, task_slots, new_state):

        # Convenience alias
        all_slots = self._slots

        # logger.debug("change_slot_states: task slots: %s" % task_slots)

        for slot in task_slots:
            # logger.debug("change_slot_states: slot content: %s" % slot)
            # Get the node and the core part
            [slot_node, slot_core] = slot.split(':')
            # Find the entry in the the all_slots list
            slot_entry = (slot for slot in all_slots if slot["node"] == slot_node).next()
            # Change the state of the slot
            slot_entry['cores'][int(slot_core)] = new_state

        # something changed - write history!
        # AM: mongodb entries MUST NOT grow larger than 16MB, or chaos will
        # ensue.  We thus limit the slot history size to 4MB, to keep sufficient
        # space for the actual operational data
        if len(str(self._slot_history)) < 4 * 1024 * 1024 :
            self._slot_history.append(self.slot_status (short=True))
        else:
            # just replace the last entry with the current one.
            self._slot_history[-1] = self.slot_status(short=True)


# ------------------------------------------------------------------------
#
class SchedulerTorus(Scheduler):

    # TODO: Ultimately all BG/Q specifics should move out of the scheduler

    # --------------------------------------------------------------------------
    #
    # Offsets into block structure
    #
    TORUS_BLOCK_INDEX  = 0
    TORUS_BLOCK_COOR   = 1
    TORUS_BLOCK_NAME   = 2
    TORUS_BLOCK_STATUS = 3
    #
    ##########################################################################

    # --------------------------------------------------------------------------
    def __init__(self, name, lrms, logger):
        Scheduler.__init__(self, name, lrms, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        if not self.lrms.cores_per_node:
            raise Exception("LRMS %s didn't configure cores_per_node." % self.lrms.name)

        self._cores_per_node = self.lrms.cores_per_node

        # keep a slot allocation history (short status), start with presumably
        # empty state now
        self._slot_history     = [self.slot_status(short=True)]
        self._slot_history_old = None

        # TODO: get rid of field below
        self._slots = 'bogus'

    # --------------------------------------------------------------------------
    #
    def slot_status(self, short=False):
        """Returns a multi-line string corresponding to slot status.
        """
        # TODO: Both short and long currently only deal with full-node status
        if short:
            slot_matrix = ""
            for slot in self.lrms.torus_block:
                slot_matrix += "|"
                if slot[self.TORUS_BLOCK_STATUS] == FREE:
                    slot_matrix += "-" * self.lrms.cores_per_node
                else:
                    slot_matrix += "+" * self.lrms.cores_per_node
            slot_matrix += "|"
            return {'timestamp': timestamp(), 
                    'slotstate': slot_matrix}
        else:
            slot_matrix = ""
            for slot in self.lrms.torus_block:
                slot_vector = ""
                if slot[self.TORUS_BLOCK_STATUS] == FREE:
                    slot_vector = " - " * self.lrms.cores_per_node
                else:
                    slot_vector = " X " * self.lrms.cores_per_node
                slot_matrix += "%s: %s\n" % (slot[self.TORUS_BLOCK_NAME].ljust(24), slot_vector)
            return slot_matrix

    # --------------------------------------------------------------------------
    #
    # Allocate a number of cores
    #
    # Currently only implements full-node allocation, so core count must
    # be a multiple of cores_per_node.
    #
    def allocate_slot(self, cores_requested):

        block = self.lrms.torus_block
        sub_block_shape_table = self.lrms.shape_table

        self.log.info("Trying to allocate %d core(s)." % cores_requested)

        if cores_requested % self.lrms.cores_per_node:
            num_cores = int(math.ceil(cores_requested / float(self.lrms.cores_per_node))) \
                        * self.lrms.cores_per_node
            self.log.error('Core not a multiple of %d, increasing request to %d!' %
                           (self.lrms.cores_per_node, num_cores))

        num_nodes = cores_requested / self.lrms.cores_per_node

        offset = self._alloc_sub_block(block, num_nodes)

        if offset is None:
            self.log.warning('No allocation made.')
            return

        # TODO: return something else than corner location? Corner index?
        corner = block[offset][self.TORUS_BLOCK_COOR]
        sub_block_shape = sub_block_shape_table[num_nodes]

        end = self.get_last_node(corner, sub_block_shape)
        self.log.debug('Allocating sub-block of %d node(s) with dimensions %s'
                       ' at offset %d with corner %s and end %s.' %
                       (num_nodes, self.lrms.shape2str(sub_block_shape), offset,
                        self.lrms.loc2str(corner), self.lrms.loc2str(end)))

        return corner, sub_block_shape
    #
    ##########################################################################

    ##########################################################################
    #
    # Allocate a sub-block within a block
    # Currently only works with offset that are exactly the sub-block size
    #
    def _alloc_sub_block(self, block, num_nodes):

        offset = 0
        # Iterate through all nodes with offset a multiple of the sub-block size
        while True:

            # Verify the assumption (needs to be an assert?)
            if offset % num_nodes != 0:
                msg = 'Sub-block needs to start at correct offset!'
                self.log.exception(msg)
                raise Exception(msg)
                # TODO: If we want to workaround this, the coordinates need to overflow

            not_free = False
            # Check if all nodes from offset till offset+size are FREE
            for peek in range(num_nodes):
                try:
                    if block[offset+peek][self.TORUS_BLOCK_STATUS] == BUSY:
                        # Once we find the first BUSY node we can discard this attempt
                        not_free = True
                        break
                except IndexError:
                    self.log.error('Block out of bound. Num_nodes: %d, offset: %d, peek: %d.' %(
                        num_nodes, offset, peek))

            if not_free == True:
                # No success at this offset
                self.log.info("No free nodes found at this offset: %d." % offset)

                # If we weren't the last attempt, then increase the offset and iterate again.
                if offset + num_nodes < self._block2num_nodes(block):
                    offset += num_nodes
                    continue
                else:
                    return

            else:
                # At this stage we have found a free spot!

                self.log.info("Free nodes found at this offset: %d." % offset)

                # Then mark the nodes busy
                for peek in range(num_nodes):
                    block[offset+peek][self.TORUS_BLOCK_STATUS] = BUSY

                return offset
    #
    ##########################################################################

    ##########################################################################
    #
    # Return the number of nodes in a block
    #
    def _block2num_nodes(self, block):
        return len(block)
    #
    ##########################################################################

    # --------------------------------------------------------------------------
    #
    def release_slot(self, (corner, shape)):
        self._free_cores(self.lrms.torus_block, corner, shape)

        # something changed - write history!
        # AM: mongodb entries MUST NOT grow larger than 16MB, or chaos will
        # ensue.  We thus limit the slot history size to 4MB, to keep sufficient
        # space for the actual operational data
        if len(str(self._slot_history)) < 4 * 1024 * 1024 :
            self._slot_history.append(self.slot_status(short=True))
        else:
            # just replace the last entry with the current one.
            self._slot_history[-1] = self.slot_status(short=True)


    ##########################################################################
    #
    # Free up an allocation
    #
    def _free_cores(self, block, corner, shape):

        # Number of nodes to free
        num_nodes = self._shape2num_nodes(shape)

        # Location of where to start freeing
        offset = self.corner2offset(block, corner)

        self.log.info("Freeing %d nodes starting at %d." % (num_nodes, offset))

        for peek in range(num_nodes):
            assert block[offset+peek][self.TORUS_BLOCK_STATUS] == BUSY, \
                'Block %d not Free!' % block[offset+peek]
            block[offset+peek][self.TORUS_BLOCK_STATUS] = FREE
    #
    ##########################################################################

    ##########################################################################
    #
    # Follow coordinates to get the last node
    #
    def get_last_node(self, origin, shape):
        return {dim: origin[dim] + shape[dim] -1 for dim in self.lrms.torus_dimension_labels}
    #
    ##########################################################################

    ##########################################################################
    #
    # Return the number of nodes for the given block shape
    #
    def _shape2num_nodes(self, shape):

        nodes = 1
        for dim in self.lrms.torus_dimension_labels:
            nodes *= shape[dim]

        return nodes
    #
    ##########################################################################

    ##########################################################################
    #
    # Return the offset into the node list from a corner
    #
    # TODO: Can this be determined instead of searched?
    #
    def corner2offset(self, block, corner):
        offset = 0

        for e in block:
            if corner == e[self.TORUS_BLOCK_COOR]:
                return offset
            offset += 1

        return offset
    #
    ##########################################################################


# ==============================================================================
#
# Launch Methods
#
# ==============================================================================
class LaunchMethod(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):

        self.name = name
        self.scheduler = scheduler
        self.log = logger

        self.launch_command = None
        self.configure()
        # TODO: This doesn't make too much sense for LM's that use multiple 
        #       commands, perhaps this needs to move to per LM __init__.
        if self.launch_command is None:
            raise Exception("Launch command not found for LaunchMethod '%s'" % name)

        logger.info("Discovered launch command: '%s'." % self.launch_command)

    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Launch Method.
    #
    @classmethod
    def create(cls, name, scheduler, logger):

        # Make sure that we are the base-class!
        if cls != LaunchMethod:
            raise Exception("LaunchMethod factory only available to base class!")

        try:
            implementation = {
                LAUNCH_METHOD_APRUN         : LaunchMethodAPRUN,
                LAUNCH_METHOD_CCMRUN        : LaunchMethodCCMRUN,
                LAUNCH_METHOD_DPLACE        : LaunchMethodDPLACE,
                LAUNCH_METHOD_FORK          : LaunchMethodFORK,
                LAUNCH_METHOD_IBRUN         : LaunchMethodIBRUN,
                LAUNCH_METHOD_MPIEXEC       : LaunchMethodMPIEXEC,
                LAUNCH_METHOD_MPIRUN_CCMRUN : LaunchMethodMPIRUNCCMRUN,
                LAUNCH_METHOD_MPIRUN_DPLACE : LaunchMethodMPIRUNDPLACE,
                LAUNCH_METHOD_MPIRUN        : LaunchMethodMPIRUN,
                LAUNCH_METHOD_MPIRUN_RSH    : LaunchMethodMPIRUNRSH,
                LAUNCH_METHOD_POE           : LaunchMethodPOE,
                LAUNCH_METHOD_RUNJOB        : LaunchMethodRUNJOB,
                LAUNCH_METHOD_SSH           : LaunchMethodSSH
            }[name]
            return implementation(name, scheduler, logger)
        except KeyError:
            raise Exception("LaunchMethod '%s' unknown!" % name)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        raise NotImplementedError("configure() not implemented for LaunchMethod: %s." % self.name)

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores, 
                          launch_script_name, opaque_slot):
        raise NotImplementedError("construct_command() not implemented for LaunchMethod: %s." % self.name)

    # --------------------------------------------------------------------------
    #
    def _find_executable(self, names):
        """Takes a (list of) name(s) and looks for an executable in the path.
        """

        if not isinstance(names, list):
            names = [names]

        for name in names:
            ret = self._which(name)
            if ret is not None:
                return ret

        return None

    # --------------------------------------------------------------------------
    #
    def _which(self, program):
        """Finds the location of an executable.
        Taken from: 
        http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
        """
        # ----------------------------------------------------------------------
        #
        def is_exe(fpath):
            return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

        fpath, fname = os.path.split(program)
        if fpath:
            if is_exe(program):
                return program
        else:
            for path in os.environ["PATH"].split(os.pathsep):
                exe_file = os.path.join(path, program)
                if is_exe(exe_file):
                    return exe_file
        return None


# ------------------------------------------------------------------------
#
class LaunchMethodFORK(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # "Regular" tasks
        self.launch_command = ''

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, opaque_slot):

        if task_args:
            command = " ".join([task_exec, task_args])
        else:
            command = task_exec

        return command, launch_script_name


# ------------------------------------------------------------------------
#
class LaunchMethodMPIRUN(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        self.launch_command = self._find_executable([
            'mpirun',            # General case
            'mpirun_rsh',        # Gordon @ SDSC
            'mpirun-mpich-mp',   # Mac OSX MacPorts
            'mpirun-openmpi-mp'  # Mac OSX MacPorts
        ])

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, (task_slots)):

        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        # Construct the hosts_string
        hosts_string = ",".join([slot.split(':')[0] for slot in task_slots])

        mpirun_command = "%s -np %s -host %s %s" % (
            self.launch_command, task_numcores, hosts_string, task_command)

        return mpirun_command, launch_script_name


# ------------------------------------------------------------------------
#
class LaunchMethodSSH(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # Find ssh command
        command = self._which('ssh')

        if command is not None:

            # Some MPI environments (e.g. SGE) put a link to rsh as "ssh" into 
            # the path.  We try to detect that and then use different arguments.
            if os.path.islink(command):

                target = os.path.realpath(command)

                if os.path.basename(target) == 'rsh':
                    self.log.info('Detected that "ssh" is a link to "rsh".')
                    return target

            command = '%s -o StrictHostKeyChecking=no' % command

        self.launch_command = command

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, (task_slots)):

        # Get the host of the first entry in the acquired slot
        host = task_slots[0].split(':')[0]

        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        # Command line to execute launch script via ssh on host
        ssh_cmdline = "%s %s %s" % (self.launch_command, host, launch_script_name)

        # Special case, return a tuple that overrides the default command line.
        return task_command, ssh_cmdline


# ------------------------------------------------------------------------
#
class LaunchMethodMPIEXEC(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # mpiexec (e.g. on SuperMUC)
        self.launch_command = self._which('mpiexec')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, (task_slots)):

        # Construct the hosts_string
        hosts_string = ",".join([slot.split(':')[0] for slot in task_slots])

        # Construct the executable and arguments
        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        mpiexec_command = "%s -n %s -host %s %s" % (
            self.launch_command, task_numcores, hosts_string, task_command)

        return mpiexec_command, launch_script_name


# ------------------------------------------------------------------------
#
class LaunchMethodAPRUN(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # aprun: job launcher for Cray systems
        self.launch_command= self._which('aprun')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, opaque_slot):

        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        aprun_command = "%s -n %d %s" % (self.launch_command, task_numcores, task_command)

        return aprun_command, launch_script_name


# ------------------------------------------------------------------------
#
class LaunchMethodCCMRUN(LaunchMethod):

    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    def configure(self):
        # ccmrun: Cluster Compatibility Mode (CCM) job launcher for Cray systems
        self.launch_command= self._which('ccmrun')

    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, opaque_slot):

        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        ccmrun_command = "%s -n %d %s" % (self.launch_command, task_numcores, task_command)

        return ccmrun_command, launch_script_name


#-------------------------------------------------------------------------
#
class LaunchMethodMPIRUNCCMRUN(LaunchMethod):
    # TODO: This needs both mpirun and ccmrun

    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    def configure(self):
        # ccmrun: Cluster Compatibility Mode job launcher for Cray systems
        self.launch_command= self._which('ccmrun')

        self.mpirun_command = self._which('mpirun')
        if not self.mpirun_command:
            raise Exception("mpirun not found!")

    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, (task_slots)):

        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        # Construct the hosts_string
        # TODO: is there any use in using $HOME/.crayccm/ccm_nodelist.$JOBID?
        hosts_string = ",".join([slot.split(':')[0] for slot in task_slots])

        # TODO: Other mpirun LM's could probably also benefit from this!
        candidate_vars = [
            'LD_LIBRARY_PATH',
            'PATH',
            'PYTHONPATH'
            'PYTHON_DIR',
        ]
        export_vars = ' '.join(['-x ' + var for var in candidate_vars if var in os.environ])

        mpirun_ccmrun_command = "%s %s %s -np %d -host %s %s" % (
            self.launch_command, self.mpirun_command, export_vars,
            task_numcores, hosts_string, task_command)

        return mpirun_ccmrun_command, launch_script_name


#-------------------------------------------------------------------------
#
class LaunchMethodRUNJOB(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # runjob: job launcher for IBM BG/Q systems, e.g. Joule
        self.launch_command= self._which('runjob')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, (corner, sub_block_shape)):

        if task_numcores % self.scheduler.lrms.cores_per_node: 
            msg = "Num cores (%d) is not a multiple of %d!" % (
                task_numcores, self.scheduler.lrms.cores_per_node)
            self.log.exception(msg)
            raise Exception(msg)

        # Runjob it is!
        runjob_command = self.launch_command

        # Set the number of tasks/ranks per node
        # TODO: Currently hardcoded, this should be configurable,
        #       but I don't see how, this would be a leaky abstraction.
        runjob_command += ' --ranks-per-node %d' % min(self.scheduler.lrms.cores_per_node, task_numcores)

        # Run this subjob in the block communicated by LoadLeveler
        runjob_command += ' --block %s' % self.scheduler.lrms.loadl_bg_block

        corner_offset = self.scheduler.corner2offset(self.scheduler.lrms.torus_block, corner)
        corner_node = self.scheduler.lrms.torus_block[corner_offset][self.scheduler.TORUS_BLOCK_NAME]
        runjob_command += ' --corner %s' % corner_node

        # convert the shape
        runjob_command += ' --shape %s' % self.scheduler.lrms.shape2str(sub_block_shape)

        # runjob needs the full path to the executable
        if os.path.basename(task_exec) == task_exec:
            if not self._which(task_exec):
                raise Exception("Can't find executable '%s' in path." % task_exec)

            # Use `which` with back-ticks as the executable,
            # will be expanded in the shell script.
            task_exec = '`which %s`' % task_exec
            # Note: We can't use the expansion from here,
            #       as the pre-execs of the CU aren't run yet!!

        # And finally add the executable and the arguments
        # usage: runjob <runjob flags> --exe /bin/hostname --args "-f"
        runjob_command += ' --exe %s' % task_exec
        if task_args:
            runjob_command += ' --args %s' % task_args

        return runjob_command, launch_script_name


# ------------------------------------------------------------------------
#
class LaunchMethodDPLACE(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # dplace: job launcher for SGI systems (e.g. on Blacklight)
        self.launch_command = self._which('dplace')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, (task_slots)):

        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        dplace_offset = self.scheduler.slots2offset(task_slots)

        dplace_command = "%s -c %d-%d %s" % (
            self.launch_command, dplace_offset, 
            dplace_offset+task_numcores-1, task_command)

        return dplace_command, launch_script_name


# ------------------------------------------------------------------------
#
class LaunchMethodMPIRUNRSH(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # mpirun_rsh (e.g. on Gordon@ SDSC)
        self.launch_command = self._which('mpirun_rsh')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, (task_slots)):

        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        # Construct the hosts_string ('h1 h2 .. hN')
        hosts_string = " ".join([slot.split(':')[0] for slot in task_slots])

        mpirun_rsh_command = "%s -export -np %s %s %s" % (
            self.launch_command, task_numcores, hosts_string, task_command)

        return mpirun_rsh_command, launch_script_name


# ------------------------------------------------------------------------
#
class LaunchMethodMPIRUNDPLACE(LaunchMethod):
    # TODO: This needs both mpirun and dplace

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # dplace: job launcher for SGI systems (e.g. on Blacklight)
        self.launch_command = self._which('dplace')
        self.mpirun_command = self._which('mpirun')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, (task_slots)):

        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        dplace_offset = self.scheduler.slots2offset(task_slots)

        mpirun_dplace_command = "%s -np %d %s -c %d-%d %s" % \
            (self.mpirun_command, task_numcores, self.launch_command,
             dplace_offset, dplace_offset+task_numcores-1, task_command)

        return mpirun_dplace_command, launch_script_name


# ------------------------------------------------------------------------
#
class LaunchMethodIBRUN(LaunchMethod):
    # NOTE: Don't think that with IBRUN it is possible to have
    # processes != cores ...

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # ibrun: wrapper for mpirun at TACC
        self.launch_command = self._which('ibrun')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, (task_slots)):

        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        ibrun_offset = self.scheduler.slots2offset(task_slots)

        ibrun_command = "%s -n %s -o %d %s" % \
                        (self.launch_command, task_numcores, 
                         ibrun_offset, task_command)

        return ibrun_command, launch_script_name


# ------------------------------------------------------------------------
#
class LaunchMethodPOE(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, scheduler, logger):
        LaunchMethod.__init__(self, name, scheduler, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # poe: LSF specific wrapper for MPI (e.g. yellowstone)
        self.launch_command = self._which('poe')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, task_exec, task_args, task_numcores,
                          launch_script_name, (task_slots)):

        # Count slots per host in provided slots description.
        hosts = {}
        for slot in task_slots:
            host = slot.split(':')[0]
            if host not in hosts:
                hosts[host] = 1
            else:
                hosts[host] += 1

        # Create string with format: "hostX N host
        hosts_string = ''
        for host in hosts:
            hosts_string += '%s %d ' % (host, hosts[host])

        if task_args:
            task_command = " ".join([task_exec, task_args])
        else:
            task_command = task_exec

        # Override the LSB_MCPU_HOSTS env variable as this is set by
        # default to the size of the whole pilot.
        poe_command = 'LSB_MCPU_HOSTS="%s" %s %s' % (
            hosts_string, self.launch_command, task_command)

        return poe_command, launch_script_name


# ==============================================================================
#
# Base class for LRMS implementations.
#
# ==============================================================================
#
class LRMS(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, requested_cores, logger):

        self.name = name
        self.log = logger
        self.requested_cores = requested_cores

        self.log.info("Configuring LRMS %s." % self.name)

        self.slot_list = []
        self.node_list = []
        self.cores_per_node = None

        self.configure()

        logger.info("Discovered execution environment: %s" % self.node_list)

        # For now assume that all nodes have equal amount of cores
        cores_avail = len(self.node_list) * self.cores_per_node
        if cores_avail < int(requested_cores):
            raise Exception("Not enough cores available (%s) to satisfy allocation request (%s)." \
                            % (str(cores_avail), str(requested_cores)))

    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the LRMS.
    #
    @classmethod
    def create(cls, name, requested_cores, logger):

        # TODO: Core counts dont have to be the same number for all hosts.

        # TODO: We might not have reserved the whole node.

        # TODO: Given that the Agent can determine the real core count, in 
        #       principle we could just ignore the config and use as many as we
        #       have to our availability (taken into account that we might not
        #       have the full node reserved of course)
        #       Answer: at least on Yellowstone this doesnt work for MPI,
        #               as you can't spawn more tasks then the number of slots.

        # Make sure that we are the base-class!
        if cls != LRMS:
            raise Exception("LRMS Factory only available to base class!")

        try:
            implementation = {
                LRMS_NAME_FORK        : ForkLRMS,
                LRMS_NAME_LOADLEVELER : LoadLevelerLRMS,
                LRMS_NAME_LSF         : LSFLRMS,
                LRMS_NAME_PBSPRO      : PBSProLRMS,
                LRMS_NAME_SGE         : SGELRMS,
                LRMS_NAME_SLURM       : SLURMLRMS,
                LRMS_NAME_TORQUE      : TORQUELRMS
            }[name]
            return implementation(name, requested_cores, logger)
        except KeyError:
            raise Exception("LRMS type '%s' unknown!" % name)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        raise NotImplementedError("Configure not implemented for LRMS type: %s." % self.name)


# ------------------------------------------------------------------------------
#
class TORQUELRMS(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, requested_cores, logger):
        LRMS.__init__(self, name, requested_cores, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):

        self.log.info("Configured to run on system with %s." % self.name)

        torque_nodefile = os.environ.get('PBS_NODEFILE')
        if torque_nodefile is None:
            msg = "$PBS_NODEFILE not set!"
            self.log.error(msg)
            raise Exception(msg)

        # Parse PBS the nodefile
        torque_nodes = [line.strip() for line in open(torque_nodefile)]
        self.log.info("Found Torque PBS_NODEFILE %s: %s" % (torque_nodefile, torque_nodes))

        # Number of cpus involved in allocation
        val = os.environ.get('PBS_NCPUS')
        if val:
            torque_num_cpus = int(val)
        else:
            msg = "$PBS_NCPUS not set! (new Torque version?)"
            torque_num_cpus = None
            self.log.warning(msg)

        # Number of nodes involved in allocation
        val = os.environ.get('PBS_NUM_NODES')
        if val:
            torque_num_nodes = int(val)
        else:
            msg = "$PBS_NUM_NODES not set! (old Torque version?)"
            torque_num_nodes = None
            self.log.warning(msg)

        # Number of cores (processors) per node
        val = os.environ.get('PBS_NUM_PPN')
        if val:
            torque_cores_per_node = int(val)
        else:
            msg = "$PBS_NUM_PPN is not set!"
            torque_cores_per_node = None
            self.log.warning(msg)

        print "torque_cores_per_node : %s" % torque_cores_per_node
        if torque_cores_per_node in [None, 1] :
            # lets see if SAGA has been forthcoming with some information
            self.log.warning("fall back to $SAGA_PPN : %s" % os.environ.get ('SAGA_PPN', None))
            torque_cores_per_node = int(os.environ.get('SAGA_PPN', torque_cores_per_node))

        # Number of entries in nodefile should be PBS_NUM_NODES * PBS_NUM_PPN
        torque_nodes_length = len(torque_nodes)
        torque_node_list    = list(set(torque_nodes))

        print "torque_cores_per_node : %s" % torque_cores_per_node
        print "torque_nodes_length   : %s" % torque_nodes_length
        print "torque_num_nodes      : %s" % torque_num_nodes
        print "torque_node_list      : %s" % torque_node_list
        print "torque_nodes          : %s" % torque_nodes

      # if torque_num_nodes and torque_cores_per_node and \
      #     torque_nodes_length < torque_num_nodes * torque_cores_per_node:
      #     msg = "Number of entries in $PBS_NODEFILE (%s) does not match with $PBS_NUM_NODES*$PBS_NUM_PPN (%s*%s)" % \
      #           (torque_nodes_length, torque_num_nodes,  torque_cores_per_node)
      #     raise Exception(msg)

        # only unique node names
        torque_node_list_length = len(torque_node_list)
        self.log.debug("Node list: %s(%d)" % (torque_node_list, torque_node_list_length))

        if torque_num_nodes and torque_cores_per_node:
            # Modern style Torque
            self.cores_per_node = torque_cores_per_node
        elif torque_num_cpus:
            # Blacklight style (TORQUE-2.3.13)
            self.cores_per_node = torque_num_cpus
        else:
            # Old style Torque (Should we just use this for all versions?)
            self.cores_per_node = torque_nodes_length / torque_node_list_length
        self.node_list = torque_node_list


# ------------------------------------------------------------------------
#
class PBSProLRMS(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, requested_cores, logger):
        LRMS.__init__(self, name, requested_cores, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):
        # TODO: $NCPUS?!?! = 1 on archer

        pbspro_nodefile = os.environ.get('PBS_NODEFILE')

        if pbspro_nodefile is None:
            msg = "$PBS_NODEFILE not set!"
            self.log.error(msg)
            raise Exception(msg)

        self.log.info("Found PBSPro $PBS_NODEFILE %s." % pbspro_nodefile)

        # Dont need to parse the content of nodefile for PBSPRO, only the length
        # is interesting, as there are only duplicate entries in it.
        pbspro_nodes_length = len([line.strip() for line in open(pbspro_nodefile)])

        # Number of Processors per Node
        val = os.environ.get('NUM_PPN')
        if val:
            pbspro_num_ppn = int(val)
        else:
            msg = "$NUM_PPN not set!"
            self.log.error(msg)
            raise Exception(msg)

        # Number of Nodes allocated
        val = os.environ.get('NODE_COUNT')
        if val:
            pbspro_node_count = int(val)
        else:
            msg = "$NODE_COUNT not set!"
            self.log.error(msg)
            raise Exception(msg)

        # Number of Parallel Environments
        val = os.environ.get('NUM_PES')
        if val:
            pbspro_num_pes = int(val)
        else:
            msg = "$NUM_PES not set!"
            self.log.error(msg)
            raise Exception(msg)

        pbspro_vnodes = self._parse_pbspro_vnodes()

        # Verify that $NUM_PES == $NODE_COUNT * $NUM_PPN == len($PBS_NODEFILE)
        if not (pbspro_node_count * pbspro_num_ppn == pbspro_num_pes == pbspro_nodes_length):
            self.log.warning("NUM_PES != NODE_COUNT * NUM_PPN != len($PBS_NODEFILE)")

        self.cores_per_node = pbspro_num_ppn
        self.node_list = pbspro_vnodes

    # --------------------------------------------------------------------------
    #
    def _parse_pbspro_vnodes(self):

        # PBS Job ID
        val = os.environ.get('PBS_JOBID')
        if val:
            pbspro_jobid = val
        else:
            msg = "$PBS_JOBID not set!"
            self.log.error(msg)
            raise Exception(msg)

        # Get the output of qstat -f for this job
        output = subprocess.check_output(["qstat", "-f", pbspro_jobid])

        # Get the (multiline) 'exec_vnode' entry
        vnodes_str = ''
        for line in output.splitlines():
            # Detect start of entry
            if 'exec_vnode = ' in line:
                vnodes_str += line.strip()
            elif vnodes_str:
                # Find continuing lines
                if " = " not in line:
                    vnodes_str += line.strip()
                else:
                    break

        # Get the RHS of the entry
        input = vnodes_str.split('=',1)[1].strip()
        self.log.debug("input: %s" % input)

        nodes_list = []
        # Break up the individual node partitions into vnode slices
        while True:
            idx = input.find(')+(')

            node_str = input[1:idx]
            nodes_list.append(node_str)
            input = input[idx+2:]

            if idx < 0:
                break

        vnodes_list = []
        cpus_list = []
        # Split out the slices into vnode name and cpu count
        for node_str in nodes_list:
            slices = node_str.split('+')
            for slice in slices:
                vnode, cpus = slice.split(':')
                cpus = int(cpus.split('=')[1])
                self.log.debug('vnode: %s cpus: %s' % (vnode, cpus))
                vnodes_list.append(vnode)
                cpus_list.append(cpus)

        self.log.debug("vnodes: %s" % vnodes_list)
        self.log.debug("cpus: %s" % cpus_list)

        cpus_list = list(set(cpus_list))
        min_cpus = int(min(cpus_list))

        if len(cpus_list) > 1:
            self.log.debug("Detected vnodes of different sizes: %s, the minimal is: %d." % (cpus_list, min_cpus))

        node_list = []
        for vnode in vnodes_list:
            # strip the last _0 of the vnodes to get the node name
            node_list.append(vnode.rsplit('_', 1)[0])

        # only unique node names
        node_list = list(set(node_list))
        self.log.debug("Node list: %s" % node_list)

        # Return the list of node names
        return node_list


# ------------------------------------------------------------------------
#
class SLURMLRMS(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, requested_cores, logger):
        LRMS.__init__(self, name, requested_cores, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):

        slurm_nodelist = os.environ.get('SLURM_NODELIST')
        if slurm_nodelist is None:
            msg = "$SLURM_NODELIST not set!"
            self.log.error(msg)
            raise Exception(msg)

        # Parse SLURM nodefile environment variable
        slurm_nodes = hostlist.expand_hostlist(slurm_nodelist)
        self.log.info("Found SLURM_NODELIST %s. Expanded to: %s" % (slurm_nodelist, slurm_nodes))

        # $SLURM_NPROCS = Total number of processes in the current job
        slurm_nprocs_str = os.environ.get('SLURM_NPROCS')
        if slurm_nprocs_str is None:
            msg = "$SLURM_NPROCS not set!"
            self.log.error(msg)
            raise Exception(msg)
        else:
            slurm_nprocs = int(slurm_nprocs_str)

        # $SLURM_NNODES = Total number of nodes in the job's resource allocation
        slurm_nnodes_str = os.environ.get('SLURM_NNODES')
        if slurm_nnodes_str is None:
            msg = "$SLURM_NNODES not set!"
            self.log.error(msg)
            raise Exception(msg)
        else:
            slurm_nnodes = int(slurm_nnodes_str)

        # $SLURM_CPUS_ON_NODE = Count of processors available to the job 
        # on this node.
        slurm_cpus_on_node_str = os.environ.get('SLURM_CPUS_ON_NODE')
        if slurm_cpus_on_node_str is None:
            msg = "$SLURM_CPUS_ON_NODE not set!"
            self.log.exception(msg)
        else:
            slurm_cpus_on_node = int(slurm_cpus_on_node_str)

        # Verify that $SLURM_NPROCS == $SLURM_NNODES * $SLURM_CPUS_ON_NODE
        if slurm_nnodes * slurm_cpus_on_node != slurm_nprocs:
            self.log.error("$SLURM_NPROCS(%d) != $SLURM_NNODES(%d) * $SLURM_CPUS_ON_NODE(%d)" % \
                           (slurm_nnodes, slurm_cpus_on_node, slurm_nprocs))

        # Verify that $SLURM_NNODES == len($SLURM_NODELIST)
        if slurm_nnodes != len(slurm_nodes):
            self.log.error("$SLURM_NNODES(%d) != len($SLURM_NODELIST)(%d)" % \
                           (slurm_nnodes, len(slurm_nodes)))

        self.cores_per_node = slurm_cpus_on_node
        self.node_list = slurm_nodes


# ------------------------------------------------------------------------
#
class SGELRMS(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, requested_cores, logger):
        LRMS.__init__(self, name, requested_cores, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):

        sge_hostfile = os.environ.get('PE_HOSTFILE')
        if sge_hostfile is None:
            msg = "$PE_HOSTFILE not set!"
            self.log.error(msg)
            raise Exception(msg)

        # SGE core configuration might be different than what multiprocessing 
        # announces
        # Alternative: "qconf -sq all.q|awk '/^slots *[0-9]+$/{print $2}'"

        # Parse SGE hostfile for nodes
        sge_node_list = [line.split()[0] for line in open(sge_hostfile)]
        # Keep only unique nodes
        sge_nodes = list(set(sge_node_list))
        self.log.info("Found PE_HOSTFILE %s. Expanded to: %s" % (sge_hostfile, sge_nodes))

        # Parse SGE hostfile for cores
        sge_cores_count_list = [int(line.split()[1]) for line in open(sge_hostfile)]
        sge_core_counts = list(set(sge_cores_count_list))
        sge_cores_per_node = min(sge_core_counts)
        self.log.info("Found unique core counts: %s Using: %d" % (sge_core_counts, sge_cores_per_node))

        self.node_list = sge_nodes
        self.cores_per_node = sge_cores_per_node


# ------------------------------------------------------------------------
#
class LSFLRMS(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, requested_cores, logger):
        LRMS.__init__(self, name, requested_cores, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):

        lsf_hostfile = os.environ.get('LSB_DJOB_HOSTFILE')
        if lsf_hostfile is None:
            msg = "$LSB_DJOB_HOSTFILE not set!"
            self.log.error(msg)
            raise Exception(msg)

        lsb_mcpu_hosts = os.environ.get('LSB_MCPU_HOSTS')
        if lsb_mcpu_hosts is None:
            msg = "$LSB_MCPU_HOSTS not set!"
            self.log.error(msg)
            raise Exception(msg)

        # parse LSF hostfile
        # format:
        # <hostnameX>
        # <hostnameX>
        # <hostnameY>
        # <hostnameY>
        #
        # There are in total "-n" entries (number of tasks)
        # and "-R" entries per host (tasks per host).
        # (That results in "-n" / "-R" unique hosts)
        #
        lsf_nodes = [line.strip() for line in open(lsf_hostfile)]
        self.log.info("Found LSB_DJOB_HOSTFILE %s. Expanded to: %s" %
                      (lsf_hostfile, lsf_nodes))
        lsf_node_list = list(set(lsf_nodes))

        # Grab the core (slot) count from the environment
        # Format: hostX N hostY N hostZ N
        lsf_cores_count_list = map(int, lsb_mcpu_hosts.split()[1::2])
        lsf_core_counts = list(set(lsf_cores_count_list))
        lsf_cores_per_node = min(lsf_core_counts)
        self.log.info("Found unique core counts: %s Using: %d" %
                      (lsf_core_counts, lsf_cores_per_node))

        self.node_list = lsf_node_list
        self.cores_per_node = lsf_cores_per_node


# ------------------------------------------------------------------------
#
class LoadLevelerLRMS(LRMS):

    # --------------------------------------------------------------------------
    #
    # BG/Q Topology of Nodes within a Board
    #
    BGQ_BOARD_TOPO = {
        0: {'A': 29, 'B':  3, 'C':  1, 'D': 12, 'E':  7},
        1: {'A': 28, 'B':  2, 'C':  0, 'D': 13, 'E':  6},
        2: {'A': 31, 'B':  1, 'C':  3, 'D': 14, 'E':  5},
        3: {'A': 30, 'B':  0, 'C':  2, 'D': 15, 'E':  4},
        4: {'A': 25, 'B':  7, 'C':  5, 'D':  8, 'E':  3},
        5: {'A': 24, 'B':  6, 'C':  4, 'D':  9, 'E':  2},
        6: {'A': 27, 'B':  5, 'C':  7, 'D': 10, 'E':  1},
        7: {'A': 26, 'B':  4, 'C':  6, 'D': 11, 'E':  0},
        8: {'A': 21, 'B': 11, 'C':  9, 'D':  4, 'E': 15},
        9: {'A': 20, 'B': 10, 'C':  8, 'D':  5, 'E': 14},
        10: {'A': 23, 'B':  9, 'C': 11, 'D':  6, 'E': 13},
        11: {'A': 22, 'B':  8, 'C': 10, 'D':  7, 'E': 12},
        12: {'A': 17, 'B': 15, 'C': 13, 'D':  0, 'E': 11},
        13: {'A': 16, 'B': 14, 'C': 12, 'D':  1, 'E': 10},
        14: {'A': 19, 'B': 13, 'C': 15, 'D':  2, 'E':  9},
        15: {'A': 18, 'B': 12, 'C': 14, 'D':  3, 'E':  8},
        16: {'A': 13, 'B': 19, 'C': 17, 'D': 28, 'E': 23},
        17: {'A': 12, 'B': 18, 'C': 16, 'D': 29, 'E': 22},
        18: {'A': 15, 'B': 17, 'C': 19, 'D': 30, 'E': 21},
        19: {'A': 14, 'B': 16, 'C': 18, 'D': 31, 'E': 20},
        20: {'A':  9, 'B': 23, 'C': 21, 'D': 24, 'E': 19},
        21: {'A':  8, 'B': 22, 'C': 20, 'D': 25, 'E': 18},
        22: {'A': 11, 'B': 21, 'C': 23, 'D': 26, 'E': 17},
        23: {'A': 10, 'B': 20, 'C': 22, 'D': 27, 'E': 16},
        24: {'A':  5, 'B': 27, 'C': 25, 'D': 20, 'E': 31},
        25: {'A':  4, 'B': 26, 'C': 24, 'D': 21, 'E': 30},
        26: {'A':  7, 'B': 25, 'C': 27, 'D': 22, 'E': 29},
        27: {'A':  6, 'B': 24, 'C': 26, 'D': 23, 'E': 28},
        28: {'A':  1, 'B': 31, 'C': 29, 'D': 16, 'E': 27},
        29: {'A':  0, 'B': 30, 'C': 28, 'D': 17, 'E': 26},
        30: {'A':  3, 'B': 29, 'C': 31, 'D': 18, 'E': 25},
        31: {'A':  2, 'B': 28, 'C': 30, 'D': 19, 'E': 24},
        }

    # --------------------------------------------------------------------------
    #
    # BG/Q Config
    #
    BGQ_CORES_PER_NODE      = 16
    BGQ_NODES_PER_BOARD     = 32 # NODE == Compute Card == Chip module
    BGQ_BOARDS_PER_MIDPLANE = 16 # NODE BOARD == NODE CARD
    BGQ_MIDPLANES_PER_RACK  = 2


    # --------------------------------------------------------------------------
    #
    # Default mapping = "ABCDE(T)"
    #
    # http://www.redbooks.ibm.com/redbooks/SG247948/wwhelp/wwhimpl/js/html/wwhelp.htm
    #
    BGQ_MAPPING = "ABCDE"


    # --------------------------------------------------------------------------
    #
    # Board labels (Rack, Midplane, Node)
    #
    BGQ_BOARD_LABELS = ['R', 'M', 'N']


    # --------------------------------------------------------------------------
    #
    # Dimensions of a (sub-)block
    #
    BGQ_DIMENSION_LABELS = ['A', 'B', 'C', 'D', 'E']


    # --------------------------------------------------------------------------
    #
    # Supported sub-block sizes (number of nodes).
    # This influences the effectiveness of mixed-size allocations
    # (and might even be a hard requirement from a topology standpoint).
    #
    # TODO: Do we actually need to restrict our sub-block sizes to this set?
    #
    BGQ_SUPPORTED_SUB_BLOCK_SIZES = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]


    # --------------------------------------------------------------------------
    #
    # Mapping of starting corners.
    #
    # "board" -> "node"
    #
    # Ordering: ['E', 'D', 'DE', etc.]
    #
    # TODO: Is this independent of the mapping?
    #
    BGQ_BLOCK_STARTING_CORNERS = {
        0:  0,
        4: 29,
        8:  4,
        12: 25
    }


    # --------------------------------------------------------------------------
    #
    # BG/Q Topology of Boards within a Midplane
    #
    BGQ_MIDPLANE_TOPO = {
        0: {'A':  4, 'B':  8, 'C':  1, 'D':  2},
        1: {'A':  5, 'B':  9, 'C':  0, 'D':  3},
        2: {'A':  6, 'B': 10, 'C':  3, 'D':  0},
        3: {'A':  7, 'B': 11, 'C':  2, 'D':  1},
        4: {'A':  0, 'B': 12, 'C':  5, 'D':  6},
        5: {'A':  1, 'B': 13, 'C':  4, 'D':  7},
        6: {'A':  2, 'B': 14, 'C':  7, 'D':  4},
        7: {'A':  3, 'B': 15, 'C':  6, 'D':  5},
        8: {'A': 12, 'B':  0, 'C':  9, 'D': 10},
        9: {'A': 13, 'B':  1, 'C':  8, 'D': 11},
        10: {'A': 14, 'B':  2, 'C': 11, 'D':  8},
        11: {'A': 15, 'B':  3, 'C': 10, 'D':  9},
        12: {'A':  8, 'B':  4, 'C': 13, 'D': 14},
        13: {'A':  9, 'B':  5, 'C': 12, 'D': 15},
        14: {'A': 10, 'B':  6, 'C': 15, 'D': 12},
        15: {'A': 11, 'B':  7, 'C': 14, 'D': 13},
        }


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, requested_cores, logger):
        LRMS.__init__(self, name, requested_cores, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):

        # Determine method for determining hosts,
        # either through hostfile or BG/Q environment.
        loadl_hostfile = os.environ.get('LOADL_HOSTFILE')
        self.loadl_bg_block = os.environ.get('LOADL_BG_BLOCK')
        if loadl_hostfile is None and self.loadl_bg_block is None:
            msg = "Neither $LOADL_HOSTFILE or $LOADL_BG_BLOCK set!"
            self.log.error(msg)
            raise Exception(msg)

        # Determine the size of the pilot allocation
        if loadl_hostfile is not None:
            # Non Blue Gene Load Leveler installation.

            loadl_total_tasks_str = os.environ.get('LOADL_TOTAL_TASKS')
            if loadl_total_tasks_str is None:
                msg = "$LOADL_TOTAL_TASKS not set!"
                self.log.error(msg)
                raise Exception(msg)
            else:
                loadl_total_tasks = int(loadl_total_tasks_str)

            # Construct the host list
            loadl_nodes = [line.strip() for line in open(loadl_hostfile)]
            self.log.info("Found LOADL_HOSTFILE %s. Expanded to: %s" %
                          (loadl_hostfile, loadl_nodes))
            loadl_node_list = list(set(loadl_nodes))

            # Verify that $LLOAD_TOTAL_TASKS == len($LOADL_HOSTFILE)
            if loadl_total_tasks != len(loadl_nodes):
                self.log.error("$LLOAD_TOTAL_TASKS(%d) != len($LOADL_HOSTFILE)(%d)" % \
                               (loadl_total_tasks, len(loadl_nodes)))

            # Determine the number of cpus per node.  Assume: 
            # cores_per_node = lenght(nodefile) / len(unique_nodes_in_nodefile)
            loadl_cpus_per_node = len(loadl_nodes) / len(loadl_node_list)

        elif self.loadl_bg_block is not None:
            # Blue Gene specific.

            loadl_bg_size_str = os.environ.get('LOADL_BG_SIZE')
            if loadl_bg_size_str is None:
                msg = "$LOADL_BG_SIZE not set!"
                self.log.error(msg)
                raise Exception(msg)
            else:
                loadl_bg_size = int(loadl_bg_size_str)

            loadl_job_name = os.environ.get('LOADL_JOB_NAME')
            if loadl_job_name is None:
                msg = "$LOADL_JOB_NAME not set!"
                self.log.error(msg)
                raise Exception(msg)

            # Get the board list and block shape from 'llq -l' output
            output = subprocess.check_output(["llq", "-l", loadl_job_name])
            loadl_bg_board_list_str = None
            loadl_bg_block_shape_str = None
            for line in output.splitlines():
                # Detect BG board list
                if "BG Node Board List: " in line:
                    loadl_bg_board_list_str = line.split(':')[1].strip()
                elif "BG Shape Allocated: " in line:
                    loadl_bg_block_shape_str = line.split(':')[1].strip()
            if not loadl_bg_board_list_str:
                msg = "No board list found in llq output!"
                self.log.error(msg)
                raise Exception(msg)
            if not loadl_bg_block_shape_str:
                msg = "No board shape found in llq output!"
                self.log.error(msg)
                raise Exception(msg)

            self.torus_dimension_labels = self.BGQ_DIMENSION_LABELS

            # Build nodes data structure to be handled by Torus Scheduler
            self.torus_block = self._bgq_shapeandboards2block(
                loadl_bg_block_shape_str, loadl_bg_board_list_str)
            self.loadl_node_list = [entry[SchedulerTorus.TORUS_BLOCK_NAME] for entry in self.torus_block]

            # Construct sub-block table
            self.shape_table = self._bgq_create_sub_block_shape_table(loadl_bg_block_shape_str)

            # Determine the number of cpus per node
            loadl_cpus_per_node = self.BGQ_CORES_PER_NODE

        self.node_list = self.loadl_node_list
        self.cores_per_node = loadl_cpus_per_node


    # --------------------------------------------------------------------------
    #
    # Walk the block and return the node name for the given location
    #
    def _bgq_nodename_by_loc(self, rack, midplane, board, node, location):

        for dim in self.BGQ_DIMENSION_LABELS:
            max_length = location[dim]

            cur_length = 0
            # Loop while we are not at the final depth
            while cur_length < max_length:

                if cur_length % 2 == 0:
                    # If the current length is even,
                    # we remain within the board,
                    # and select the next node.
                    node = self.BGQ_BOARD_TOPO[node][dim]
                else:
                    # Otherwise we jump to another midplane.
                    board = self.BGQ_MIDPLANE_TOPO[board][dim]

                # Increase the length for the next iteration
                cur_length += 1

        return 'R%.2d-M%.1d-N%.2d-J%.2d' % (rack, midplane, board, node)


    # --------------------------------------------------------------------------
    #
    # Convert the board string as given by llq into a board structure
    #
    # E.g. 'R00-M1-N08,R00-M1-N09,R00-M1-N10,R00-M0-N11' =>
    # [{'R': 0, 'M': 1, 'N': 8}, {'R': 0, 'M': 1, 'N': 9},
    #  {'R': 0, 'M': 1, 'N': 10}, {'R': 0, 'M': 0, 'N': 11}]
    #
    def _bgq_str2boards(self, boards_str):

        boards = boards_str.split(',')

        board_dict_list = []

        for board in boards:
            elements = board.split('-')

            board_dict = {}
            for l, e in zip(self.BGQ_BOARD_LABELS, elements):
                board_dict[l] = int(e.split(l)[1])

            board_dict_list.append(board_dict)

        return board_dict_list


    # --------------------------------------------------------------------------
    #
    # Convert the string as given by llq into a block shape structure:
    #
    # E.g. '1x2x3x4x5' => {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5}
    #
    def _bgq_str2shape(self, shape_str):

        # Get the lengths of the shape
        shape_lengths = shape_str.split('x', 4)

        shape_dict = {}
        for dim, length in zip(self.BGQ_DIMENSION_LABELS, shape_lengths):
            shape_dict[dim] = int(length)

        return shape_dict


    # --------------------------------------------------------------------------
    #
    # Convert location dict into a tuple string
    # E.g. {'A': 1, 'C': 4, 'B': 1, 'E': 2, 'D': 4} => '(1,4,1,2,4)'
    #
    def loc2str(self, loc):
        return str(tuple(loc[dim] for dim in self.BGQ_DIMENSION_LABELS))


    # --------------------------------------------------------------------------
    #
    # Convert a shape dict into string format
    #
    # E.g. {'A': 1, 'C': 4, 'B': 1, 'E': 2, 'D': 4} => '1x4x1x2x4'
    #
    def shape2str(self, shape):

        shape_str = ''
        for l in self.BGQ_DIMENSION_LABELS:

            # Get the corresponding count
            shape_str += str(shape[l])

            # Add an 'x' behind all but the last label
            if l in self.BGQ_DIMENSION_LABELS[:-1]:
                shape_str += 'x'

        return shape_str


    # --------------------------------------------------------------------------
    #
    # Return list of nodes that make up the block
    #
    # Format: [(index, location, nodename, status), (i, c, n, s), ...]
    #
    def _bgq_get_block(self, rack, midplane, board, shape):

        nodes = []
        start_node = self.BGQ_BLOCK_STARTING_CORNERS[board]

        self.log.debug('Shape: %s' % shape)

        index = 0

        for a in range(shape['A']):
            for b in range(shape['B']):
                for c in range(shape['C']):
                    for d in range(shape['D']):
                        for e in range(shape['E']):
                            location = {'A': a, 'B': b, 'C': c, 'D': d, 'E':e}
                            nodename = self._bgq_nodename_by_loc(rack, midplane, board, start_node, location)
                            nodes.append([index, location, nodename, FREE])
                            index += 1
        return nodes


    # --------------------------------------------------------------------------
    #
    # Use block shape and board list to construct block structure
    #
    def _bgq_shapeandboards2block(self, block_shape_str, boards_str):

        board_dict_list = self._bgq_str2boards(boards_str)
        self.log.debug('Board dict list:\n%s' % '\n'.join([str(x) for x in board_dict_list]))

        # TODO: this assumes a single midplane block
        rack     = board_dict_list[0]['R']
        midplane = board_dict_list[0]['M']

        board_list = [entry['N'] for entry in board_dict_list]
        start_board = min(board_list)

        block_shape = self._bgq_str2shape(block_shape_str)

        return self._bgq_get_block(rack, midplane, start_board, block_shape)


    # --------------------------------------------------------------------------
    #
    # Construction of sub-block shapes based on overall block allocation.
    #
    def _bgq_create_sub_block_shape_table(self, shape_str):

        # Convert the shape string into dict structure
        block_shape = self._bgq_str2shape(shape_str)

        # Dict to store the results
        table = {}

        # Create a sub-block dict with shape 1x1x1x1x1
        sub_block_shape = {l: 1 for l in self.BGQ_DIMENSION_LABELS}

        # Look over all the dimensions starting at the most right
        for dim in self.BGQ_MAPPING[::-1]:
            while True:

                # Calculate the number of nodes for the current shape
                num_nodes = reduce(mul, filter(lambda length: length != 0, sub_block_shape.values()))

                if num_nodes in self.BGQ_SUPPORTED_SUB_BLOCK_SIZES:
                    table[num_nodes] = copy.copy(sub_block_shape)
                else:
                    self.log.warning("Non supported sub-block size: %d." % num_nodes)

                # Done with iterating this dimension
                if sub_block_shape[dim] >= block_shape[dim]:
                    break

                # Increase the length in this dimension for the next iteration.
                sub_block_shape[dim] += 1

        return table


# ------------------------------------------------------------------------
#
class ForkLRMS(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, requested_cores, logger):
        LRMS.__init__(self, name, requested_cores, logger)

    # --------------------------------------------------------------------------
    #
    def configure(self):

        self.log.info("Using fork on localhost.")

        detected_cpus = multiprocessing.cpu_count()
        selected_cpus = max(detected_cpus, self.requested_cores)
        # FIXME: max -> min

        self.log.info("Detected %d cores on localhost, using %d." % (detected_cpus, selected_cpus))

        self.node_list = ["localhost"]
        self.cores_per_node = selected_cpus


# ------------------------------------------------------------------------------
#
class Task(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid, executable, arguments, environment, numcores, mpi,
                 pre_exec, post_exec, workdir, stdout_file, stderr_file, 
                 agent_output_staging, ftw_output_staging):

        self._log         = None
        self._description = None

        # static task properties
        self.uid            = uid
        self.environment    = environment
        self.executable     = executable
        self.arguments      = arguments
        self.workdir        = workdir
        self.stdout_file    = stdout_file
        self.stderr_file    = stderr_file
        self.agent_output_staging = agent_output_staging
        self.ftw_output_staging = ftw_output_staging
        self.numcores       = numcores
        self.mpi            = mpi
        self.pre_exec       = pre_exec
        self.post_exec      = post_exec

        # Location
        self.slots          = None

        # dynamic task properties
        self.started        = None
        self.finished       = None

        self.state          = None
        self.exit_code      = None
        self.stdout         = ""
        self.stderr         = ""

        self._log           = []
        self._proc          = None


# ------------------------------------------------------------------------------
#
class ExecWorker(threading.Thread):
    """An ExecWorker competes for the execution of tasks in a task queue
    and writes the results back to MongoDB.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, exec_env, logger, execution_queue, command_queue,
                 output_staging_queue, mongodb_url, mongodb_name, mongodb_auth,
                 pilot_id, session_id, cu_environment, workdir):

        """Le Constructeur creates a new ExecWorker instance.
        """
        self._log = logger

        prof ('ExecWorker init')

        threading.Thread.__init__(self)
        self._terminate = threading.Event()

        self.cu_environment    = cu_environment
        self._workdir          = workdir
        self._pilot_id         = pilot_id
        self._slot_history_old = None

        mongo_client   = pymongo.MongoClient(mongodb_url)
        self._mongo_db = mongo_client[mongodb_name]

        if len(mongodb_auth) >= 3:
            user, pwd = mongodb_auth.split(':', 1)
            self._mongo_db.authenticate(user, pwd)

        self._p  = mongo_db["%s.p"  % session_id]
        self._cu = mongo_db["%s.cu" % session_id]

        # Queued tasks by the Agent
        self._execution_queue = execution_queue

        # Queued transfers
        self._output_staging_queue = output_staging_queue

        # Queued commands by the Agent
        self._command_queue = command_queue

        # Launched tasks by this ExecWorker
        self._running_tasks = []
        self._cuids_to_cancel = []

        # Container for scheduler, lrms and launch method.
        self.exec_env = exec_env

        self._p.update(
            {"_id": ObjectId(self._pilot_id)},
            {"$set": {"slothistory" : self.exec_env.scheduler._slot_history,
                      #"capability"  : 0,
                      #"slots"       : self.exec_env.scheduler._slots}
                     }
            })

    # --------------------------------------------------------------------------
    #
    def _slots2free(self, slots):
        """Convert slots structure into a free core count
        """

        free_cores = 0
        for node in slots:
            free_cores += node['cores'].count(FREE)

        return free_cores

    # --------------------------------------------------------------------------
    #
    def _slots2caps(self, slots):
        """Convert slots structure into a capability structure.
        """

        all_caps_tuples = {}
        for node in slots:
            free_cores = node['cores'].count(FREE)
            # (Free_cores, Continuous, Single_Node) = Count
            cap_tuple = (free_cores, False, True)

            if cap_tuple in all_caps_tuples:
                all_caps_tuples[cap_tuple] += 1
            else:
                all_caps_tuples[cap_tuple] = 1

        # Convert to please the gods of json and mongodb
        all_caps_dict = []
        for caps_tuple in all_caps_tuples:
            free_cores, cont, single = cap_tuple
            count = all_caps_tuples[cap_tuple]
            cap_dict = {'free_cores': free_cores, 'continuous': cont,
                        'single_node': single, 'count': count}
            all_caps_dict.append(cap_dict)

        return all_caps_dict

    # --------------------------------------------------------------------------
    #
    def stop(self):
        """Terminates the thread's main loop.
        """
        # AM: Why does this call exist?  It is never called....
        self._terminate.set()

    # --------------------------------------------------------------------------
    #
    def run(self):
        """Starts the thread when Thread.start() is called.
        """
        try:
            # report initial slot status
            # TODO: Where does this abstraction belong?
            self._log.debug(self.exec_env.scheduler.slot_status())

            while not self._terminate.isSet () :

                idle = True

                # See if there are commands for the worker!
                try:
                    command = self._command_queue.get_nowait()
                    if command[COMMAND_TYPE] == COMMAND_CANCEL_COMPUTE_UNIT:
                        self._cuids_to_cancel.append(command[COMMAND_ARG])
                    else:
                        raise Exception("Command %s not applicable in this context." %
                                        command[COMMAND_TYPE])
                except Queue.Empty:
                    # do nothing if we don't have any queued commands
                    pass

                
                try:
                    cu = self._execution_queue.get_nowait()

                except Queue.Empty:
                    # do nothing if we don't have any queued tasks
                    idle += self._check_running ()

                    if idle :
                        time.sleep (QUEUE_POLL_SLEEPTIME)
                    continue
                    

                # any work to do?
                if  cu :

                    prof ('ExecWorker gets cu from queue', uid=cu['uid'], tag='preprocess')

                    task_dir_name = "%s/unit-%s" % (self._workdir, cu['uid'])
                    stdout = cu["description"].get ('stdout')
                    stderr = cu["description"].get ('stderr')

                    if  stdout : stdout_file = task_dir_name+'/'+stdout
                    else       : stdout_file = task_dir_name+'/STDOUT'
                    if  stderr : stderr_file = task_dir_name+'/'+stderr
                    else       : stderr_file = task_dir_name+'/STDERR'

                    prof ('Task create', uid=cu['uid'])
                    task = Task(
                        uid         = cu['uid'],
                        executable  = cu["description"]["executable"],
                        arguments   = cu["description"]["arguments"],
                        environment = cu["description"]["environment"],
                        numcores    = cu["description"]["cores"],
                        mpi         = cu["description"]["mpi"],
                        pre_exec    = cu["description"]["pre_exec"],
                        post_exec   = cu["description"]["post_exec"],
                        workdir     = task_dir_name,
                        stdout_file = stdout_file,
                        stderr_file = stderr_file,
                        agent_output_staging = bool(cu['Agent_Output_Directives']),
                        ftw_output_staging   = bool(cu['FTW_Output_Directives'])
                        )

                    task.state  = ALLOCATING
                    opaque_slot = None

                    # FIXME: push scheduling state update into updater queue

                    try :

                        if task.mpi:
                            if not self.exec_env.mpi_launcher:
                                raise Exception("Can't launch MPI tasks without MPI launcher.")

                            launcher = self.exec_env.mpi_launcher
                            self._log.debug("Launching MPI task with %s (%s)." % (
                                    launcher.name, launcher.launch_command))
                        else:
                            if not self.exec_env.task_launcher:
                                raise Exception("Can't launch tasks without Task launcher.")

                            launcher = self.exec_env.task_launcher
                            self._log.debug("Launching task with %s (%s)." % (
                                launcher.name, launcher.launch_command))

                        # Call the scheduler for this task, and receive an 
                        # opaque handle that has meaning to the LRMS, Scheduler 
                        # and LaunchMethod.
                        opaque_slot = self.exec_env.scheduler.allocate_slot(task.numcores) 
                        
                        # Check if we got results
                        if opaque_slot is None:
                            # No resources available, put back in queue
                            self._execution_queue.put(cu)
                            prof ('ExecWorker returns task to queue', uid=task.uid)
                        else:
                            # got an allocation, go off and launch the process
                            task.opaque_slot = opaque_slot
                            self._launch_task(task, launcher)
                            idle = False

                    except Exception as e :
                        # append the startup error to the units stderr.  This is
                        # not completely correct (as this text is not produced
                        # by the unit), but it seems the most intuitive way to
                        # communicate that error to the application/user.
                        task.stderr += "\nPilot cannot start compute unit:\n%s\n%s" \
                                     % (str(e), traceback.format_exc())
                        task.state   = FAILED
                        task.stderr += "\nPilot cannot start compute unit: '%s'" % e
                        
                        self._log.exception("Launching task failed: '%s'." % e) 
                        
                        # Free the Slots, Flee the Flots, Ree the Frots!
                        if opaque_slot:
                            self.exec_env.scheduler.release_slot(opaque_slot)

                        self._update_tasks(task)



        except Exception, ex:
            msg = "Error in ExecWorker loop"
            self._log.exception (msg)
            pilot_FAILED(self._p, self._pilot_id, self._log, msg)
            return

    # --------------------------------------------------------------------------
    #
    def _launch_task(self, task, launcher):

        prof ('ExecWorker task launch', uid=task.uid)

        # create working directory in case it
        # doesn't exist
        try :
            os.makedirs(task.workdir)
        except OSError as e :
            # ignore failure on existing directory
            if  e.errno == errno.EEXIST and os.path.isdir (task.workdir) :
                pass
            else :
                raise

        # Start a new subprocess to launch the task
        # TODO: This is scheduler specific
        proc = self.exec_env.spawner.spawn (task     = task,
                                            launcher = launcher,
                                            env      = self.cu_environment)

        prof ('ExecWorker task launched', uid=task.uid, tag='task_launching')

        task.started = timestamp()
        task.state   = EXECUTING
        task._proc   = proc

        # Add to the list of monitored tasks
        self._running_tasks.append(task) # add task here?

        # Update to mongodb
        #
        # AM: FIXME: this mongodb update is effectively a (or rather multiple)
        # synchronous remote operation(s) in the exec worker main loop.  Even if
        # spanning multiple exec workers, we would still share the mongodb
        # channel, which would still need serialization.  This is rather
        # inefficient.  We should consider to use a async communication scheme.
        # For example, we could collect all messages for a second (but not
        # longer) and send those updates in a bulk.
        self._update_tasks(task)


    # --------------------------------------------------------------------------
    # Iterate over all running tasks, check their status, and decide on the 
    # next step.  Also check for a requested cancellation for the task.
    def _check_running(self):

        idle = True

        # we update tasks in 'bulk' after each iteration.
        # all tasks that require DB updates are in update_tasks
        update_tasks = []

        # We record all completed tasks
        finished_tasks = []

        for task in self._running_tasks:

            # Get the subprocess object to poll on
            proc = task._proc
            ret_code = proc.poll()
            if ret_code is None:
                # Process is still running

                if task.uid in self._cuids_to_cancel:
                    # We got a request to cancel this task.
                    proc.kill()
                    state = CANCELED
                    finished_tasks.append(task)
                else:
                    # No need to continue [sic] further for this iteration
                    continue
            else:
                prof ('ExecWorker task found done', uid=task.uid, tag='task executing')

                # The task ended (eventually FAILED or DONE).
                finished_tasks.append(task)

              # # Make sure all stuff reached the spindles
              # # FIXME: do we still need this?
              # proc.close_and_flush_filehandles()

                # Convenience shortcut
                uid = task.uid
                self._log.info("Task %s terminated with return code %s." % (uid, ret_code))

                if ret_code != 0:
                    # The task failed, no need to deal with its output data.
                    state = FAILED
                else:
                    # The task finished cleanly, see if we need to deal with 
                    # output data.

                    if task.agent_output_staging or task.ftw_output_staging:

                        # TODO: this should ideally be PendingOutputStaging, but
                        # that introduces a race condition currently
                        state = STAGING_OUTPUT 

                        # Check if there are Directives that need to be 
                        # performed by the Agent.
                        if task.agent_output_staging:

                            prof ('ExecWorker task needs output staging', uid=task.uid)

                            # Find the task in the database
                            # TODO: shouldnt this be available somewhere 
                            #       already, that would save a roundtrip?!
                            cu = self._cu.find_one({"_id": ObjectId(uid)})

                            for directive in cu['Agent_Output_Directives']:
                                output_staging = {
                                    'directive': directive,
                                    'sandbox': task.workdir,
                                    # TODO: the staging/area pilot directory 
                                    # should  not be derived like this:
                                    'staging_area': os.path.join(os.path.dirname(task.workdir), STAGING_AREA),
                                    'cu_id': uid
                                }

                                # Put the output staging directives in the queue
                                self._output_staging_queue.put(output_staging)

                                self._cu.update(
                                    {"_id": ObjectId(uid)},
                                    {"$set": {"Agent_Output_Status": EXECUTING}}
                                )

                            prof ('ExecWorker task gets  output staging', uid=task.uid)


                        # Check if there are Directives that need to be 
                        # performed by the FTW.
                        # Obviously these are not executed here (by the Agent),
                        # but we need this code to set the state so that the FTW
                        # gets notified that it can start its work.
                        if task.ftw_output_staging:
                            prof ('ExecWorker task needs FTW_O ', uid=task.uid)
                            self._cu.update(
                                {"_id": ObjectId(uid)},
                                {"$set": {"FTW_Output_Status": PENDING}}
                            )
                            prof ('ExecWorker task gets  FTW_O ', uid=task.uid)
                    else:
                        # If there is no output data to deal with, the task 
                        # becomes DONE
                        state = DONE

            #
            # At this stage the task is ended: DONE, FAILED or CANCELED.
            #

            prof ('ExecWorker task postprocess start', uid=task.uid)

            idle = False

            # store stdout and stderr to the database
            workdir = task.workdir
            task_id = task.uid

            if  os.path.isfile(task.stdout_file):
                with open(task.stdout_file, 'r') as stdout_f:
                    try :
                        txt = unicode(stdout_f.read(), "utf-8")
                    except UnicodeDecodeError :
                        txt = "unit stdout contains binary data -- use file staging directives"

                    if  len(txt) > MAX_IO_LOGLENGTH :
                        txt = "[... CONTENT SHORTENED ...]\n%s" % txt[-MAX_IO_LOGLENGTH:]
                    task.stdout += txt

            if  os.path.isfile(task.stderr_file):
                with open(task.stderr_file, 'r') as stderr_f:
                    try :
                        txt = unicode(stderr_f.read(), "utf-8")
                    except UnicodeDecodeError :
                        txt = "unit stderr contains binary data -- use file staging directives"

                    if  len(txt) > MAX_IO_LOGLENGTH :
                        txt = "[... CONTENT SHORTENED ...]\n%s" % txt[-MAX_IO_LOGLENGTH:]
                    task.stderr += txt

            task.exit_code = ret_code

            # Record the time and state
            task.finished = timestamp()
            task.state = state

            # Put it on the list of tasks to update in bulk
            update_tasks.append(task)

            # Free the Slots, Flee the Flots, Ree the Frots!
            self.exec_env.scheduler.release_slot(task.opaque_slot)

            prof ('ExecWorker task postprocess done', uid=task.uid)

        #
        # At this stage we are outside the for loop of running tasks.
        #

        # Update all the tasks that were marked for update.
        self._update_tasks(update_tasks)

        # Remove all tasks that don't require monitoring anymore.
        for e in finished_tasks:
            self._running_tasks.remove(e)

        return idle

    # --------------------------------------------------------------------------
    #
    def _update_tasks(self, tasks):
        """Updates the database entries for one or more tasks, including
        task state, log, etc.
        """

        if  not isinstance(tasks, list):
            tasks = [tasks]

        ts = timestamp()
        # We need to know which unit manager we are working with. We can pull
        # this information here:

        # Update capabilities
        #self._capability = self._slots2caps(self._slots)
        #self._capability = self._slots2free(self.exec_env.scheduler._slots)

        # AM: FIXME: this at the moment pushes slot history whenever a task
        # state is updated...  This needs only to be done on ExecWorker
        # shutdown.  Well, alas, there is currently no way for it to find out
        # when it is shut down... Some quick and  superficial measurements 
        # though show no negative impact on agent performance.
        # AM: the capability publication cannot be delayed until shutdown
        # though...
        # TODO: check that slot history is correctly recorded
        if  (self._slot_history_old !=
             self.exec_env.scheduler._slot_history) : # or \
          # (self._capability_old
          #  != self.exec_env.scheduler._capability):

            self._p.update(
                {"_id": ObjectId(self._pilot_id)},
                {"$set": {"slothistory" : self.exec_env.scheduler._slot_history,
                          #"slots"       : self._slots,
                          #"capability"  : self.exec_env.scheduler._capability
                         }
                }
                )

            prof ('ExecWorker pilot state pushed')

            self._slot_history_old = self.exec_env.scheduler._slot_history[:]

        for task in tasks:
            self._cu.update({"_id": ObjectId(task.uid)}, 
            {"$set": {"state"        : task.state,
                      "started"      : task.started,
                      "finished"     : task.finished,
                      "slots"        : task.slots, # TODO: keep around?
                      "exit_code"    : task.exit_code,
                      "stdout"       : task.stdout,
                      "stderr"       : task.stderr},
             "$push": {"statehistory": {"state": task.state, "timestamp": ts}}
            })

            if task.state in [DONE, CANCELED, FAILED] :
                prof ('ExecWorker task state pushed', uid=task.uid, tag='task postprocessing')
            else :
                prof ('ExecWorker task state pushed', uid=task.uid)


# ------------------------------------------------------------------------------
#
class UpdateWorker(threading.Thread):
    """
    An UpdateWorker pushes CU and Pilot state updates to mongodb.  Its instances
    compete for update requests on the update_queue.  Those requests will be
    triplets of collection name, query dict, and update dict.  Update requests
    will be collected into bulks over some time (BULK_COLLECTION_TIME), to
    reduce number of roundtrips.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, logger, session_id, 
                 update_queue, mongodb_url, mongodb_name):

        threading.Thread.__init__(self)

        self._log           = logger
        self._session_id    = session_id
        self._update_queue  = update_queue
        self._terminate     = threading.Event ()

        mongo_client        = pymongo.MongoClient(mongodb_url)
        self._mongo_db      = mongo_client[mongodb_name]
        self._cinfo         = dict()  # collection cache


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._terminate.set ()


    # --------------------------------------------------------------------------
    #
    def run(self):

        self._log.info('UpdateWorker started ...')

        while not self._terminate.isSet () :

            # ------------------------------------------------------------------
            def timed_bulk_execute (cinfo) :

                # returns number of bulks pushed (0 or 1)
                if  not cinfo['bulk'] :
                    return 0

                now = time.time ()
                age = now - cinfo['last']

                if  cinfo['bulk'] and age > BULK_COLLECTION_TIME :

                    res  = cinfo['bulk'].execute ()
                    self._log.debug ('bulk update result: %s' % res)

                    for uid in cinfo['uids'] :
                        prof ('state update bulk pushed', uid=uid)

                    cinfo['last'] = now
                    cinfo['bulk'] = None
                    cinfo['uids'] = list()
                    return 1

                else :
                    return 0
            # ------------------------------------------------------------------

            try :

                try:
                    update_request = self._update_queue.get_nowait()

                except Queue.Empty:

                    # no new requests: push any pending bulks
                    action = 0
                    for cname in self._cinfo :
                        action += timed_bulk_execute (self._cinfo[cname])

                    if  not action :
                        time.sleep(QUEUE_POLL_SLEEPTIME)

                    continue


                # got a new request.  Add to bulk (create as needed), 
                # and push bulk if time is up.
                cu = update_request['unit']
                prof ('state update pulled', uid=cu['uid'])

                cbase       = update_request.get ('cbase', '.cu')
                query_dict  = update_request.get ('query',  dict())
                update_dict = update_request.get ('update', dict())

                query_dict['_id'] = cu['_id']

                cname = self._session_id + cbase

                if not cname in self._cinfo :
                    coll =  self._mongo_db[cname]
                    self._cinfo[cname] = {
                            'coll' : coll,
                            'bulk' : None,
                            'last' : time.time(),  # time of last push
                            'uids' : list()
                            }

                cinfo = self._cinfo[cname]

                if  not cinfo['bulk'] : 
                    cinfo['bulk'] = coll.initialize_ordered_bulk_op ()

                cinfo['uids'].append (cu['uid'])
                cinfo['bulk'].find   (query_dict) \
                             .update (update_dict)
                
                timed_bulk_execute (cinfo)
                prof ('state update bulked', uid=cu['uid'])

            except Exception as e :
                self._log.exception ("state update failed")

                # FIXME: should we fail the pilot at this point?  
                # FIXME: Are the strategies to recover?


# ------------------------------------------------------------------------------
#
class StageinWorker(threading.Thread):
    """An StageinWorker performs the agent side staging directives.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, logger, execution_queue, stagein_queue, update_queue, workdir) :

        threading.Thread.__init__(self)

        self._log             = logger
        self._execution_queue = execution_queue
        self._stagein_queue   = stagein_queue
        self._update_queue    = update_queue
        self._workdir         = workdir
        self._terminate       = threading.Event ()


    # --------------------------------------------------------------------------
    #
    def stop(self):
        """Terminates the process' main loop.
        """
        self._terminate.set ()

    # --------------------------------------------------------------------------
    #
    def run(self):

        self._log.info('StageinWorker started ...')

        while not self._terminate.isSet () :
            try:
                try:
                    cu = self._stagein_queue.get_nowait()

                except Queue.Empty:
                    time.sleep(QUEUE_POLL_SLEEPTIME)
                    continue

                sandbox      = os.path.join (self._workdir, 'unit-%s' % cu['uid']),
                staging_area = os.path.join (self._workdir, 'staging_area'),


                for directive in cu['Agent_Input_Directives']:
                    prof ('Agent input_staging queue', uid=cu['uid'], msg=directive)

                    # Perform input staging
                    self._log.info('Task input staging directives %s for cu: %s to %s' %
                                   (directive, cu['uid'], sandbox))

                    # Convert the source_url into a SAGA Url object
                    source_url = saga.Url(directive['source'])

                    if source_url.scheme == 'staging':
                        self._log.info('Operating from staging')
                        # Remove the leading slash to get a relative path from the staging area
                        rel2staging = source_url.path.split('/',1)[1]
                        source = os.path.join(staging_area, rel2staging)
                    else:
                        self._log.info('Operating from absolute path')
                        source = source_url.path

                    # Get the target from the directive and convert it to the location 
                    # in the sandbox
                    target = directive['target']
                    abs_target = os.path.join(sandbox, target)

                    log_message = ''
                    # Act upon the directive now.

                    if directive['action'] == LINK:
                        log_message = 'Linking %s to %s' % (source, abs_target)
                        os.symlink(source, abs_target)
                    elif directive['action'] == COPY:
                        log_message = 'Copying %s to %s' % (source, abs_target)
                        shutil.copyfile(source, abs_target)
                    elif directive['action'] == MOVE:
                        log_message = 'Moving %s to %s' % (source, abs_target)
                        shutil.move(source, abs_target)
                    elif directive['action'] == TRANSFER:
                        # TODO: SAGA REMOTE TRANSFER
                        log_message = 'Transferring %s to %s' % (source, abs_target)
                    else:
                        raise Exception('Action %s not supported' % directive['action'])

                    # If we reached this far, assume the staging succeeded
                    log_message += ' succeeded.'
                    self._log.info(log_message)


                # If all went fine, update the state of this StagingDirective 
                # to done
                self._update_queue.put ({
                    'unit'   : cu, 
                    'query'  : {
                        'Agent_Input_Status'           : EXECUTING,
                        'Agent_Input_Directives.state' : PENDING,
                        'Agent_Input_Directives.source': directive['source'],
                        'Agent_Input_Directives.target': directive['target']
                        },
                    'update' : {
                        '$set' : {'Agent_Input_Directives.$.state': DONE},
                        '$push': {'log': log_message}
                        }
                    })

            except:
                # If we catch an exception, assume the staging failed
                log_message += ' failed.'
                self._log.error(log_message)

                # If a staging directive fails, fail the CU also.
                self._update_queue.put ({
                    'unit'   : cu, 
                    'query'  : {
                        'Agent_Input_Status'           : EXECUTING,
                        'Agent_Input_Directives.state' : PENDING,
                        'Agent_Input_Directives.source': directive['source'],
                        'Agent_Input_Directives.target': directive['target']
                        },
                    'update' : {
                        '$set' : { 'Agent_Input_Directives.$.state': FAILED,
                                   'Agent_Input_Status'            : FAILED,
                                   'state'                         : FAILED},
                        '$push': {
                            'log': 'Staging Directive failed'}
                        }
                    })


# ------------------------------------------------------------------------------
#
class StageoutWorker(threading.Thread):
    """An StageoutWorker performs the agent side staging directives.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, logger, execution_queue, stageout_queue, update_queue, workdir) :

        threading.Thread.__init__(self)

        self._log             = logger
        self._execution_queue = execution_queue
        self._stageout_queue  = stageout_queue
        self._update_queue    = update_queue
        self._workdir         = workdir
        self._terminate       = threading.Event ()


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._terminate.set()


    # --------------------------------------------------------------------------
    #
    def run(self):

        self._log.info('StageoutWorker started ...')

        while not self._terminate.isSet () :
            try :
                try:
                    cu = self._stageout_queue.get_nowait()

                except Queue.Empty:
                    time.sleep(QUEUE_POLL_SLEEPTIME)
                    continue

                sandbox      = os.path.join (self._workdir, 'unit-%s' % cu['uid']),
                staging_area = os.path.join (self._workdir, 'staging_area'),


                for directive in cu['Agent_Output_Directives']:

                    # Perform output staging

                    self._log.info('Task output staging directives %s for cu: %s to %s' % (
                        directive, cu['uid'], sandbox))

                    source = str(directive['source'])
                    abs_source = os.path.join(sandbox, source)

                    # Convert the target_url into a SAGA Url object
                    target_url = saga.Url(directive['target'])

                    # Handle special 'staging' scheme
                    if target_url.scheme == 'staging':
                        self._log.info('Operating from staging')
                        # Remove the leading slash to get a relative path from 
                        # the staging area
                        rel2staging = target_url.path.split('/',1)[1]
                        target = os.path.join(staging_area, rel2staging)
                    else:
                        self._log.info('Operating from absolute path')
                        target = target_url.path

                    # Create output directory in case it doesn't exist yet
                    try :
                        os.makedirs(os.path.dirname(target))
                    except OSError as e:
                        # ignore failure on existing directory
                        if e.errno == errno.EEXIST and os.path.isdir(os.path.dirname(target)):
                            pass
                        else:
                            raise

                    if directive['action'] == LINK:
                        self._log.info('Going to link %s to %s' % (abs_source, target))
                        os.symlink(abs_source, target)
                        logmessage = 'Linked %s to %s' % (abs_source, target)
                    elif directive['action'] == COPY:
                        self._log.info('Going to copy %s to %s' % (abs_source, target))
                        shutil.copyfile(abs_source, target)
                        logmessage = 'Copied %s to %s' % (abs_source, target)
                    elif directive['action'] == MOVE:
                        self._log.info('Going to move %s to %s' % (abs_source, target))
                        shutil.move(abs_source, target)
                        logmessage = 'Moved %s to %s' % (abs_source, target)
                    elif directive['action'] == TRANSFER:
                        self._log.info('Going to transfer %s to %s' % (
                            directive['source'], os.path.join(sandbox, directive['target'])))
                        # TODO: SAGA REMOTE TRANSFER
                        logmessage = 'Transferred %s to %s' % (abs_source, target)
                    else:
                        # TODO: raise
                        self._log.error('Action %s not supported' % directive['action'])

                    # If all went fine, update the state of this 
                    # StagingDirective to Done
                    self._update_queue.put ({
                        'unit'   : cu, 
                        'query'  : {
                            'Agent_Output_Status'           : EXECUTING,
                            'Agent_Output_Directives.state' : PENDING,
                            'Agent_Output_Directives.source': directive['source'],
                            'Agent_Output_Directives.target': directive['target']
                            },
                        'update' : {
                            '$set' : {'Agent_Output_Directives.$.state': DONE},
                            '$push': {'log': logmessage}
                            }
                        })

            except Exception, ex:
                self._log.exception("Error in StageoutWorker loop")
                raise


# ------------------------------------------------------------------------------
#
class Agent (object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, logger, lrms_name, requested_cores, 
            task_launch_method, mpi_launch_method, 
            scheduler_name, runtime, 
            mongodb_url, mongodb_name, mongodb_auth, 
            pilot_id, session_id):

        prof ('Agent init')

        self._log                   = logger
        self._pilot_id              = pilot_id
        self._runtime               = runtime
        self._terminate             = threading.Event()
        self._starttime             = time.time()
        self._workdir               = os.getcwd()
        self._session_id            = session_id
        self._pilot_id              = pilot_id

        self._execution_worker_list = list()
        self._update_worker_list    = list()
        self._stagein_worker_list   = list()
        self._stageout_worker_list  = list()

        self._execution_queue       = multiprocessing.Queue()
        self._update_queue          = multiprocessing.Queue()
        self._stagein_queue         = multiprocessing.Queue()
        self._stageout_queue        = multiprocessing.Queue()

        mongo_client = pymongo.MongoClient(mongodb_url)
        mongo_db     = mongo_client[mongodb_name]

        # do auth on username *and* password (ignore empty split results)
        auth_elems = filter (None, mongodb_auth.split (':', 1))
        if  len (auth_elems) == 2 :
            mongo_db.authenticate (auth_elems[0], auth_elems[1])

        self._p  = mongo_db["%s.p"  % self._session_id]
        self._cu = mongo_db["%s.cu" % self._session_id]

        # the task queue holds the tasks that are pulled from the MongoDB
        # server. The ExecWorkers compete for the tasks in the queue. 

        # The staging queues holds the staging directives to be performed
        self._input_staging_queue  = multiprocessing.Queue()
        self._output_staging_queue = multiprocessing.Queue()

        # Channel for the Agent to communicate commands with the ExecWorker
        self._command_queue = multiprocessing.Queue()

        #--------------------------------------------------------------------------
        # Discover environment, nodes, cores, mpi, etc.
        prof ('exec env setup')
        self._exec_env = ExecutionEnvironment(
                logger             = self._log,
                lrms_name          = lrms_name,
                requested_cores    = requested_cores,
                task_launch_method = task_launch_method,
                mpi_launch_method  = mpi_launch_method,
                scheduler_name     = scheduler_name
        )


        for n in range(N_EXECUTION_WORKER) :
            prof ('Exec Worker create %s' % n)
            exec_worker = ExecWorker(
                exec_env        = self._exec_env,
                logger          = self._log,
                execution_queue = self._execution_queue,
                output_staging_queue   = self._output_staging_queue,
                command_queue   = self._command_queue,
                #node_list       = self._exec_env.lrms.node_list,
                #cores_per_node  = self._exec_env.lrms.cores_per_node,
                #launch_methods  = self._exec_env.discovered_launch_methods,
                mongodb_url     = mongodb_url,
                mongodb_name    = mongodb_name,
                mongodb_auth    = mongodb_auth,
                pilot_id        = self._pilot_id,
                session_id      = self._session_id,
                cu_environment  = self._exec_env.cu_environment,
                workdir         = self._workdir
            )
            exec_worker.start()
            self._log.info("Started up %s serving nodes %s" %
                           (exec_worker, self._exec_env.lrms.node_list))
            self._execution_worker_list.append (exec_worker)


        for n in range(N_UPDATE_WORKER) :
            prof ('Update Worker create')
            update_worker = UpdateWorker(
                logger          = self._log,
                session_id      = self._session_id,
                update_queue    = self._update_queue,
                mongodb_url     = mongodb_url,
                mongodb_name    = mongodb_name
            )
            update_worker.start()
            self._log.info("Started up %s." % update_worker)
            self._update_worker_list.append (update_worker)


        for n in range(N_STAGEIN_WORKER) :
            prof ('IS Worker create')
            stagein_worker = StageinWorker(
                logger          = self._log,
                execution_queue = self._execution_queue,
                stagein_queue   = self._stagein_queue,
                update_queue    = self._update_queue,
                workdir         = self._workdir
            )
            stagein_worker.start()
            self._log.info("Started up %s." % stagein_worker)
            self._stagein_worker_list.append (stagein_worker)


        for n in range(N_STAGEOUT_WORKER) :
            prof ('OS Worker create')
            stageout_worker = StageoutWorker(
                logger          = self._log,
                execution_queue = self._execution_queue,
                stageout_queue  = self._stageout_queue,
                update_queue    = self._update_queue,
                workdir         = self._workdir
            )
            stageout_worker.start()
            self._log.info("Started up %s." % stageout_worker)
            self._stageout_worker_list.append (stageout_worker)


        prof ('Agent init done')


    # --------------------------------------------------------------------------
    #
    def stop(self):
        """Terminate the agent main loop.
        """
        prof ('Agent stop()')

        # First, we need to shut down all the workers
        for exec_worker in self._execution_worker_list :
            exec_worker.stop ()

        for update_worker in self._update_worker_list :
            update_worker.stop ()

        for stagein_worker in self._stagein_worker_list :
            stagein_worker.stop ()

        for stageout_worker in self._stageout_worker_list :
            stageout_worker.stop ()

        # Next, we set our own termination signal
        self._terminate.set()

    # --------------------------------------------------------------------------
    #
    def run(self):

        prof ('Agent run()')

        # first order of business: set the start time and state of the pilot
        self._log.info("Agent %s starting ..." % self._pilot_id)
        ts = timestamp()
        ret = self._p.update(
            {"_id": ObjectId(self._pilot_id)}, 
            {"$set": {"state"          : ACTIVE,
                      # TODO: The two fields below are currently scheduler 
                      #       specific!
                      "nodes"          : self._exec_env.lrms.node_list,
                      "cores_per_node" : self._exec_env.lrms.cores_per_node,
                      "started"        : ts,
                      "capability"     : 0},
             "$push": {"statehistory": {"state"    : ACTIVE, 
                                        "timestamp": ts}}
            })
        # TODO: Check for return value, update should be true!
        self._log.info("Database updated! %s" % ret)

        prof ('Agent start loop')

        while not self._terminate.isSet() :

            try:

                action  = 0
                action += self._check_worker_state ()
                action += self._check_commands     ()
                action += self._check_units        ()

                if  not action :
                    time.sleep(QUEUE_POLL_SLEEPTIME)

            except Exception, ex:
                # If we arrive here, there was an exception in the main loop.
                pilot_FAILED(self._p, self._pilot_id, self._log, 
                    "ERROR in agent main loop: %s. %s" % (str(ex), traceback.format_exc()))
                return

        # main loop terminated, so self._terminate was set
        pilot_CANCELED(self._p, self._pilot_id, self._log,
                "Terminated (_terminate set).")
        sys.exit (0)


    # --------------------------------------------------------------------------
    #
    def _check_worker_state (self) :

        # Check the workers periodically. If they have died, we 
        # exit as well. this can happen, e.g., if the worker 
        # process has caught a ctrl+C
        for worker in self._execution_worker_list :
            if  worker.is_alive() is False:
                msg = 'Execution worker %s died' % str(worker)
                pilot_FAILED(self._p, self._pilot_id, self._log, msg)
                sys.exit (1)

        for worker in self._update_worker_list :
            if  worker.is_alive() is False:
                msg = 'Update worker %s died' % str(worker)
                pilot_FAILED(self._p, self._pilot_id, self._log, msg)
                sys.exit (1)

        for worker in self._stagein_worker_list :
            if  worker.is_alive() is False:
                msg = 'Stagein worker %s died' % str(worker)
                pilot_FAILED(self._p, self._pilot_id, self._log, msg)
                sys.exit (1)

        for worker in self._stageout_worker_list :
            if  worker.is_alive() is False:
                msg = 'Stageout worker %s died' % str(worker)
                pilot_FAILED(self._p, self._pilot_id, self._log, msg)
                sys.exit (1)

        # Make sure that we haven't exceeded the agent runtime. if 
        # we have, terminate. 
        if  time.time() >= self._starttime + (int(self._runtime) * 60):
            self._log.info("Agent has reached runtime limit of %s seconds." % self._runtime*60)
            pilot_DONE(self._p, self._pilot_id)
            sys.exit (1)

        # don't increase idle count
        return 0  


    # --------------------------------------------------------------------------
    #
    def _check_commands (self) :

        # Check if there's a command waiting
        retdoc = self._p.find_and_modify(
                    query  = {"_id"  : ObjectId(self._pilot_id)},
                    update = {"$set" : {COMMAND_FIELD: []}}, # Wipe content of array
                    fields = [COMMAND_FIELD, 'state']
                    )

        commands = list()
        if retdoc:
            commands = retdoc[COMMAND_FIELD]
            state    = retdoc['state']


        for command in commands:

            prof ('Agent get command', msg=[command[COMMAND_TYPE], command[COMMAND_ARG]])

            if  command[COMMAND_TYPE] == COMMAND_CANCEL_PILOT :
                pilot_CANCELED(self._p, self._pilot_id, self._log, "CANCEL received. Terminating.")
                sys.exit (1)

            elif state == CANCELING :
                pilot_CANCELED(self._p, self._pilot_id, self._log, "CANCEL implied. Terminating.")
                sys.exit (1)

            elif command[COMMAND_TYPE] == COMMAND_CANCEL_COMPUTE_UNIT:
                self._log.info("Received Cancel Compute Unit command for: %s" % command[COMMAND_ARG])
                # Put it on the command queue of the ExecWorker
                self._command_queue.put(command)

            elif command[COMMAND_TYPE] == COMMAND_KEEP_ALIVE:
                self._log.info("Received KeepAlive command.")

            else:
                raise Exception("Received unknown command: %s with arg: %s." %
                                (command[COMMAND_TYPE], command[COMMAND_ARG]))

        # only increase idle timer if anything happened
        return len(commands)


    # --------------------------------------------------------------------------
    #
    def _check_units (self) :

        # Check if there are compute units waiting for execution,
        # and log that we pulled it.
        #
        # Unfortunately, find_and_modify is not bulkable, so we have to use
        # find.  To avoid finding the same units over and over again, we have to
        # update the state *before* running the next find -- so we basically do
        # it right here...
        cu_cursor  = self._cu.find (multi = True, 
                                    spec  = {"pilot" : self._pilot_id,
                                             "state" : PENDING_EXECUTION})

        if cu_cursor.count() :
            prof ('Agent get units', msg="number of units: %d" % cu_cursor.count(),
                  logger=self._log.info)

        cu_list = list (cu_cursor)


        cu_uids = list()
        for cu in cu_list :
            cu_uids.append (cu['_id'])

        if  cu_uids :
            updated_ids = self._cu.update (
                    multi    = True, 
                    spec     = {"_id"   : {"$in"    : cu_uids}},
                    document = {"$set"  : {"state"  : ALLOCATING}, 
                                "$push" : {"statehistory" : 
                                    {
                                        "state"     : ALLOCATING, 
                                        "timestamp" : timestamp()
                                    }
                               }})
            self._log.debug ("found   IDs: %s" % len(cu_uids))
            self._log.debug ("found   IDs: %s" % cu_uids)
            self._log.debug ("updated IDs: %s" % updated_ids)


        for cu in cu_list :

            try :
                cu['uid'] = str(cu['_id'])

                prof ('Agent get unit', uid=cu['uid'], tag='cu arriving', 
                      logger=self._log.info)

                # create unit sandbox
                sandbox = os.path.join (self._workdir, 'unit-%s' % cu['uid'])
                try :
                    os.makedirs(sandbox)
                except OSError as e :
                    if  e.errno == errno.EEXIST :
                        pass
                    else :
                        raise

                # and send to staging / execution, respectively
                if cu['Agent_Input_Directives'] :

                    self._update_queue.put ({
                        'unit'   :  cu,
                        'update' : {
                            "$set"  : {"state"       : ALLOCATING},
                            "$push" : {"statehistory": {
                                "state"    : ALLOCATING, 
                                "timestamp": timestamp()}}
                            }
                        })
                    self._stagein_queue.put (cu)

                else :
                    self._update_queue.put ({
                        'unit'   :  cu,
                        'update' : {
                            "$set"  : {"state"       : STAGING_INPUT},
                            "$push" : {"statehistory": {
                                "state"    : STAGING_INPUT, 
                                "timestamp": timestamp()}}
                            }
                        })
                    self._execution_queue.put (cu)


            except Exception as e :
                # if any unit sorting step failed, the unit did
                # not end up in a queue -- we set it to FAILED
                self._log.exception ('oops')
                msg = "could not sort unit (%s)" % e
                prof ('error', msg=msg, tag="failed", 
                      uid=cu['uid'], logger=logger.exception)
                self._update_queue.put ({
                    'unit'   : cu, 
                    'update' : {
                        '$set' : {'state'   : FAILED},
                        '$push': {
                            'log'           : msg,
                            'statehistory'  : {
                                'state'     : FAILED, 
                                'timestamp' : timestamp()
                                }
                            }
                        }
                    })
                # this is final, the unit will not be touched
                # anymore.

        return len(cu_list)


# ==============================================================================
#
# Spawners
#
# ==============================================================================
class Spawner (object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, logger):

        self.name     = name
        self.log      = logger


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Launch Method.
    #
    @classmethod
    def create(cls, name, logger):

        # Make sure that we are the base-class!
        if cls != Spawner:
            raise Exception("Spawner Factory only available to base class!")

        try:
            implementation = { SPAWNER_NAME_POPEN : SpawnerPopen,
                               SPAWNER_NAME_PTY   : SpawnerPty  }[name]

            return implementation(name, logger)

        except KeyError:
            raise Exception("Spawner '%s' unknown!" % name)


    # --------------------------------------------------------------------------
    #
    def spawn (self, launcher, task, env):
        raise NotImplementedError("spawn() not implemented for Spawner '%s'." % self.name)


# ------------------------------------------------------------------------------
#
class SpawnerPopen (Spawner):


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, logger):

        Spawner.__init__(self, name, logger)


    # --------------------------------------------------------------------------
    #
    def spawn (self, launcher, task, env):
        
        prof ('Spawner spawn', uid=task.uid)
    
        launch_script_name = '%s/radical_pilot_cu_launch_script.sh' % task.workdir
        self.log.debug('Created launch_script: %s' % launch_script_name)

        with open (launch_script_name, "w") as launch_script :
            launch_script.write('#!/bin/bash -l\n')
            launch_script.write('\n# Change to working directory for task\ncd %s\n' % task.workdir)
    
            # Before the Big Bang there was nothing
            if task.pre_exec:
                pre_exec_string = ''
                if isinstance(task.pre_exec, list):
                    for elem in task.pre_exec:
                        pre_exec_string += "%s\n" % elem
                else :
                    pre_exec_string += "%s\n" % task.pre_exec
                launch_script.write('# Pre-exec commands\n%s' % pre_exec_string)
    
            # Create string for environment variable setting
            if task.environment and len(task.environment.keys()):
                env_string = 'export'
                for key,val in task.environment.itemize ():
                    env_string += ' %s=%s' % (key, val)
                launch_script.write('# Environment variables\n%s\n' % env_string)
    
            # Task Arguments (if any)
            task_args_string = ''
            if  task.arguments :
                for arg in task.arguments:
                    if not arg:
                         # ignore empty args
                         continue
    
                    arg = arg.replace('"', '\\"')          # Escape all double quotes
                    if arg[0] == arg[-1] == "'" :          # If a string is between outer single quotes,
                        task_args_string += '%s ' % arg    # ... pass it as is.
                    else:
                        task_args_string += '"%s" ' % arg  # Otherwise return between double quotes.
    
            # The actual command line, constructed per launch-method
            prof ('_Process construct command', uid=task.uid)
            launch_command, cmdline = \
                    launcher.construct_command(task.executable,  
                                               task_args_string,
                                               task.numcores, 
                                               launch_script_name, 
                                               task.opaque_slot)
    
            launch_script.write('# The command to run\n%s\n' % launch_command)
    
            # After the universe dies the infrared death, there will be nothing
            if  task.post_exec:
                post_exec_string = ''
                if isinstance(task.post_exec, list):
                    for elem in task.post_exec:
                        post_exec_string += "%s\n" % elem
                else :
                    post_exec_string += "%s\n" % task.post_exec
                launch_script.write('%s\n' % post_exec_string)
    
        # done writing to launch script, get it ready for execution.
        st = os.stat(launch_script_name)
        os.chmod(launch_script_name, st.st_mode | stat.S_IEXEC)
    
        self._stdout_file_h = open(task.stdout_file, "w")
        self._stderr_file_h = open(task.stderr_file, "w")
    
        self.log.info("Launching task %s via %s in %s" % (task.uid, cmdline, task.workdir))
        prof ('spawning pass to popen', uid=task.uid, tag='task spawning')

        proc = subprocess.Popen ( args               = cmdline,
                                  bufsize            = 0,
                                  executable         = None,
                                  stdin              = None,
                                  stdout             = self._stdout_file_h,
                                  stderr             = self._stderr_file_h,
                                  preexec_fn         = None,
                                  close_fds          = True,
                                  shell              = True,
                                  # TODO: cwd        d oesn't always make sense if it runs remotely (still true?)
                                  cwd                = task.workdir,
                                  env                = env,
                                  universal_newlines = False,
                                  startupinfo        = None,
                                  creationflags      = 0)
    
        prof ('spawning passed to popen', uid=task.uid, tag='task spawning')
        prof ('Spawner spawned', uid=task.uid)

        return proc


# ------------------------------------------------------------------------------
#
class SpawnerPty (Spawner):
    pass

# ------------------------------------------------------------------------------
#
def parse_commandline():

    parser = optparse.OptionParser()

    parser.add_option('-a', dest='mongodb_auth')
    parser.add_option('-c', dest='cores',       type='int')
    parser.add_option('-d', dest='debug_level', type='int')
    parser.add_option('-j', dest='task_launch_method')
    parser.add_option('-k', dest='mpi_launch_method')
    parser.add_option('-l', dest='lrms')
    parser.add_option('-m', dest='mongodb_url')
    parser.add_option('-n', dest='database_name')
    parser.add_option('-p', dest='pilot_id')
    parser.add_option('-q', dest='agent_scheduler')
    parser.add_option('-r', dest='runtime',     type='int')
    parser.add_option('-s', dest='session_id')

    # parse the whole shebang
    (options, args) = parser.parse_args()

    if not options.cores                : parser.error("Missing number of cores (-c)")
    if not options.debug_level          : parser.error("Missing DEBUG level (-d)")
    if not options.task_launch_method   : parser.error("Missing task launch method (-j)")
    if not options.mpi_launch_method    : parser.error("Missing mpi launch method (-k)")
    if not options.lrms                 : parser.error("Missing LRMS (-l)")
    if not options.mongodb_url          : parser.error("Missing MongoDB URL (-m)")
    if not options.database_name        : parser.error("Missing database name (-n)")
    if not options.pilot_id             : parser.error("Missing pilot id (-p)")
    if not options.agent_scheduler      : parser.error("Missing agent scheduler (-q)")
    if not options.runtime              : parser.error("Missing agent runtime (-r)")
    if not options.session_id           : parser.error("Missing session id (-s)")

    return options


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    mongo_p = None
    options = parse_commandline()

    prof ('start', tag='bootstrapping', uid=options.pilot_id)

    # configure the agent logger
    logger    = logging.getLogger   ('radical.pilot.agent')
    handle    = logging.FileHandler ("AGENT.LOG")
    formatter = logging.Formatter   ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.setLevel(options.debug_level)
    handle.setFormatter(formatter)
    logger.addHandler(handle)

    logger.info("Using SAGA version %s" % saga.version)
    logger.info("Using RADICAL-Pilot multicore agent, version %s" % git_ident)
  # logger.info("Using RADICAL-Pilot version %s" % rp.version)


    # --------------------------------------------------------------------------
    def sigint_handler(signal, frame):
        msg = 'Caught SIGINT. EXITING.'
        pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
        sys.exit (2)
    signal.signal(signal.SIGINT, sigint_handler)


    # --------------------------------------------------------------------------
    def sigalarm_handler(signal, frame):
        msg = 'Caught SIGALRM (Walltime limit reached?). EXITING'
        pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
        sys.exit (3)
    signal.signal(signal.SIGALRM, sigalarm_handler)


    try:
        # --------------------------------------------------------------------------
        # Establish database connection
        prof ('db setup')
        host, port   = options.mongodb_url.split(':', 1)
        mongo_client = pymongo.MongoClient(options.mongodb_url)
        mongo_db     = mongo_client[options.database_name]

        if  len (options.mongodb_auth) >= 3 :
            user, pwd = options.mongodb_auth.split (':', 1)
            mongo_db.authenticate (user, pwd)

        mongo_p = mongo_db["%s.p" % options.session_id]


        # --------------------------------------------------------------------------
        # Launch the agent thread
        prof ('Agent create')
        agent = Agent(
                logger             = logger,
                lrms_name          = options.lrms,
                requested_cores    = options.cores,
                task_launch_method = options.task_launch_method,
                mpi_launch_method  = options.mpi_launch_method,
                scheduler_name     = options.agent_scheduler,
                runtime            = options.runtime,
                mongodb_url        = options.mongodb_url,
                mongodb_name       = options.database_name,
                mongodb_auth       = options.mongodb_auth,
                pilot_id           = options.pilot_id,
                session_id         = options.session_id
        )

        agent.run()
        prof ('Agent done')

    except SystemExit:
        logger.error("Caught keyboard interrupt. EXITING")

    except Exception as ex:
        msg = "Error running agent: %s" % str(ex)
        logger.exception(msg)
        pilot_FAILED(mongo_p, options.pilot_id, logger, msg)
        sys.exit(6)

    finally :
        prof ('stop', msg='finally clause')
        sys.exit(7)


# ------------------------------------------------------------------------------

