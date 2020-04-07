

__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class IBRun(LaunchMethod):

    node_list = None

    # NOTE: Don't think that with IBRUN it is possible to have
    #       processes != cores ...

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)

        self._node_list = self._cfg.rm_info.node_list


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # ibrun: wrapper for mpirun at TACC
        self.launch_command = ru.which('ibrun')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cpu_processes']  # FIXME: handle cpu_threads
        task_args    = cud.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)

        # consider external environment variable that is set manually
        # and reflects how many actual tasks per node were submitted
        # (env is not set yet and is located in pre_exec list)
        tacc_tasks_per_node = None
        for pre_exec_cmd in cud.get('pre_exec', []):
            if 'TACC_TASKS_PER_NODE=' in pre_exec_cmd:
                tacc_tasks_per_node = int(pre_exec_cmd.split('=')[1])
                break

        cpn = tacc_tasks_per_node or self._cfg.cores_per_node
        index = 0
        offsets = list()

        # FIXME: remove assumption that every task would have the same number
        #  of processes and threads (along with TACC_TASKS_PER_NODE it is
        #  assumed that num_processes=1)
        for node in self._node_list:
            for slot_node in slots['nodes']:
                if slot_node['uid'] == node[0]:
                    for core_map in slot_node['core_map']:
                        if core_map:
                            offsets.append(
                                (core_map[0] // len(core_map)) + index)
            index += cpn
        ibrun_offset = min(offsets)

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        ibrun_command = "%s -n %s -o %d %s" % \
                        (self.launch_command, task_cores,
                         ibrun_offset, task_command)

        return ibrun_command, None


# ------------------------------------------------------------------------------

