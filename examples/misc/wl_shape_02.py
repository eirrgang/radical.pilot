#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys
import time

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'

    session = rp.Session()
    try:

        report.info('read config')
        pwd    = os.path.dirname(os.path.abspath(__file__))
        config = ru.read_json('%s/../config.json' % pwd)
        report.ok('>>ok\n')

        report.header('submit pilots')
        pd_init = {'resource'      : resource,
                   'runtime'       : 15,  # pilot runtime (min)
                   'exit_on_error' : True,
                   'project'       :     config[resource]['project'],
                   'queue'         :     config[resource]['queue'],
                   'access_schema' :     config[resource]['schema'],
                   'cores'         : max(config[resource]['cores'], 128)
                  }
        pdesc = rp.ComputePilotDescription(pd_init)
        pmgr  = rp.PilotManager(session=session)
        pilot = pmgr.submit_pilots(pdesc)

        report.header('stage data')
        pilot.stage_in({'source': 'client:///examples/misc/gromacs/',
                        'target': 'pilot:///',
                        'action': rp.TRANSFER})
        report.ok('>>ok\n')

        report.header('submit units')

        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        args = 'mdrun -o traj.trr -e ener.edr -s topol.tpr -g mdlog.log ' \
             + '-c outgro -cpo state.cpt'

        tags = {'app-stats'  : 'this_app',
                'constraint' : 'p * t <= 32'}

        cudis = list()
        for f in ['grompp.mdp', 'mdout.mdp', 'start.gro',
                  'topol.top',  'topol.tpr']:
            cudis.append({'source': 'pilot:///gromacs/%s' % f, 
                          'target': 'unit:///%s' % f,
                          'action': rp.LINK})

        n = 2 * 1024  # number of units to run
        n = 16
        report.info('create %d unit description(s)\n\t' % n)
        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.executable       = 'gmx'
            cud.arguments        = args.split()
            cud.tags             = tags
            cud.gpu_processes    = 0
            cud.cpu_processes    = '1-32'
            cud.cpu_threads      = '1-16'
            cud.cpu_process_type = rp.MPI
            cud.cpu_thread_type  = rp.OpenMP
            cud.input_staging    = cudis

            cuds.append(cud)
            report.progress()
        report.ok('>>ok\n')

        umgr.submit_units(cuds)
        report.header('gather results')
        umgr.wait_units()


    finally:
        report.header('finalize')
        session.close(download=True)

    report.header()


# ------------------------------------------------------------------------------

