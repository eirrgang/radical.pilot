#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.scheduler.DirectSubmissionScheduler
   :platform: Unix
   :synopsis: A single-pilot, direct submission scheduler.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os

from radical.pilot.utils.logger        import logger
from radical.pilot.scheduler.interface import Scheduler 

# -----------------------------------------------------------------------------
# 
class DirectSubmissionScheduler(Scheduler):
    """DirectSubmissionScheduler implements a single-pilot 'pass-through' 
    scheduling algorithm.
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self, manager, session):
        """Le constructeur.
        """

        self.manager = manager
        self.session = session

        logger.info("Loaded scheduler: %s." % self.name)

    # -------------------------------------------------------------------------
    #
    def __del__(self):
        """Le destructeur.
        """
        if os.getenv("RADICAL_PILOT_GCDEBUG", None) is not None:
            logger.debug("__del__(): %s." % self.name)

    # -------------------------------------------------------------------------
    #
    def _name(self):
        return "DirectSubmissionScheduler"

    # -------------------------------------------------------------------------
    #
    def schedule(self, unit_descriptions):

        pilots = self.manager.list_pilots()

        if not len (pilots):
            raise RuntimeError ('Unit scheduler cannot operate on empty pilot set')

        if len (pilots) > 1:
            raise RuntimeError ('Direct Submission only works for a single pilot!')
        
        ret            = dict()
        for ud in unit_descriptions:
            ret[ud] = pilots[0]

        return ret

