"""
.. module:: radical.pilot.controller.input_file_transfer_worker
.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga
import datetime
import traceback
import multiprocessing

from bson.objectid import ObjectId
from radical.pilot.states import * 
from radical.pilot.utils.logger import logger
from radical.pilot.staging_directives import TRANSFER

# BULK_LIMIT defines the max. number of transfer requests to pull from DB.
BULK_LIMIT=1

# ----------------------------------------------------------------------------
#
class InputFileTransferWorker(multiprocessing.Process):
    """InputFileTransferWorker handles the staging of input files
    for a UnitManagerController.
    """

    # ------------------------------------------------------------------------
    #
    def __init__(self, session, db_connection_info, unit_manager_id, number=None):

        self._session = session

        # Multiprocessing stuff
        multiprocessing.Process.__init__(self)
        self.daemon = True

        self.db_connection_info = db_connection_info
        self.unit_manager_id = unit_manager_id

        self._worker_number = number
        self.name = "InputFileTransferWorker-%s" % str(self._worker_number)

    # ------------------------------------------------------------------------
    #
    def run(self):
        """Starts the process when Process.start() is called.
        """

        logger.info("Starting InputFileTransferWorker")

        # saga_session holds the SSH context infos.
        saga_session = saga.Session()

        # Try to connect to the database and create a tailable cursor.
        try:
            connection = self.db_connection_info.get_db_handle()
            db = connection[self.db_connection_info.dbname]
            um_col = db["%s.w" % self.db_connection_info.session_id]
            logger.debug("Connected to MongoDB. Serving requests for UnitManager %s." % self.unit_manager_id)

        except Exception, ex:
            logger.error("Connection error: %s. %s" % (str(ex), traceback.format_exc()))
            return

        while True:
            # See if we can find a ComputeUnit that is waiting for
            # input file transfer.
            compute_unit = None

            ts = datetime.datetime.utcnow()
            compute_unit = um_col.find_and_modify(
                query={"unitmanager": self.unit_manager_id,
                       "FTW_Input_Status": PENDING},
                update={"$set" : {"FTW_Input_Status": EXECUTING,
                                  "state": STAGING_INPUT},
                        "$push": {"statehistory": {"state": STAGING_INPUT, "timestamp": ts}}},
                limit=BULK_LIMIT # TODO: bulklimit is probably not the best way to ensure there is just one
            )
            state = STAGING_INPUT

            if compute_unit is None:
                # Sleep a bit if no new units are available.
                time.sleep(1) # TODO: Probably need better sleep logic as we also have the logic on the end now
            else:
                # AM: The code below seems wrong when BULK_LIMIT != 1 -- the
                # compute_unit will be a list then I assume.
                try:
                    log_messages = []

                    # We have found a new CU. Now we can process the transfer
                    # directive(s) wit SAGA.
                    compute_unit_id = str(compute_unit["_id"])
                    remote_sandbox = compute_unit["sandbox"]
                    input_staging = compute_unit["FTW_Input_Directives"]

                    # We need to create the WU's directory in case it doesn't exist yet.
                    log_msg = "Creating ComputeUnit sandbox directory %s." % remote_sandbox
                    log_messages.append(log_msg)
                    logger.info(log_msg)

                    # Creating the sandbox directory.
                    try:
                        wu_dir = saga.filesystem.Directory(
                            remote_sandbox,
                            flags=saga.filesystem.CREATE_PARENTS,
                            session=self._session)
                        wu_dir.close()
                    except Exception, ex:
                        tb = traceback.format_exc()
                        logger.info('Error: %s. %s' % (str(ex), tb))

                    logger.info("Processing input file transfers for ComputeUnit %s" % compute_unit_id)
                    # Loop over all transfer directives and execute them.
                    for sd in input_staging:

                        state_doc = um_col.find_one(
                            {"_id": ObjectId(compute_unit_id)},
                            fields=["state"]
                        )
                        if state_doc['state'] == CANCELED:
                            logger.info("Compute Unit Canceled, interrupting input file transfers.")
                            state = CANCELED
                            break

                        abs_src = os.path.abspath(sd['source'])
                        input_file_url = saga.Url("file://localhost/%s" % abs_src)
                        if not sd['target']:
                            target = remote_sandbox
                        else:
                            target = "%s/%s" % (remote_sandbox, sd['target'])

                        log_msg = "Transferring input file %s -> %s" % (input_file_url, target)
                        logmessage = "Transferred input file %s -> %s" % (input_file_url, target)
                        log_messages.append(log_msg)
                        logger.debug(log_msg)

                        # Execute the transfer.
                        input_file = saga.filesystem.File(
                            input_file_url,
                            session=self._session
                        )
                        try:
                            input_file.copy(target)
                        except Exception, ex:
                            tb = traceback.format_exc()
                            logger.info('Error: %s. %s' % (str(ex), tb))

                        input_file.close()

                        # If all went fine, update the state of this StagingDirective to Done
                        um_col.find_and_modify(
                            query={"_id" : ObjectId(compute_unit_id),
                                   'FTW_Input_Status': EXECUTING,
                                   'FTW_Input_Directives.state': PENDING,
                                   'FTW_Input_Directives.source': sd['source'],
                                   'FTW_Input_Directives.target': sd['target'],
                                   },
                            update={'$set': {'FTW_Input_Directives.$.state': 'Done'},
                                    '$push': {'log': logmessage}
                            }
                        )

                except Exception, ex:
                    # Update the CU's state 'FAILED'.
                    ts = datetime.datetime.utcnow()
                    log_messages = "Input transfer failed: %s\n%s" % (str(ex), traceback.format_exc())
                    logger.error(log_messages)
                    um_col.update(
                        {"_id": ObjectId(compute_unit_id)},
                        {"$set": {"state": FAILED},
                         "$push": {"statehistory": {"state": FAILED, "timestamp": ts}},
                         "$push": {"log": log_messages}}
                    )

            # Code below is only to be run by the "first" or only worker
            if self._worker_number > 1:
                continue

            # If the CU was canceled we can skip the remainder of this loop.
            if state == CANCELED:
                continue

            #
            # Check to see if there are more pending Directives, if not, we are Done
            #
            cursor_w = um_col.find({"unitmanager": self.unit_manager_id,
                                    "$or": [ {"Agent_Input_Status": EXECUTING},
                                             {"FTW_Input_Status": EXECUTING}
                                           ]
                                    }
                                   )
            # Iterate over all the returned CUs (if any)
            for wu in cursor_w:
                # See if there are any FTW Input Directives still pending
                if wu['FTW_Input_Status'] == EXECUTING and \
                        not any(d['state'] == EXECUTING or d['state'] == PENDING for d in wu['FTW_Input_Directives']):
                    # All Input Directives for this FTW are done, mark the WU accordingly
                    #wu['FTW_Input_Status'] = DONE # TODO: Is this changing of the "local" copy required?
                    um_col.update({"_id": ObjectId(wu["_id"])},
                                  {'$set': {'FTW_Input_Status': DONE},
                                   '$push': {'log': 'All FTW Input Staging Directives done - %d.' % self._worker_number}})

                # See if there are any Agent Input Directives still pending
                if wu['Agent_Input_Status'] == EXECUTING and \
                        not any(d['state'] == EXECUTING or d['state'] == PENDING for d in wu['Agent_Input_Directives']):
                    # All Input Directives for this Agent are done, mark the WU accordingly
                    #wu['Agent_Input_Status'] = DONE # TODO: Is this changing of the "local" copy required?
                    um_col.update({"_id": ObjectId(wu["_id"])},
                                   {'$set': {'Agent_Input_Status': DONE},
                                    '$push': {'log': 'All Agent Input Staging Directives done - %d.' % self._worker_number}
                                   })

            #
            # Check for all CUs if both Agent and FTW staging is done, we can then mark the CU PendingExecution
            #
            ts = datetime.datetime.utcnow()
            um_col.find_and_modify(
                query={"unitmanager": self.unit_manager_id,
                       "Agent_Input_Status": { "$in": [ NULL, DONE ] },
                       "FTW_Input_Status": { "$in": [ NULL, DONE ] },
                       "state": STAGING_INPUT
                },
                update={"$set": {
                            "state": PENDING_EXECUTION
                        },
                        "$push": {
                            "statehistory": {"state": PENDING_EXECUTION, "timestamp": ts}
                        }
                }
            )
