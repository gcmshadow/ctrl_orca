#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#


from builtins import object
import lsst.log as log
from lsst.ctrl.orca.multithreading import SharedData


class WorkflowMonitor(object):
    """In charge of monitoring and/or controlling the progress of a running workflow.
    """

    def __init__(self):
        # _locked: a container for data to be shared across threads that
        # have access to this object.
        self._locked = SharedData.SharedData(False, {"running": False, "done": False})

        log.debug("WorkflowMonitor:__init__")
        self._statusListeners = []

    def addStatusListener(self, statusListener):
        """Add a status listener to this monitor
        """
        log.debug("WorkflowMonitor:addStatusListener")
        self._statusListeners.append(statusListener)

    def handleEvent(self, event):
        """Act on an event request
        """
        log.debug("WorkflowMonitor:handleEvent")

    def handleFailure(self):
        """Handle a failure
        """
        log.debug("WorkflowMonitor:handleFailure")

    def isRunning(self):
        """Report if the workflow is running

        Returns
        -------
        running : `bool`
            True if the workflow being monitored appears to still be running
        """
        return self._locked.running

    def isDone(self):
        """Report if the workflow has completed

        Returns
        -------
        done : `bool`
            True if the workflow being monitored has completed
        """
        log.debug("WorkflowMonitor:isDone")
        return self._locked.done

    def stopWorkflow(self, urgency):
        """Stop the workflow
        """
        log.debug("WorkflowMonitor:stopWorkflow")
