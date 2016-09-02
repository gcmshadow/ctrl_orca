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

import lsst.log as log
from lsst.ctrl.orca.multithreading import SharedData

##
# @brief in charge of monitoring and/or controlling the progress of a
#        running workflow.
#


class WorkflowMonitor:

    # initialize
    def __init__(self):

        # _locked: a container for data to be shared across threads that
        # have access to this object.
        self._locked = SharedData(False,
                                  {"running": False, "done": False})

        log.debug("WorkflowMonitor:__init__")
        self._statusListeners = []

    ##
    #
    # @brief add a status listener to this monitor
    #
    def addStatusListener(self, statusListener):
        log.debug("WorkflowMonitor:addStatusListener")
        self._statusListeners.append(statusListener)

    ##
    # @brief handle an event
    #
    def handleEvent(self, event):
        log.debug("WorkflowMonitor:handleEvent")

    ##
    # @brief handle a failure
    #
    def handleFailure(self):
        log.debug("WorkflowMonitor:handleFailure")

    ##
    # @brief return True if the workflow being monitored appears to still be
    #        running
    #
    def isRunning(self):
        return self._locked.running

    ##
    # @brief determine whether workflow has completed
    #
    def isDone(self):
        log.debug("WorkflowMonitor:isDone")
        return self._locked.done

    ##
    # @brief stop the workflow
    #
    def stopWorkflow(self, urgency):
        log.debug("WorkflowMonitor:stopWorkflow")
