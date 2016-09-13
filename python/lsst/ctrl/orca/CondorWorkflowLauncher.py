from __future__ import print_function
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

import os
import lsst.log as log
from lsst.ctrl.orca.WorkflowLauncher import WorkflowLauncher
from lsst.ctrl.orca.CondorJobs import CondorJobs
from lsst.ctrl.orca.CondorWorkflowMonitor import CondorWorkflowMonitor

# @brief launches condor workflows using DAGman files


class CondorWorkflowLauncher(WorkflowLauncher):
    # @brief initialize

    def __init__(self, prodConfig, wfConfig, runid, localStagingDir, dagFile, monitorConfig):
        log.debug("CondorWorkflowLauncher:__init__")

        # production configuration
        self.prodConfig = prodConfig

        # workflow configuration
        self.wfConfig = wfConfig

        # run id
        self.runid = runid

        # local directory where staging occurs
        self.localStagingDir = localStagingDir

        # DAGman file
        self.dagFile = dagFile

        # monitor configuration
        self.monitorConfig = monitorConfig

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        log.debug("CondorWorkflowLauncher:cleanUp")

    ##
    # @brief launch this workflow
    #
    def launch(self, statusListener, loggerManagers):
        log.debug("CondorWorkflowLauncher:launch")

        # start the monitor first, because we want to catch any pipeline
        # events that might be sent from expiring pipelines.
        eventBrokerHost = self.prodConfig.production.eventBrokerHost
        shutdownTopic = self.prodConfig.production.productionShutdownTopic

        # Launch process
        startDir = os.getcwd()
        os.chdir(self.localStagingDir)

        cj = CondorJobs()
        condorDagId = cj.condorSubmitDag(self.dagFile)
        log.debug("Condor dag submitted as job "+condorDagId)
        os.chdir(startDir)

        # workflow monitor for HTCondor jobs
        self.workflowMonitor = CondorWorkflowMonitor(
            eventBrokerHost, shutdownTopic, self.runid, condorDagId,
            loggerManagers, self.monitorConfig)

        if statusListener is not None:
            self.workflowMonitor.addStatusListener(statusListener)
        self.workflowMonitor.startMonitorThread(self.runid)

        return self.workflowMonitor
