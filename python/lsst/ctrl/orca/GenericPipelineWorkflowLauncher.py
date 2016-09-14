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
from lsst.ctrl.orca.GenericPipelineWorkflowMonitor import GenericPipelineWorkflowMonitor

# generic pipeline workflow launcher


class GenericPipelineWorkflowLauncher(WorkflowLauncher):
    """Launcher for HTCondor workflows using DAGman files

    Parameters
    ----------
    cmds : ['cmd1', 'cmd2']
        list of strings containing commands to execute
    prodConfig : Config
        production Config
    wfConfig : Config
        workflow Config
    runid : `str`
        run id
    fileWaiter : object
        object that waits for files to be created
    pipelineNames : ['pipe1', 'pipe2']
        list of strings containing pipeline names
    """

    def __init__(self, cmds, prodConfig, wfConfig, runid, fileWaiter, pipelineNames):
        log.debug("GenericPipelineWorkflowLauncher:__init__")

        # list of commands to execute
        self.cmds = cmds

        # workflow configuration
        self.wfConfig = wfConfig

        # production configuration
        self.prodConfig = prodConfig

        # run id for this workflow
        self.runid = runid

        # object that waits for files to be created
        self.fileWaiter = fileWaiter

        # list of pipeline names
        self.pipelineNames = pipelineNames

    def cleanUp(self):
        """Perform cleanup after workflow has ended.
        """
        log.debug("GenericPipelineWorkflowLauncher:cleanUp")

    def launch(self, statusListener, loggerManagers):
        """Launch this workflow
        """
        log.debug("GenericPipelineWorkflowLauncher:launch")

        eventBrokerHost = self.prodConfig.production.eventBrokerHost
        shutdownTopic = self.wfConfig.shutdownTopic

        # listen on this topic for "workers" sending messages

        # start the monitor first, because we want to catch any pipeline
        # events that might be sent from expiring pipelines.

        # Generic workflow monitor
        self.workflowMonitor = GenericPipelineWorkflowMonitor(
            eventBrokerHost, shutdownTopic, self.runid, self.pipelineNames, loggerManagers)
        if statusListener is not None:
            self.workflowMonitor.addStatusListener(statusListener)

        # start the thread
        self.workflowMonitor.startMonitorThread(self.runid)

        # wait for first file to be created
        firstJob = True
        for key in self.cmds:
            cmd = key
            pid = os.fork()
            if not pid:
                os.execvp(cmd[0], cmd)
            if firstJob is True:
                self.fileWaiter.waitForFirstFile()
                firstJob = False

        # now wait for the rest of the files to be created
        self.fileWaiter.waitForAllFiles()

        return self.workflowMonitor
