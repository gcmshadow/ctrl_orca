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
import lsst.ctrl.orca as orca
import lsst.log as log
from lsst.ctrl.orca.WorkflowLauncher import WorkflowLauncher
from lsst.ctrl.orca.CondorJobs import CondorJobs
from lsst.ctrl.orca.VanillaCondorWorkflowMonitor import VanillaCondorWorkflowMonitor

##
# @deprecated VanillaCondorWorkflowLauncher
#


class VanillaCondorWorkflowLauncher(WorkflowLauncher):
    ##
    # initializes the workflow launcher for HTCondor Vanilla Universe
    #

    def __init__(self, jobs, localScratch, condorGlideinFile, prodConfig,
                 wfConfig, runid, filewaiter, pipelineNames):
        log.debug("VanillaCondorWorkflowLauncher:__init__")
        # list of jobs being run
        self.jobs = jobs

        # location of local scratch directory
        self.localScratch = localScratch

        # location of condor glide in file
        self.condorGlideinFile = condorGlideinFile

        # production configuration
        self.prodConfig = prodConfig

        # workflow configuration
        self.wfConfig = wfConfig

        # run id for this workflow
        self.runid = runid

        # object which watches files to show up
        self.filewaiter = filewaiter

        # named pipelines
        self.pipelineNames = pipelineNames

    ##
    # @brief perform cleanup after workflow has ended.
    #
    def cleanUp(self):
        log.debug("VanillaCondorWorkflowLauncher:cleanUp")

    ##
    # @brief launch this workflow
    #
    def launch(self, statusListener, loggerManagers):
        log.debug("VanillaCondorWorkflowLauncher:launch")

        # start the monitor first, because we want to catch any pipeline
        # events that might be sent from expiring pipelines.
        eventBrokerHost = self.prodConfig.production.eventBrokerHost
        shutdownTopic = self.wfConfig.shutdownTopic

        # the specialized monitor this workflow
        self.workflowMonitor = VanillaCondorWorkflowMonitor(
            eventBrokerHost, shutdownTopic, self.runid, self.pipelineNames, loggerManagers)
        if statusListener is not None:
            self.workflowMonitor.addStatusListener(statusListener)
        self.workflowMonitor.startMonitorThread(self.runid)

        # Three step launch process
        # 1 - Glidein request
        # wait for glidein to complete
        # 2 - start joboffice job
        # wait for joboffice to start
        # 3 - start all other jobs.
        # wait for all other jobs to start
        # notify the user it's time to do the announceData

        condor = CondorJobs()

        # if we've set the "skipglidein", just don't do that.
        if not orca.skipglidein:
            curDir = os.getcwd()

            # switch to this directory to make condor happy
            os.chdir(self.localScratch)
            glideinJobNumber = condor.submitJob(self.condorGlideinFile)
            os.chdir(curDir)

            # write the glidein job file to the workflow's directory
            tempWorkDir = os.path.dirname(self.condorGlideinFile)
            tempPipelineDir = os.path.dirname(tempWorkDir)
            workflowDir = os.path.dirname(tempPipelineDir)
            glideinJobFile = os.path.join(workflowDir, "glidein.job")
            self.writeJobFile(glideinJobNumber, glideinJobFile)

            # now wait for it to show up.
            condor.waitForJobToRun(glideinJobNumber, "this might take a few minutes.")
        else:
            print("Command line request to skip condor glidein.  Skipping.")

        # for now, make sure joboffice is the first job, launch and wait for it
        firstJob = True
        jobNumbers = []
        for job in self.jobs:
            if firstJob:
                jobNumber = condor.submitJob(job)
                jobDir = os.path.dirname(job)
                jobFileName = os.path.join(jobDir, "%s.job" % os.path.basename(jobDir))
                self.writeJobFile(jobNumber, jobFileName)
                condor.waitForJobToRun(jobNumber)
                # Wait for that first log file to show up from joboffice
                self.filewaiter.waitForFirstFile()
                firstJob = False
            else:
                jobNumber = condor.submitJob(job)
                jobDir = os.path.dirname(job)
                jobFileName = os.path.join(jobDir, "%s.job" % os.path.basename(jobDir))
                self.writeJobFile(jobNumber, jobFileName)
                jobNumbers.append(jobNumber)

        condor.waitForAllJobsToRun(jobNumbers)

        # wait for all jobs to launch
        self.filewaiter.waitForAllFiles()

        return self.workflowMonitor

    ##
    # write a job file containing the launched HTCondor job number
    #
    def writeJobFile(self, jobNumber, jobFile):
        log.debug("VanillaCondorWorkflowLauncher:writeJobFile: writing %s" % jobFile)
        jobfile = open(jobFile, 'w')
        jobfile.write("%s.0\n" % jobNumber)
        jobfile.close()
