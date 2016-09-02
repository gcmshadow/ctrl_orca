#!/usr/bin/env python

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

from __future__ import print_function
import os
import sys
import optparse
import lsst.pex.policy as pol


class CondorJobInfo:

    class PipelineJob:

        def __init__(self, scratchDir, wfName, pipelineName, pipelineNumber):
            self.wfName = wfName
            self.pipelineName = pipelineName
            self.pipelineNumber = pipelineNumber

            self.pipelineIndexedName = "%s_%s" % (pipelineName, pipelineNumber)
            self.wfScratchDir = os.path.join(scratchDir, self.wfName)

        def getWorkflowName(self):
            return self.wfName

        def getPipelineName(self):
            return self.pipelineName

        def getPipelineNumber(self):
            return int(self.pipelineNumber)

        def getFileName(self):
            pipelineDir = os.path.join(self.wfScratchDir, self.pipelineIndexedName)
            pipelineWorkDir = os.path.join(pipelineDir, "work")
            pipelineJobDir = os.path.join(pipelineWorkDir, self.pipelineIndexedName)
            pipelineJobFile = os.path.join(pipelineJobDir, "%s.job" % self.pipelineIndexedName)
            return pipelineJobFile

    class GlideinJob:

        def __init__(self, scratchDir, wfName):
            self.scratchDir = scratchDir
            self.wfName = wfName
            wfScratchDir = os.path.join(scratchDir, wfName)
            self.glideinFileName = os.path.join(wfScratchDir, "glidein.job")

        def getWorkflowName(self):
            return self.wfName

        def getFileName(self):
            return self.glideinFileName

    def __init__(self, prodPolicy, runid):
        self.prodPolicy = prodPolicy
        self.runid = runid

    ##
    # @brief given a list of pipelinePolicies, number the section we're
    # interested in based on the order they are in, in the productionPolicy
    #
    def getPipelineJobs(self):
        wfPolicies = self.prodPolicy.getArray("workflow")
        expanded = []
        for wfPolicy in wfPolicies:
            wfShortName = wfPolicy.get("shortName")
            pipelinePolicies = wfPolicy.getArray("pipeline")
            localScratch = self.getLocalScratch(wfPolicy)
            for policy in pipelinePolicies:
                runCount = 1  # default to 1, if runCount doesn't exist
                if policy.exists("runCount"):
                    runCount = policy.get("runCount")
                for i in range(0, runCount):
                    pipelineShortName = policy.get("shortName")
                    pipelineJob = self.PipelineJob(localScratch, wfShortName, pipelineShortName, i+1)
                    expanded.append(pipelineJob)
        return expanded

    ##
    # @brief given a list of pipelinePolicies, number the section we're
    # interested in based on the order they are in, in the productionPolicy
    #
    def getGlideinJobs(self):
        wfPolicies = self.prodPolicy.getArray("workflow")
        glideinJobs = []
        for wfPolicy in wfPolicies:
            localScratch = self.getLocalScratch(wfPolicy)
            wfShortName = wfPolicy.get("shortName")
            glideinJob = self.GlideinJob(localScratch, wfShortName)
            glideinJobs.append(glideinJob)
        return glideinJobs

    def getLocalScratch(self, wfPolicy):
        configurationPolicy = wfPolicy.get("configuration")
        condorDataPolicy = configurationPolicy.get("condorData")
        localScratch = condorDataPolicy.get("localScratch")

        return os.path.join(localScratch, self.runid)


class JobKiller:

    def __init__(self, workflowName, pipelineName, pipelineNumber):
        self.workflowName = workflowName
        self.pipelineName = pipelineName
        self.pipelineNumber = pipelineNumber
        return

    # TODO: make this read multiple lines
    def killJob(self, filename):
        print("killJob: ", filename)
        try:
            input = open(filename, 'r')
        except Exception:
            # couldn't find that file, so pass
            return
        line = input.readline()
        line = line.strip('\n')
        cmd = ["condor_rm", line]

        pid = os.fork()
        if not pid:
            os.execvp(cmd[0], cmd)
        os.wait()[0]
        return

    def processGlideinJob(self, job):
        jobFile = job.getFileName()
        if self.workflowName is None:
            self.killJob(jobFile)
        elif self.workflowName == job.getWorkflowName():
            self.killJob(jobFile)
        return

    def processPipelineJob(self, job):
        jobFile = job.getFileName()

        if self.workflowName is None:
            self.killJob(jobFile)
        elif self.workflowName == job.getWorkflowName():
            if self.pipelineName is None:
                self.killJob(jobFile)
            elif self.pipelineName == job.getPipelineName():
                if self.pipelineNumber is None:
                    self.killJob(jobFile)
                elif self.pipelineNumber == job.getPipelineNumber():
                    self.killJob(jobFile)
        return


if __name__ == "__main__":
    usage = """usage %prog [-g] [-w workflow[-p pipeline [-n pipelineNum]] productionPolicyFile runId"""

    parser = optparse.OptionParser(usage)

    parser.add_option("-g", "--glidein", action="store_true",
                      dest="killglidein", default=False, help="kill the glidein")
    parser.add_option("-w", "--workflow", action="store", dest="workflowArg",
                      default=None, help="workflow shortname")
    parser.add_option("-p", "--pipeline", action="store", dest="pipelineArg",
                      default=None, help="pipeline shortname")
    parser.add_option("-n", "--pipelineNum", action="store",
                      dest="pipelineNumArg", default=None, help="pipeline number")

    parser.opts = {}
    parser.args = []

    (parser.opts, parser.args) = parser.parse_args()

    workflowArg = parser.opts.workflowArg
    pipelineArg = parser.opts.pipelineArg
    pipelineNumArg = None
    if parser.opts.pipelineNumArg is not None:
        pipelineNumArg = int(parser.opts.pipelineNumArg)
    killGlidein = parser.opts.killglidein

    if len(parser.args) < 2:
        print(usage)
        raise RuntimeError("Missing args: productionPolicyFile runId")

    prodPolicyFile = parser.args[0]
    runid = parser.args[1]

    prodPolicy = pol.Policy.createPolicy(prodPolicyFile, False)

    jobInfo = CondorJobInfo(prodPolicy, runid)

    killer = JobKiller(workflowArg, pipelineArg, pipelineNumArg)

    if killGlidein is True:
        glideinJobs = jobInfo.getGlideinJobs()
        for job in glideinJobs:
            killer.processGlideinJob(job)
        sys.exit(0)

    pipelineJobs = jobInfo.getPipelineJobs()
    for job in pipelineJobs:
        killer.processPipelineJob(job)
