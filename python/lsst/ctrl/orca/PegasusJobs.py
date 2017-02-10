#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
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
from builtins import str
from builtins import object
import os
import subprocess
import re
import time
import lsst.log as log
from lsst.ctrl.orca.CondorJobs import CondorJobs

class PegasusJobs(CondorJobs):
    """Handles interaction with Pegasus
    This class is highly dependent on the output of the pegasus commands
    """

    def __init__(self):
        log.debug("PegasusJobs:__init__")
        return

    def waitForJobToRun(self, num, extramsg=None):
        """Wait for a condor job to reach it's run state.

        Parameters
        ----------
        num : `str`
            job number.
        extramsg : `str`, optional
            addition message to print to stdout

        Notes
        -----
        expected output:
        -- Submitter: srp@lsst6.ncsa.uiuc.edu : <141.142.15.103:40900> : lsst6.ncsa.uiuc.edu
         ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
        1016.0   srp             5/24 09:17   0+00:00:00 I  0   0.0  launch_joboffices_
        1017.0   srp             5/24 09:18   0+00:00:00 R  0   0.0  launch_joboffices_
        """
        log.debug("PegasusJobs:waitForJobToRun")
        jobNum = "%s.0" % num
        queueExp = re.compile("\S+")
        cJobSeen = 0
        print("waiting for job %s to run." % num)
        if extramsg is not None:
            print(extramsg)
        secondsWaited = 0
        while 1:
            pop = os.popen("condor_q", "r")
            bJobSeenNow = False
            if (secondsWaited > 0) and ((secondsWaited % 60) == 0):
                minutes = secondsWaited/60
                msg = "waited %d minute%s so far. still waiting for job %s to run."
                print(msg % ((secondsWaited / 60), ("" if (minutes == 1) else "s"), num))
            while 1:
                line = pop.readline()
                line = line.decode()
                if not line:
                    break
                values = queueExp.findall(line)
                if len(values) == 0:
                    continue
                runstate = values[5]
                if (values[0] == jobNum):
                    cJobSeen = cJobSeen + 1
                    bJobSeenNow = True
                if (values[0] == jobNum) and (runstate == 'R'):
                    pop.close()
                    print("Job %s is now being run." % num)
                    return runstate
                if (values[0] == jobNum) and (runstate == 'H'):
                    pop.close()
                    # throw exception here
                    print("Job %s is being held.  Please review the logs." % num)
                    return runstate
                if (values[0] == jobNum) and (runstate == 'X'):
                    print(values)
                    pop.close()
                    # throw exception here
                    print("Saw job %s, but it was being aborted" % num)
                    return runstate
                if (values[0] == jobNum) and (runstate == 'C'):
                    pop.close()
                    # throw exception here
                    print("Job %s is being cancelled." % num)
                    return runstate
            # check to see if we've seen the job before, but that
            # it disappeared
            if (cJobSeen > 0) and not bJobSeenNow:
                pop.close()
                print("Was monitoring job %s, but it exitted." % num)
                # throw exception
                return None
            pop.close()
            time.sleep(1)
            secondsWaited = secondsWaited + 1

    def waitForAllJobsToRun(self, numList):
        """Waits for all jobs to enter the run state

        Parameters
        ----------
        numList : `list`
            list of condor job ids
        """
        log.debug("PegasusJobs:waitForAllJobsToRun")
        queueExp = re.compile("\S+")
        jobList = list(numList)
        while 1:
            pop = os.popen("condor_q", "r")
            while 1:
                line = pop.readline()
                line = line.decode()
                if not line:
                    break
                values = queueExp.findall(line)
                if len(values) == 0:
                    continue
                jobNum = values[0]
                runstate = values[5]
                for jobEntry in jobList:
                    jobId = "%s.0" % jobEntry
                    if (jobNum == jobId) and (runstate == 'R'):
                        jobList = [job for job in jobList if job[:] != jobEntry]
                        if len(jobList) == 0:
                            return
                        break
                    else:
                        continue
                    if (jobNum == jobEntry) and (runstate == 'H'):
                        pop.close()
                        # throw exception here
                        return
            pop.close()
            time.sleep(1)

    def pegasusSubmitDax(self, sitesFile, transformationFile, daxFile):
        """Submit a pegagus dax and return its cluster number

        Parameters
        ----------
        daxFile : `str`
            name of pegasus DAX file
        """
        log.debug("PegasusJobs: pegasusSubmitDax %s",daxFile)
        """
        Notes - This is the type of output we're dealing with....
        -----
        ---expected output begin---
        2017.02.07 14:06:53.482 CST:
        2017.02.07 14:06:53.488 CST:   -----------------------------------------------------------------------
        2017.02.07 14:06:53.493 CST:   File for submitting this DAG to HTCondor           : CiHscDax-0.dag.condor.sub
        2017.02.07 14:06:53.498 CST:   Log of DAGMan debugging messages                 : CiHscDax-0.dag.dagman.out
        2017.02.07 14:06:53.504 CST:   Log of HTCondor library output                     : CiHscDax-0.dag.lib.out
        2017.02.07 14:06:53.509 CST:   Log of HTCondor library error messages             : CiHscDax-0.dag.lib.err
        2017.02.07 14:06:53.514 CST:   Log of the life of condor_dagman itself          : CiHscDax-0.dag.dagman.log
        2017.02.07 14:06:53.519 CST:
        
        2017.02.07 14:06:53.525 CST:   -no_submit given, not submitting DAG to HTCondor.  You can do this with:
        2017.02.07 14:06:53.535 CST:   -----------------------------------------------------------------------
        2017.02.07 14:06:55.207 CST:   Your database is compatible with Pegasus version: 4.7.0
        2017.02.07 14:06:55.326 CST:   Submitting to condor CiHscDax-0.dag.condor.sub
        2017.02.07 14:06:55.382 CST:   Submitting job(s).
        2017.02.07 14:06:55.387 CST:   1 job(s) submitted to cluster 164870.
        2017.02.07 14:06:55.392 CST:
        
        2017.02.07 14:06:55.398 CST:   Your workflow has been started and is running in the base directory:
        2017.02.07 14:06:55.403 CST:
        2017.02.07 14:06:55.408 CST:     /scratch/srp/condor_scratch/srp_2017_0207_110530/scripts/submit/srp/pegasus/CiHscDax/run0008
        2017.02.07 14:06:55.414 CST:
        2017.02.07 14:06:55.419 CST:   *** To monitor the workflow you can run ***
        2017.02.07 14:06:55.424 CST:
        2017.02.07 14:06:55.429 CST:     pegasus-status -l /scratch/srp/condor_scratch/srp_2017_0207_110530/scripts/submit/srp/pegasus/CiHscDax/run0008
        2017.02.07 14:06:55.435 CST:
        2017.02.07 14:06:55.440 CST:   *** To remove your workflow run ***
        2017.02.07 14:06:55.445 CST:
        2017.02.07 14:06:55.451 CST:     pegasus-remove /scratch/srp/condor_scratch/srp_2017_0207_110530/scripts/submit/srp/pegasus/CiHscDax/run0008
        2017.02.07 14:06:55.456 CST:
        2017.02.07 14:06:55.637 CST:   Time taken to execute is 3.555 seconds
        ---expected output end---
        """

        # expressions to match
        clusterexp = re.compile(".*1 job\(s\) submitted to cluster (\d+).")
        statusexp = re.compile(".*pegasus-status -l ((\W|\w)+)")
        removeexp = re.compile(".*pegasus-remove ((\W|\w)+)")

        cmd = "pegasus-plan -Dpegasus.transfer.links=true -Dpegasus.catalog.site.file=%s -Dpegasus.catalog.transformation.file=%s -Dpegasus.data.configuration=sharedfs --sites lsstvc --output-dir output --dir submit --dax %s --submit" % (sitesFile, transformationFile, daxFile)
        print(cmd)
        log.debug(cmd)
        process = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE)
        output = []
        line = process.stdout.readline()
        line = line.decode()
        i = 0
        while line != "":
            line = line.strip()
            output.append(line)
            line = process.stdout.readline()
            line = line.decode()
            i += 1

        condorClusterId = -1
        statusInfo = None
        removeInfo = None
        for line in output:
            cluster = clusterexp.findall(line)
            if len(cluster) != 0:
                condorClusterId = cluster[0]
                continue
            status = statusexp.findall(line)
            if len(status) != 0:
                statusInfo = status[0]
                continue
            remove = removeexp.findall(line)
            if len(remove) != 0:
                removeInfo = remove[0]
            
        # read the rest (if any) and terminate
        stdoutdata, stderrdata = process.communicate()
        return condorClusterId, statusInfo, removeInfo

    def killCondorId(self, cid):
        """Kill the HTCondor job with a this id

        Parameters
        ----------
        cid : `str`
            condor job id
        """
        log.debug("PegasusJobs: killCondorId %s", str(cid))
        cmd = "condor_rm "+str(cid)
        process = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE)
        line = process.stdout.readline()
        line = line.decode()
        while line != "":
            line = line.strip()
            
        # read the rest (if any) and terminate
        stdoutdata, stderrdata = process.communicate()
        return -1

    def killCondorId(self, cid):
        """Kill the HTCondor job with a this id

        Parameters
        ----------
        cid : `str`
            condor job id
        """
        log.debug("PegasusJobs: killCondorId %s", str(cid))
        cmd = "condor_rm "+str(cid)
        process = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE)
        line = process.stdout.readline()
        line = line.decode()
        while line != "":
            line = line.strip()
            line = process.stdout.readline()
            line = line.decode()
        # read the rest (if any) and terminate
        stdoutdata, stderrdata = process.communicate()

    def isJobAlive(self, cid):
        """Check to see if the job with id "cid" is still alive

        Parameters
        ----------
        cid : `str`
            condor job id
        """
        jobNum = int(cid)
        cmd = "condor_q -af ClusterId"
        process = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE)
        while 1:
            line = process.stdout.readline()
            line = line.strip()
            if not line:
                break
            value = int(line)
            if value == jobNum:
                # read the rest (if any) and terminate
                stdoutdata, stderrdata = process.communicate()
                return True
        # read the rest (if any) and terminate
        stdoutdata, stderrdata = process.communicate()
        return False
