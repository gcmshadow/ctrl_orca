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


import os
import subprocess
import sys
import re
import time
import lsst.log as log


## CondorJobs - handles interaction with HTCondor
# This class is highly dependent on the output of the condor commands
# condor_submit and condor_q
#
class CondorJobs:
    ## initializer

    def __init__(self):
        log.debug("CondorJobs:__init__")
        return

    ## submit a condor file, and return the job number associated with it.
    # expected output:
    # Submitting job(s).
    # Logging submit event(s).
    # 1 job(s) submitted to cluster 1317.

    def submitJob(self, condorFile):
        log.debug("CondorJobs:submitJob")
        clusterexp = re.compile("1 job\(s\) submitted to cluster (\d+).")

        submitRequest = "condor_submit %s" % condorFile

        pop = os.popen(submitRequest, "r")

        line = pop.readline()
        #line = pop.readline()
        line = pop.readline()
        num = clusterexp.findall(line)
        if len(num) == 0:
            return None
        print "submitted job # %s as file %s" % (num[0], condorFile)
        return num[0]

    ## wait for a condor job to reach it's run state.
    # expected output:
    #-- Submitter: srp@lsst6.ncsa.uiuc.edu : <141.142.15.103:40900> : lsst6.ncsa.uiuc.edu
    # ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD
    #1016.0   srp             5/24 09:17   0+00:00:00 I  0   0.0  launch_joboffices_
    #1017.0   srp             5/24 09:18   0+00:00:00 R  0   0.0  launch_joboffices_

    def waitForJobToRun(self, num, extramsg=None):
        log.debug("CondorJobs:waitForJobToRun")
        jobNum = "%s.0" % num
        queueExp = re.compile("\S+")
        cJobSeen = 0
        print "waiting for job %s to run." % num
        if extramsg != None:
            print extramsg
        secondsWaited = 0
        while 1:
            pop = os.popen("condor_q", "r")
            bJobSeenNow = False
            if (secondsWaited > 0) and ((secondsWaited % 60) == 0):
                minutes = secondsWaited/60
                print "waited %d minute%s so far. still waiting for job %s to run." % ((secondsWaited / 60), ("" if (minutes == 1) else "s"), num)
            while 1:
                line = pop.readline()
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
                    print "Job %s is now being run." % num
                    return runstate
                if (values[0] == jobNum) and (runstate == 'H'):
                    pop.close()
                    # throw exception here
                    print "Job %s is being held.  Please review the logs." % num
                    return runstate
                if (values[0] == jobNum) and (runstate == 'X'):
                    print values
                    pop.close()
                    # throw exception here
                    print "Saw job %s, but it was being aborted" % num
                    return runstate
                if (values[0] == jobNum) and (runstate == 'C'):
                    pop.close()
                    # throw exception here
                    print "Job %s is being cancelled." % num
                    return runstate
            # check to see if we've seen the job before, but that
            # it disappeared
            if (cJobSeen > 0) and (bJobSeenNow == False):
                pop.close()
                print "Was monitoring job %s, but it exitted." % num
                # throw exception
                return None
            pop.close()
            time.sleep(1)
            secondsWaited = secondsWaited + 1

    ## waits for all jobs to enter the run state
    def waitForAllJobsToRun(self, numList):
        log.debug("CondorJobs:waitForAllJobsToRun")
        queueExp = re.compile("\S+")
        jobList = list(numList)
        while 1:
            pop = os.popen("condor_q", "r")
            while 1:
                line = pop.readline()
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

    ## submit a condor dag and return its cluster number
    def condorSubmitDag(self, filename):
        log.debug("CondorJobs: submitCondorDag "+filename)
        # Just a note about why this was done this way...
        # There's something wierd about how "condor_submit_dag" prints it's output.
        # If you run it on the command line, it'll print the "1 job(s) submitted"
        # message as one of the last lines of output.
        # If you redirect output, even on the command line, to a file, it will
        # be one of the first lines.
        # In an effort to avoid having to fix any output behavior issues in the
        # future, we just try and match every line of output with "1 jobs(s) submitted"
        # and if we find, it, we grab the cluster id out of that line.
        clusterexp = re.compile("1 job\(s\) submitted to cluster (\d+).")
        cmd = "condor_submit_dag %s" % filename
        print cmd
        process = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE)
        output = []
        line = process.stdout.readline()
        i = 0
        while line != "":
            line = line.strip()
            output.append(line)
            line = process.stdout.readline()
            i += 1
        for line in output:
            num = clusterexp.findall(line)
            if len(num) != 0:
                # read the rest (if any) and terminate
                stdoutdata, stderrdata = process.communicate()
                return num[0]
        # read the rest (if any) and terminate
        stdoutdata, stderrdata = process.communicate()
        return -1

    ## kill the HTCondor job with a this id
    def killCondorId(self, cid):
        log.debug("CondorJobs: killCondorId"+str(cid))
        cmd = "condor_rm "+str(cid)
        process = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE)
        line = process.stdout.readline()
        while line != "":
            line = line.strip()
            print line
            line = process.stdout.readline()
        # read the rest (if any) and terminate
        stdoutdata, stderrdata = process.communicate()

    ## check to see if the job with id "cid" is still alive
    def isJobAlive(self, cid):
        jobNum = "%s.0" % cid
        queueExp = re.compile("\S+")
        process = subprocess.Popen("condor_q", shell=False, stdout=subprocess.PIPE)
        while 1:
            line = process.stdout.readline()
            if not line:
                break
            values = queueExp.findall(line)
            if len(values) == 0:
                continue
            if (values[0] == jobNum):
                # read the rest (if any) and terminate
                stdoutdata, stderrdata = process.communicate()
                return True
        # read the rest (if any) and terminate
        stdoutdata, stderrdata = process.communicate()
        return False
