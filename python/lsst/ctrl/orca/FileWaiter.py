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
import sys
import time
import lsst.log as log


##
# waits for files to come into existence on a remote resource
#
class FileWaiter:
    ## initializer

    def __init__(self, remoteNode, remoteFileWaiter, fileListName, logger=None):
        log.debug("FileWaiter:__init__")
        ## name of the remote node to execute on
        self.remoteNode = remoteNode
        ## name of the remote file list file
        self.fileListName = fileListName
        ## name of the remote file waiter script
        self.remoteFileWaiter = remoteFileWaiter

    ## waits for first file in the list to come into existence
    def waitForFirstFile(self):
        log.debug("FileWaiter:waitForFirstFile")
        print "waiting for log file to be created to confirm launch."
        cmd = "gsissh %s %s -f %s" % (self.remoteNode, self.remoteFileWaiter, self.fileListName)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh", cmd.split())
        os.wait()[0]

    ## waits for all files in the list to come into existence
    def waitForAllFiles(self):
        log.debug("FileWaiter:waitForAllFiles")

        print "waiting for all log files to be created to confirm launch"
        cmd = "gsissh %s %s -l %s" % (self.remoteNode, self.remoteFileWaiter, self.fileListName)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh", cmd.split())
        os.wait()[0]
