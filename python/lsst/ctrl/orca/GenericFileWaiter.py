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
from builtins import object
import os
import time
import lsst.log as log


class GenericFileWaiter(object):
    """Waits for files to come into existence

    Parameters
    ----------
    fileNames : ['file1', 'file2']
        list of file names
    """

    def __init__(self, fileNames):
        log.debug("GenericFileWaiter:__init__")

        # list of file names
        self.fileNames = fileNames

    def waitForFirstFile(self):
        """Wait for the first file in the list to come into existence
        """
        log.debug("GenericFileWaiter:waitForFirstFile")
        print("waiting for log file to be created to confirm launch.")

        while os.path.exists(self.fileNames[0]) is False:
            time.sleep(1)
        return

    def waitForAllFiles(self):
        """Wait for all files in the list to come into existence
        """
        log.debug("GenericFileWaiter:waitForAllFiles")

        print("waiting for all log files to be created to confirm launch")

        list = self.fileNames
        while len(list):
            newlist = [item for item in list if (os.path.exists(item) is False)]
            list = newlist
            time.sleep(1)
        return
