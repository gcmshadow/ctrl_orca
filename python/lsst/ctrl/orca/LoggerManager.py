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


from builtins import object
import os
import signal
import subprocess
import lsst.log as log


class LoggerManager(object):
    """Manage the Logger process

    Parameters
    ----------
    broker : `str`
        the name of the event broker host
    runid : `str`
        run id
    dbHost : `str`, optional
        the name of the database process host
    dbPort : `int`, optional
        the port number of the database process
    dbName : `str`, optional
        the name of the database
    """

    def __init__(self, broker, runid, dbHost=None, dbPort=None, dbName=None):
        log.debug("LoggerManager:__init__")

        # the event broker to listen on
        self.broker = broker

        # the run id of the logger messages to listen for
        self.runid = runid

        # the database host to connect to
        self.dbHost = dbHost

        # the database port to connect to
        self.dbPort = dbPort

        # the database name
        self.dbName = dbName

        # the logger process
        self.process = None

        return

    def getPID(self):
        """Access to get logger process id
        Returns
        -------
        pid : `int`
            process id
        """
        return self.process.pid

    def start(self):
        """Start the logger daemon process
        """
        log.debug("LoggerManager:start")
        if self.process is not None:
            return

        directory = os.getenv("CTRL_ORCA_DIR")
        cmd = None
        if self.dbHost is None:
            cmd = "%s/bin/Logger.py --broker %s --runid %s" % (directory, self.broker, self.runid)
        else:
            cmd = "%s/bin/Logger.py --broker %s --host %s --port %s --runid %s --database %s" % (
                directory, self.broker, self.dbHost, self.dbPort, self.runid, self.dbName)
        log.debug("LoggerManager:cmd = %s " % cmd)
        self.process = subprocess.Popen(cmd, shell=True)
        return

    def stop(self):
        """Halt the logger daemon process
        """
        log.debug("LoggerManager:stop")
        if self.process is None:
            return

        try:
            os.kill(self.process.pid, signal.SIGKILL)
            os.waitpid(self.process.pid, 0)
            self.process = None
            log.debug("LoggerManager:stop: killed Logger process")
        except Exception:
            log.debug("LoggerManager:stop: tried to kill Logger process, but it didn't exist")

        return
