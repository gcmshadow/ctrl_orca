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

import argparse
import os
import sys
import lsst.ctrl.events as events

from lsst.daf.base import PropertySet
from lsst.ctrl.orca.db.DatabaseLogger import DatabaseLogger
from lsst.daf.persistence import DbAuth
from lsst.pex.policy import Policy


class EventChecker(object):

    def __init__(self, broker, runid):
        self.runid = runid

        # create an event receiver
        self.receiver = events.EventReceiver(broker, events.LogEvent.LOGGING_TOPIC, "RUNID = '%s'" % runid)

        # create an event transmitter
        self.transmitter = events.EventTransmitter(broker, "LoggerStatus")

    def finalMessageReceived(self, propSet):
        log = propSet.get("LOGGER")
        if log != "orca.control":
            return False
        status = propSet.get("STATUS")
        if status != "eol":
            return False

        eventSystem = events.EventSystem.getDefaultEventSystem()
        id = eventSystem.createOriginatorId()
        root = PropertySet()
        root.setString("logger.status", "done")
        root.setInt("logger.pid", os.getpid())
        event = events.StatusEvent(self.runid, id, root)
        self.transmitter.publishEvent(event)
        return True


class Logger(EventChecker):

    def __init__(self, broker, host, port, runid, database):
        super(Logger, self).__init__(broker, runid)
        # set the highwater mark for the number of messages retrieved before attempting to drain them.
        self.highwatermark = 10000

        self.database = database

        #
        # get database authorization info
        #
        home = os.getenv("HOME")
        pol = Policy(home+"/.lsst/db-auth.paf")

        dbAuth = DbAuth()
        dbAuth.setPolicy(pol)

        user = dbAuth.username(host, port)
        password = dbAuth.password(host, port)

        #
        # create the logger for the database and connect to it
        #
        self.dbLogger = DatabaseLogger(host, int(port))

        self.dbLogger.connect(user, password, self.database)

    def execute(self):
        # set to the name of the file to write to
        tmpFilename = "/dev/shm/logdb_"+self.runid+".txt"

        # incoming messages list
        msgs = []

        # initialize the message counter
        cnt = 0

        #
        # main loop - attempt to get messages until either no messages are retrieved, or until
        # the highwater mark is reached.  If either of these happens, insert all current events
        # in the incoming message list into the database
        #
        while True:
            event = self.receiver.receiveEvent(50)
            if event != None:
                propSet = event.getPropertySet()
                log = propSet.get("LOGGER")
                if log == None:
                    continue
                if self.finalMessageReceived(propSet):
                    self.dbLogger.disconnect()
                    sys.exit(10)
                msgs.append(propSet)
                cnt += 1
                if cnt >= self.highwatermark:
                    self.dbLogger.insertRecords("%s.Logs" % self.database, msgs, tmpFilename)
                    cnt = 0
                    msgs = []
            elif event == None:
                if len(msgs) > 0:
                    self.dbLogger.insertRecords("%s.Logs" % self.database, msgs, tmpFilename)
                    cnt = 0
                    msgs = []


class Monitor(EventChecker):

    def __init__(self, broker, runid):
        super(Monitor, self).__init__(broker, runid)
        pass

    def execute(self):
        while True:
            event = self.receiver.receiveEvent(50)
            if event == None:
                continue
            propSet = event.getPropertySet()
            log = propSet.get("LOGGER")
            if log == None:
                continue
            if self.finalMessageReceived(propSet):
                sys.exit(10)


if __name__ == "__main__":
    basename = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(basename, "monitor logging information and completion of pipeline")

    parser.add_argument("--broker", action="store", dest="broker", required=True)
    parser.add_argument("--runid", action="store", dest="runid", required=True)

    parser.add_argument("--host", action="store", dest="host", default=None, required=False)
    parser.add_argument("--port", action="store", dest="port", default=None, required=False)
    parser.add_argument("--database", action="store", dest="database", default=None, required=False)

    args = parser.parse_args()

    num_args = len([x for x in (args.host, args.port, args.database) if x is not None])

    if num_args == 1 or num_args == 2:
        print "if used, --host --port and --database must be given together"
        sys.exit(10)

    if num_args == 3:
        logger = Logger(args.broker, args.host, args.port, args.runid, args.database)
        logger.execute()
    else:
        mon = Monitor(args.broker, args.runid)
        mon.execute()
