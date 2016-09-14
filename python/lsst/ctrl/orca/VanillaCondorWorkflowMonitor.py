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

from __future__ import with_statement
from __future__ import print_function
import threading
import time
import lsst.ctrl.events as events
import lsst.log as log

from lsst.daf.base import PropertySet
from lsst.ctrl.orca.WorkflowMonitor import WorkflowMonitor
from lsst.ctrl.orca.multithreading import SharedData

##
# @deprecated VanillaCondorWorkflowMonitor
#


class VanillaCondorWorkflowMonitor(WorkflowMonitor):
    ##
    # @brief in charge of monitoring and/or controlling the progress of a
    #        running workflow.
    #

    def __init__(self, eventBrokerHost, shutdownTopic, runid, pipelineNames, loggerManagers):

        # _locked: a container for data to be shared across threads that
        # have access to this object.
        self._locked = SharedData.SharedData(False, {"running": False, "done": False})

        log.debug("VanillaCondorWorkflowMonitor:__init__")
        self._statusListeners = []
        # make a copy of this liste, since we'll be removing things.

        # named pipelines
        self.pipelineNames = pipelineNames[:]

        # PID of all logger processes
        self.loggerPIDs = []
        for lm in loggerManagers:
            self.loggerPIDs.append(lm.getPID())

        # all logger manager objects
        self.loggerManagers = loggerManagers

        self._eventBrokerHost = eventBrokerHost
        self._shutdownTopic = shutdownTopic

        # ctrl_events monitoring topic
        self.orcaTopic = "orca.monitor"

        # run id for this workflow
        self.runid = runid

        self._wfMonitorThread = None

        # event system object where all transmitters & receivers are registered
        self.eventSystem = events.EventSystem.getDefaultEventSystem()

        # the id of where the events originate.
        self.originatorId = self.eventSystem.createOriginatorId()

        # indicates whether the last logger event been seen
        self.bSentLastLoggerEvent = False

        # indicates whether a job office event has been sent
        self.bSentJobOfficeEvent = False

        with self._locked:
            self._wfMonitorThread = VanillaCondorWorkflowMonitor._WorkflowMonitorThread(
                self, self._eventBrokerHost, self._shutdownTopic, self.orcaTopic, runid)

    # monitor thread which watches for job office events and shutdown events from logger
    class _WorkflowMonitorThread(threading.Thread):
        ##
        # initialize the workflow monitor thread object

        def __init__(self, parent, eventBrokerHost, shutdownTopic, eventTopic, runid):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self._parent = parent
            self._eventBrokerHost = eventBrokerHost
            self._shutdownTopic = shutdownTopic
            self._eventTopic = eventTopic
            selector = "RUNID = '%s'" % runid
            self._receiver = events.EventReceiver(self._eventBrokerHost, self._eventTopic, selector)
            self._Logreceiver = events.EventReceiver(self._eventBrokerHost, "LoggerStatus", selector)
            self._jobOfficeReceiver = events.EventReceiver(self._eventBrokerHost, "JobOfficeStatus", selector)

        ##
        # process  events at regular intervals
        def run(self):
            log.debug("VanillaCondorWorkflowMonitor Thread started")
            sleepInterval = 5
            # we don't decide when we finish, someone else does.
            while True:
                # TODO:  this timeout value should go away when the GIL lock relinquish is
                # implemented in events.
                if sleepInterval != 0:
                    time.sleep(sleepInterval)
                event = self._receiver.receiveEvent(1)

                logEvent = self._Logreceiver.receiveEvent(1)

                jobOfficeEvent = self._jobOfficeReceiver.receiveEvent(1)

                if jobOfficeEvent is not None:
                    val = self._parent.handleJobOfficeEvent(jobOfficeEvent)
                if event is not None:
                    val = self._parent.handleEvent(event)
                    if self._parent._locked.running is False:
                        print("and...done!")
                        return
                elif logEvent is not None:
                    val = self._parent.handleEvent(logEvent)
                    if val is None:
                        print("error receiving last log event")
                        return
                    if self._parent._locked.running is False:
                        print("logger handled... and... done!")
                        return
                if (jobOfficeEvent is not None) or (jobOfficeEvent is not None) \
                        or (jobOfficeEvent is not None):
                    sleepInterval = 0
                else:
                    sleepInterval = 5

    ##
    # Start Monitor Thread running
    def startMonitorThread(self, runid):
        with self._locked:
            self._wfMonitorThread.start()
            self._locked.running = True

    ##
    # watch for event from the job office, and if it indicates the job is
    # completed, stop the workflow
    def handleJobOfficeEvent(self, event):
        log.debug("VanillaCondorWorkflowMonitor:handleJobOfficeEvent")
        if event.getType() == events.EventTypes.STATUS:
            ps = event.getPropertySet()
            status = ps.get("STATUS")
            print("STATUS = "+status)
            if status == "joboffice:done":
                log.debug("VanillaCondorWorkflowMonitor:handleJobOfficeEvent joboffice:done received")
                self.stopWorkflow(1)
        log.debug("VanillaCondorWorkflowMonitor:handleJobOfficeEvent done")
        return

    ##
    # handle incoming events from the pipeline and loggers
    def handleEvent(self, event):
        log.debug("VanillaCondorWorkflowMonitor:handleEvent called")

        # make sure this is really for us.

        ps = event.getPropertySet()

        if event.getType() == events.EventTypes.STATUS:
            ps = event.getPropertySet()

            if ps.exists("pipeline"):
                pipeline = ps.get("pipeline")
                print("pipeline-->", pipeline)
                if pipeline in self.pipelineNames:
                    self.pipelineNames.remove(pipeline)
            elif ps.exists("logger.status"):
                pid = ps.getInt("logger.pid")
                if pid in self.loggerPIDs:
                    self.loggerPIDs.remove(pid)

            cnt = len(self.pipelineNames)
            print("pipelineNames: ")
            print(self.pipelineNames)
            # TODO: clean up to not specifically name "joboffices_1"
            if cnt == 1 and self.pipelineNames[0] == "joboffices_1" and self.bSentJobOfficeEvent is False:
                self.stopWorkflow(1)
                self.bSentJobOfficeEvent = True

            if (cnt == 0) and (self.bSentLastLoggerEvent is False):
                transmitter = events.EventTransmitter(self._eventBrokerHost, events.LogEvent.LOGGING_TOPIC)

                props = PropertySet()
                props.set("LOGGER", "orca.control")
                props.set("STATUS", "eol")
                e = events.Event(self.runid, props)
                transmitter.publishEvent(e)

                self.bSentLastLoggerEvent = True

            # if both lists are empty we're finished.
            if (len(self.pipelineNames) == 0) and (len(self.loggerPIDs) == 0):
                with self._locked:
                    self._locked.running = False
        elif event.getType() == events.EventTypes.COMMAND:
            with self._locked:
                self._locked.running = False
        else:
            print("didn't handle anything")
    ##
    # @brief stop the workflow
    #

    def stopWorkflow(self, urgency):
        log.debug("VanillaCondorWorkflowMonitor:stopWorkflow")
        transmit = events.EventTransmitter(self._eventBrokerHost, self._shutdownTopic)

        root = PropertySet()
        root.setInt("level", urgency)
        root.setString("STATUS", "shut things down")

        event = events.CommandEvent(self.runid, self.originatorId, 0, root)
        transmit.publishEvent(event)
