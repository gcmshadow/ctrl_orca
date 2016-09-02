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
from lsst.ctrl.orca.CondorJobs import CondorJobs


# HTCondor workflow monitor
class CondorWorkflowMonitor(WorkflowMonitor):

    ##
    # @brief in charge of monitoring and/or controlling the progress of a
    #        running workflow.
    #
    def __init__(self, eventBrokerHost, shutdownTopic, runid, condorDagId, loggerManagers, monitorConfig):

        # _locked: a container for data to be shared across threads that
        # have access to this object.
        self._locked = SharedData(False,
                                  {"running": False, "done": False})

        log.debug("CondorWorkflowMonitor:__init__")
        self._statusListeners = []

        # make a copy of this liste, since we'll be removing things.

        # list of logger process ids
        self.loggerPIDs = []
        for lm in loggerManagers:
            self.loggerPIDs.append(lm.getPID())

        # list of logger process managers
        self.loggerManagers = loggerManagers

        self._eventBrokerHost = eventBrokerHost
        self._shutdownTopic = shutdownTopic

        # the topic that orca uses to monitor events
        self.orcaTopic = "orca.monitor"

        # run id for this workflow
        self.runid = runid

        # HTCondor DAG id assigned to this workflow
        self.condorDagId = condorDagId

        # monitor configuration
        self.monitorConfig = monitorConfig

        self._wfMonitorThread = None

        # registry for event transmitters and receivers
        self.eventSystem = events.EventSystem.getDefaultEventSystem()

        # create event identifier for this process
        self.originatorId = self.eventSystem.createOriginatorId()

        self.bSentLastLoggerEvent = False

        with self._locked:
            self._wfMonitorThread = CondorWorkflowMonitor._WorkflowMonitorThread(
                    self, self._eventBrokerHost, self._shutdownTopic,
                    self.orcaTopic, runid, self.condorDagId, self.monitorConfig)

    ##
    # @brief workflow thread that watches for shutdown
    #
    class _WorkflowMonitorThread(threading.Thread):
        ##
        # @brief initialize
        def __init__(self, parent, eventBrokerHost, shutdownTopic,
                     eventTopic, runid, condorDagId, monitorConfig):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self._parent = parent
            self._eventBrokerHost = eventBrokerHost
            self._shutdownTopic = shutdownTopic
            self._eventTopic = eventTopic

            selector = "RUNID = '%s'" % runid
            self._receiver = events.EventReceiver(self._eventBrokerHost, self._eventTopic, selector)
            self._Logreceiver = events.EventReceiver(self._eventBrokerHost, "LoggerStatus", selector)

            # the dag id assigned to this workflow
            self.condorDagId = condorDagId

            # monitor configuration
            self.monitorConfig = monitorConfig

        ##
        # @brief continously monitor incoming events for shutdown sequence
        #
        def run(self):
            cj = CondorJobs()
            log.debug("CondorWorkflowMonitor Thread started")
            statusCheckInterval = int(self.monitorConfig.statusCheckInterval)
            sleepInterval = statusCheckInterval
            # we don't decide when we finish, someone else does.
            while True:
                # TODO:  this timeout value should go away when the GIL lock relinquish is
                # implemented in events.
                if sleepInterval != 0:
                    time.sleep(sleepInterval)
                event = self._receiver.receiveEvent(1)

                logEvent = self._Logreceiver.receiveEvent(1)

                if event is not None:
                    val = self._parent.handleEvent(event)
                    if self._parent._locked.running is False:
                        print("and...done!")
                        return
                elif logEvent is not None:
                    val = self._parent.handleEvent(logEvent)

                    if val is None:
                        raise Exception("error receiving event")

                    if self._parent._locked.running is False:
                        print("logger handled... and... done!")
                        return

                if (event is not None) or (logEvent is not None):
                    sleepInterval = 0
                else:
                    sleepInterval = statusCheckInterval
                # if the dag is no longer running, send the logger an event
                # telling it to clean up.
                if cj.isJobAlive(self.condorDagId) is False:
                    self._parent.sendLastLoggerEvent()

    ##
    # @brief begin one monitor thread
    #
    def startMonitorThread(self, runid):
        with self._locked:
            self._wfMonitorThread.start()
            self._locked.running = True

    ##
    # @brief wait for final shutdown events from the production processes
    #
    def handleEvent(self, event):
        log.debug("CondorWorkflowMonitor:handleEvent called")

        # make sure this is really for us.

        ps = event.getPropertySet()

        # check for Logger event status
        if event.getType() == events.EventTypes.STATUS:
            ps = event.getPropertySet()

            if ps.exists("logger.status"):
                pid = ps.getInt("logger.pid")
                print("logger.pid = ", pid)
                if pid in self.loggerPIDs:
                    self.loggerPIDs.remove(pid)

            # if the logger list is empty, we're finished.
            if len(self.loggerPIDs) == 0:
                with self._locked:
                    self._locked.running = False
        elif event.getType() == events.EventTypes.COMMAND:
            # TODO: stop this thing right now.
            # that means the logger and the dag.
            with self._locked:
                self._locked.running = False
        else:
            print("didn't handle anything")

    # send a message to the logger that we're done
    def sendLastLoggerEvent(self):
        # only do this one time
        if self.bSentLastLoggerEvent is False:
            print("sending last Logger Event")
            transmitter = events.EventTransmitter(self._eventBrokerHost, events.LogEvent.LOGGING_TOPIC)

            props = PropertySet()
            props.set("LOGGER", "orca.control")
            props.set("STATUS", "eol")

            e = events.Event(self.runid, props)
            transmitter.publishEvent(e)

            self.bSentLastLoggerEvent = True

    ##
    # @brief stop the workflow
    #
    def stopWorkflow(self, urgency):
        log.debug("CondorWorkflowMonitor:stopWorkflow")

        # do a condor_rm on the cluster id for the dag we submitted.
        cj = CondorJobs()
        cj.killCondorId(self.condorDagId)

        self.sendLastLoggerEvent()
