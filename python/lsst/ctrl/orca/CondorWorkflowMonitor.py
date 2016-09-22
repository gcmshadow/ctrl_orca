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
from builtins import str
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
    """Monitors the progress of the running workflow.

    Parameters
    ----------
    eventBrokerHost : `str`
        host name of the event broker
    shutdownTopic : `str`
        name of shutdown topic to use for this workflow
    runid : `str`
        run id for this workflow
    condorDagId : `str`
        job id of submitted HTCondor dag
    loggerManagers: [ logMgr1, logMgr2 ]
        list of logger process managers
    monitorConfig : Config
        configuration file for monitor information
    """
    def __init__(self, eventBrokerHost, shutdownTopic, runid, condorDagId, loggerManagers, monitorConfig):

        # _locked: a container for data to be shared across threads that
        # have access to this object.
        self._locked = SharedData.SharedData(False, {"running": False, "done": False})

        log.debug("CondorWorkflowMonitor:__init__")
        self._statusListeners = []

        # make a copy of this liste, since we'll be removing things.

        # list of logger process ids
        self.loggerPIDs = []
        for lm in loggerManagers:
            self.loggerPIDs.append(lm.getPID())

        self.loggerManagers = loggerManagers

        self._eventBrokerHost = eventBrokerHost
        self._shutdownTopic = shutdownTopic

        # the topic that orca uses to monitor events
        self.orcaTopic = "orca.monitor"

        self.runid = runid

        self.condorDagId = condorDagId

        self.monitorConfig = monitorConfig

        self._wfMonitorThread = None

        # registry for event transmitters and receivers
        self.eventSystem = events.EventSystem.getDefaultEventSystem()

        # create event identifier for this process
        self.originatorId = self.eventSystem.createOriginatorId()

        # flag to indicate that last logger event has been sent
        self.bSentLastLoggerEvent = False

        with self._locked:
            self._wfMonitorThread = CondorWorkflowMonitor._WorkflowMonitorThread(self,
                                                                                 self._eventBrokerHost,
                                                                                 self._shutdownTopic,
                                                                                 self.orcaTopic,
                                                                                 runid,
                                                                                 self.condorDagId,
                                                                                 self.monitorConfig)

    class _WorkflowMonitorThread(threading.Thread):
        """Workflow thread that watches for shutdown

        Parameters
        ----------
        parent : `Thread`
            direct parent thread of this thread
        eventBrokerHost : `str`
            host name of the event broker
        shutdownTopic : `str`
            name of shutdown topic to use for this workflow
        runid : `str`
            run id for this workflow
        condorDagId : `str`
            job id of submitted HTCondor dag
        loggerManagers: [ logMgr1, logMgr2 ]
            list of logger process managers
        monitorConfig : `Config`
            configuration file for monitor information
        """
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

        def run(self):
            """Continously monitor incoming events for shutdown sequence
            """
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
                    # val = self._parent.handleEvent(event)
                    self._parent.handleEvent(event)
                    if not self._parent._locked.running:
                        print("and...done!")
                        return
                elif logEvent is not None:
                    self._parent.handleEvent(logEvent)
                    # val = self._parent.handleEvent(logEvent)

                    if not self._parent._locked.running:
                        print("logger handled... and... done!")
                        return

                if not event or not logEvent:
                    sleepInterval = 0
                else:
                    sleepInterval = statusCheckInterval
                # if the dag is no longer running, send the logger an event
                # telling it to clean up.
                if not cj.isJobAlive(self.condorDagId):
                    self._parent.sendLastLoggerEvent()

    def startMonitorThread(self, runid):
        """Begin one monitor thread

        Parameters
        ----------
        runid : `str`
            run id

        Notes
        -----
            run id is current unused
        """
        with self._locked:
            self._wfMonitorThread.start()
            self._locked.running = True

    def handleEvent(self, event):
        """Wait for final shutdown events from the production processes

        Parameters
        ----------
        event : `Event`
            Event message
        """
        log.debug("CondorWorkflowMonitor:handleEvent called")

        # make sure this is really for us.

        ps = event.getPropertySet()

        # check for Logger event status
        if event.getType() == events.EventTypes.STATUS:
            ps = event.getPropertySet()

            if ps.exists("logger.status"):
                pid = ps.getInt("logger.pid")
                log.debug("logger.pid = " + str(pid))
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

    def sendLastLoggerEvent(self):
        """Send a message to the logger that we're done
        """
        # only do this one time
        if not self.bSentLastLoggerEvent:
            print("sending last Logger Event")
            transmitter = events.EventTransmitter(self._eventBrokerHost, events.LogEvent.LOGGING_TOPIC)

            props = PropertySet()
            props.set("LOGGER", "orca.control")
            props.set("STATUS", "eol")

            e = events.Event(self.runid, props)
            transmitter.publishEvent(e)

            self.bSentLastLoggerEvent = True

    def stopWorkflow(self, urgency):
        """Stop the workflow
        """
        log.debug("CondorWorkflowMonitor:stopWorkflow")

        # do a condor_rm on the cluster id for the dag we submitted.
        cj = CondorJobs()
        cj.killCondorId(self.condorDagId)

        self.sendLastLoggerEvent()
