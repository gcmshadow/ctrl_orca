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
from __future__ import absolute_import

import os
import os.path
import threading
import time
import lsst.ctrl.events as events
from lsst.ctrl.orca.config.ProductionConfig import ProductionConfig
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.StatusListener import StatusListener
import lsst.log as log

from .EnvString import EnvString
from .exceptions import ConfigurationError
from .exceptions import MultiIssueConfigurationError
from .multithreading import SharedData
from .ProductionRunConfigurator import ProductionRunConfigurator

##
# @brief A class in charge of launching, monitoring, managing, and stopping
# a production run
#


class ProductionRunManager:

    ##
    # @brief initialize
    # @param runid           name of the run
    # @param configFileName  production run config file
    # @param repository      the config repository to assume; this will
    #                          override the value in the config file
    #
    def __init__(self, runid, configFileName, repository=None):

        # _locked: a container for data to be shared across threads that
        # have access to this object.
        self._locked = SharedData(False,
                                  {"running": False, "done": False})

        # the run id for this production
        self.runid = runid

        # once the workflows that make up this production is created we will
        # cache them here
        self._workflowManagers = None

        # a list of workflow Monitors
        self._workflowMonitors = []

        # a list of logger managers
        self._loggerManagers = []

        # the cached ProductionRunConfigurator instance
        self._productionRunConfigurator = None

        # the full path the configuration
        self.fullConfigFilePath = ""
        if os.path.isabs(configFileName) is True:
            self.fullConfigFilePath = configFileName
        else:
            self.fullConfigFilePath = os.path.join(os.path.realpath('.'), configFileName)

        # create Production configuration
        self.config = ProductionConfig()
        # load the production config object
        self.config.load(self.fullConfigFilePath)

        # the repository location
        self.repository = repository

        # determine repository location
        if not self.repository:
            self.repository = self.config.production.repositoryDirectory
        if not self.repository:
            self.repository = "."
        else:
            self.repository = EnvString.resolve(self.repository)

        # shutdown thread
        self._sdthread = None

    ##
    # @brief returns the runid of this production run
    #
    def getRunId(self):
        return self.runid

    ##
    # @brief setup this production to run.
    #
    # If the production was already configured, it will not be
    # reconfigured.
    #
    # @param workflowVerbosity  the verbosity to pass down to configured
    #                             workflows and the pipelines they run.
    # @throws ConfigurationError  raised if any error arises during configuration or
    #                             while checking the configuration.
    #
    def configure(self, workflowVerbosity=None):
        if self._productionRunConfigurator:
            log.info("production has already been configured.")
            return

        # lock this branch of code
        try:
            self._locked.acquire()

            # TODO - SRP
            self._productionRunConfigurator = self.createConfigurator(self.runid,
                                                                      self.fullConfigFilePath)
            workflowManagers = self._productionRunConfigurator.configure(workflowVerbosity)

            self._workflowManagers = {"__order": []}
            for wfm in workflowManagers:
                self._workflowManagers["__order"].append(wfm)
                self._workflowManagers[wfm.getName()] = wfm

            loggerManagers = self._productionRunConfigurator.getLoggerManagers()
            for lm in loggerManagers:
                self._loggerManagers.append(lm)

        finally:
            self._locked.release()

    ##
    # @brief run the entire production
    # @param skipConfigCheck    skip the checks that ensures that configuration
    #                             was completed correctly.
    # @param workflowVerbosity  if not None, override the config-specified logger
    #                             verbosity.  This is only used if the run has not
    #                             already been configured via configure().
    # @return bool  False is returned if the production was already started
    #                   once
    # @throws ConfigurationError  raised if any error arises during configuration or
    #                             while checking the configuration.
    def runProduction(self, skipConfigCheck=False, workflowVerbosity=None):
        log.debug("Running production: " + self.runid)

        if not self.isRunnable():
            if self.isRunning():
                log.info("Production Run %s is already running" % self.runid)
            if self.isDone():
                log.info("Production Run %s has already run; start with new runid" % self.runid)
            return False

        # set configuration check care level.
        # Note: this is not a sanctioned pattern; should be replaced with use
        # of default config.
        checkCare = 1

        if self.config.production.configCheckCare != 0:
            checkCare = self.config.production.configCheckCare
        if checkCare < 0:
            skipConfigCheck = True

        # lock this branch of code
        try:
            self._locked.acquire()
            self._locked.running = True

            # configure the production run (if it hasn't been already)
            if not self._productionRunConfigurator:
                self.configure(workflowVerbosity)

            # make sure the configuration was successful.
            if not self._workflowManagers:
                raise ConfigurationError("Failed to obtain workflowManagers from configurator")

            if skipConfigCheck is False:
                self.checkConfiguration(checkCare)

            # launch the logger daemon
            for lm in self._loggerManagers:
                lm.start()

            # TODO - Re-add when Provenance is complete
            # provSetup = self._productionRunConfigurator.getProvenanceSetup()
            #
            # provSetup.recordProduction()

            for workflow in self._workflowManagers["__order"]:
                mgr = self._workflowManagers[workflow.getName()]

                statusListener = StatusListener()
                # this will block until the monitor is created.
                monitor = mgr.runWorkflow(statusListener, self._loggerManagers)
                self._workflowMonitors.append(monitor)

        finally:
            self._locked.release()

        # start the thread that will listen for shutdown events
        if self.config.production.productionShutdownTopic is not None:
            self._startShutdownThread()

        print("Production launched.")
        print("Waiting for shutdown request.")

    ##
    # @brief determine whether production is currently running
    #
    def isRunning(self):
        #
        # check each monitor.  If any of them are still running,
        # the production is still running.
        #
        for monitor in self._workflowMonitors:
            if monitor.isRunning() is True:
                return True

        with self._locked:
            self._locked.running = False

        return False

    ##
    # @brief determine whether production has completed
    #
    def isDone(self):
        return self._locked.done

    ##
    # @brief determine whether production can be run
    #
    def isRunnable(self):
        return not self.isRunning() and not self.isDone()

    ##
    # @brief setup and create the ProductionRunConfigurator
    # @return ProductionRunConfigurator
    #
    #
    def createConfigurator(self, runid, configFile):
        log.debug("ProductionRunManager:createConfigurator")

        configuratorClass = ProductionRunConfigurator
        configuratorClassName = None
        if self.config.configurationClass is not None:
            configuratorClassName = self.config.configurationClass
        if configuratorClassName is not None:
            classFactory = NamedClassFactory()
            configuratorClass = classFactory.createClass(configuratorClassName)

        return configuratorClass(runid, configFile, self.repository)

    ##
    # @brief
    # @param care      the thoroughness of the checks.
    # @param issueExc  an instance of MultiIssueConfigurationError to add
    #                   problems to.  If not None, this function will not
    #                   raise an exception when problems are encountered; they
    #                   will merely be added to the instance.  It is assumed
    #                   that the caller will raise that exception is necessary.
    def checkConfiguration(self, care=1, issueExc=None):
        # care - level of "care" in checking the configuration to take. In
        # general, the higher the number, the more checks that are made.
        log.debug("checkConfiguration")

        if not self._workflowManagers:
            msg = "%s: production has not been configured yet" % self.runid
            if self._name:
                msg = "%s %s" % (self._name, msg)
            if issueExc is None:
                raise ConfigurationError(msg)
            else:
                issueExc.addProblem(msg)
                return

        myProblems = issueExc
        if myProblems is None:
            myProblems = MultiIssueConfigurationError("problems encountered while checking configuration")

        # check production-wide configuration
        self._productionRunConfigurator.checkConfiguration(care, myProblems)

        # check configuration for each workflow
        for workflow in self._workflowManagers["__order"]:
            workflowMgr = self._workflowManagers[workflow]
            workflowMgr.checkConfiguration(care, myProblems)

        if not issueExc and myProblems.hasProblems():
            raise myProblems

    ##
    # @brief  stops all workflows in this production run
    #
    def stopProduction(self, urgency, timeout=1800):
        # urgency - an indicator of how urgently to carry out the shutdown.
        #
        # Recognized values are:
        #   FINISH_PENDING_DATA - end after all currently available data has
        #                         been processed
        #   END_ITERATION       - end after the current data ooping iteration
        #   CHECKPOINT          - end at next checkpoint opportunity
        #                         (typically between stages)
        #   NOW                 - end as soon as possible, forgoing any
        #                         checkpointing
        if not self.isRunning():
            log.info("shutdown requested when production is not running")
            return

        log.info("Shutting down production (urgency=%s)" % urgency)

        for workflow in self._workflowManagers["__order"]:
            workflowMgr = self._workflowManagers[workflow.getName()]
            workflowMgr.stopWorkflow(urgency)

        pollintv = 0.2
        running = self.isRunning()
        lasttime = time.time()
        while running and timeout > 0:
            time.sleep(pollintv)
            for workflow in self._workflowManagers["__order"]:
                running = self._workflowManagers[workflow.getName()].isRunning()
                if running:
                    break
            timeout -= time.time() - lasttime
        if not running:
            with self._locked:
                self._locked.running = False
                self._locked.done = True
        else:
            log.debug("Failed to shutdown pipelines within timeout: %ss" % timeout)
            return False

        # stop loggers after everything else has died
        for lm in self._loggerManagers:
            lm.stop()

        return True

    ##
    # @brief  return the "short" name for each workflow in this
    # production.
    #
    # These may have been tweaked to ensure a unique list.  These are names
    # that can be passed to getWorkflowManager()
    #
    def getWorkflowNames(self):
        if self._workflowManagers:
            return self._workflowManagers["__order"]
        elif self._productionRunConfigurator:
            return self._productionRunConfigurator.getWorkflowNames()
        else:
            cfg = self.createConfigurator(self.fullConfigFilePath)
            return cfg.getWorkflowNames()

    ##
    # @brief return the workflow manager for the given named workflow
    # @return   a WorkflowManager instance or None if it has not been
    #             created yet or name is not one of the names returned
    #             by getWorkflowNames()
    def getWorkflowManager(self, name):
        if not self._workflowManagers or name not in self._workflowManagers:
            return None
        return self._workflowManagers[name]

    # shutdown thread
    class _ShutdownThread(threading.Thread):
        # @brief initialize the shutdown thread
        def __init__(self, parent, runid, pollingIntv=0.2, listenTimeout=10):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self._runid = runid
            self._parent = parent
            self._pollintv = pollingIntv
            self._timeout = listenTimeout

            brokerhost = parent.config.production.eventBrokerHost

            self._topic = parent.config.production.productionShutdownTopic
            self._evsys = events.EventSystem.getDefaultEventSystem()
            selector = "RUNID = '%s'" % self._runid
            self._evsys.createReceiver(brokerhost, self._topic, selector)

        ##
        # @brief listen for the shutdown event at regular intervals, and shutdown
        # when the event is received.
        #
        def run(self):
            log.debug("listening for shutdown event at %s s intervals" % self._pollintv)

            log.debug("checking for shutdown event")
            log.debug("self._timeout = %s" % self._timeout)
            shutdownEvent = self._evsys.receiveEvent(self._topic, self._timeout)
            while self._parent.isRunning() and shutdownEvent is None:
                time.sleep(self._pollintv)
                shutdownEvent = self._evsys.receiveEvent(self._topic, self._timeout)
            log.debug("DONE!")

            if shutdownEvent:
                shutdownData = shutdownEvent.getPropertySet()
                self._parent.stopProduction(shutdownData.getInt("level"))
            log.debug("Everything shutdown - All finished")

    def _startShutdownThread(self):
        self._sdthread = ProductionRunManager._ShutdownThread(self, self.runid)
        self._sdthread.start()

    ##
    # @returns the shutdown thread for this production
    def getShutdownThread(self):
        return self._sdthread

    ##
    # thread join the shutdown thread for this production
    def joinShutdownThread(self):
        if self._sdthread is not None:
            self._sdthread.join()
