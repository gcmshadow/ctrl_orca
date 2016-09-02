from __future__ import print_function
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

from lsst.ctrl.orca.LoggerManager import LoggerManager
from lsst.ctrl.orca.NamedClassFactory import NamedClassFactory
from lsst.ctrl.orca.WorkflowManager import WorkflowManager
from lsst.ctrl.orca.config.ProductionConfig import ProductionConfig
from lsst.ctrl.orca.exceptions import MultiIssueConfigurationError
import lsst.log as log

##
# @brief create a basic production run.
# Note that all ProductionRunConfigurator subclasses must support this
# constructor signature.


class ProductionRunConfigurator:
    ##
    # @brief initialize
    #
    def __init__(self, runid, configFile, repository=None, workflowVerbosity=None):

        log.debug("ProductionRunConfigurator:__init__")

        # run id for this production
        self.runid = runid

        self._prodConfigFile = configFile

        # production configuration
        self.prodConfig = ProductionConfig()
        self.prodConfig.load(configFile)

        # location of the repository
        self.repository = repository

        # verbosity level for this workflow
        self.workflowVerbosity = workflowVerbosity

        self._provSetup = None

        # provenance dictionary
        self.provenanceDict = {}
        self._wfnames = None

        # cache the database configurators for checking the configuraiton.
        self._databaseConfigurators = []

        # logger managers
        self._loggerManagers = []

        # hostname of the event broker
        self.eventBrokerHost = None

        # these are config settings which can be overriden from what they
        # are in the workflow policies.

        # dictionary of configuration override values
        self.configOverrides = dict()

        production = self.prodConfig.production
        if production.eventBrokerHost is not None:
            self.eventBrokerHost = production.eventBrokerHost
            self.configOverrides["execute.eventBrokerHost"] = production.eventBrokerHost
        if production.logThreshold is not None:
            self.configOverrides["execute.logThreshold"] = production.logThreshold
        if production.productionShutdownTopic is not None:
            self.configOverrides["execute.shutdownTopic"] = production.productionShutdownTopic

    ##
    # @brief create the WorkflowManager for the pipelien with the given shortName
    #
    def createWorkflowManager(self, prodConfig, wfName, wfConfig):
        log.debug("ProductionRunConfigurator:createWorkflowManager")

        wfManager = WorkflowManager(wfName, self.runid, self.repository, prodConfig, wfConfig)
        return wfManager

    ##
    # @brief return provenanceSetup
    #
    def getProvenanceSetup(self):
        return self._provSetup

    ##
    # @brief configure this production run
    #
    def configure(self, workflowVerbosity):
        log.debug("ProductionRunConfigurator:configure")

        # TODO - IMPORTANT - NEXT TWO LINES ARE FOR PROVENANCE
        # --------------
        # self._provSetup = ProvenanceSetup()
        # self._provSetup.addAllProductionConfigFiles(self._prodConfigFile, self.repository)
        # --------------

        #
        # setup the database for each database listed in production config.
        # cache the configurators in case we want to check the configuration
        # later.
        #
        databaseConfigs = self.prodConfig.database

        for databaseName in databaseConfigs:
            databaseConfig = databaseConfigs[databaseName]
            cfg = self.createDatabaseConfigurator(databaseConfig)
            cfg.setup(self._provSetup)
            dbInfo = cfg.getDBInfo()
            # check to see if we're supposed to launch a logging daemon
            if databaseConfig.logger is not None:
                loggerConfig = databaseConfig.logger
                if loggerConfig.launch is not None:
                    launch = loggerConfig.launch
                    loggerManager = None
                    if launch is True:
                        loggerManager = LoggerManager(self.eventBrokerHost, self.runid, dbInfo[
                                                      "host"], dbInfo["port"], dbInfo["dbrun"])
                    else:
                        loggerManager = LoggerManager(self.eventBrokerHost, self.runid)
                    if loggerManager is not None:
                        self._loggerManagers.append(loggerManager)
            self._databaseConfigurators.append(cfg)

        #
        # do specialized production level configuration, if it exists
        #
        if self.prodConfig.production.configuration.configurationClass is not None:
            specialConfigurationConfig = self.prodConfig.production.configuration
            # XXX - specialConfigurationConfig maybe?
            self.specializedConfigure(specialConfigurationConfig)

        workflowConfigs = self.prodConfig.workflow
        workflowManagers = []
        for wfName in workflowConfigs:
            wfConfig = workflowConfigs[wfName]
            # copy in appropriate production level info into workflow Node  -- ?

            workflowManager = self.createWorkflowManager(self.prodConfig, wfName, wfConfig)
            workflowLauncher = workflowManager.configure(self._provSetup, workflowVerbosity)
            if workflowLauncher is None:
                    raise MultiIssueConfigurationError("error configuring workflowLauncher")

            workflowManagers.append(workflowManager)

        return workflowManagers

    ##
    # @return list of logger managers
    def getLoggerManagers(self):
        return self._loggerManagers

    ##
    # @brief carry out production-wide configuration checks.
    # @param care      the thoroughness of the checks.
    # @param issueExc  an instance of MultiIssueConfigurationError to add
    #                   problems to.  If not None, this function will not
    #                   raise an exception when problems are encountered; they
    #                   will merely be added to the instance.  It is assumed
    #                   that the caller will raise that exception is necessary.
    #
    def checkConfiguration(self, care=1, issueExc=None):
        log.debug("checkConfiguration")
        myProblems = issueExc
        if myProblems is None:
            myProblems = MultiIssueConfigurationError("problems encountered while checking configuration")

        for dbconfig in self._databaseConfigurators:
            print("-> dbconfig = ", dbconfig)
            dbconfig.checkConfiguration(care, issueExc)

        if not issueExc and myProblems.hasProblems():
            raise myProblems

    ##
    # @brief lookup and create the configurator for database operations
    #
    def createDatabaseConfigurator(self, databaseConfig):
        log.debug("ProductionRunConfigurator:createDatabaseConfigurator")
        className = databaseConfig.configurationClass
        classFactory = NamedClassFactory()
        configurationClass = classFactory.createClass(className)
        configurator = configurationClass(self.runid, databaseConfig, self.prodConfig, None)
        return configurator

    ##
    # @brief do any production-wide setup not covered by the setup of the
    # databases or the individual workflows.
    #
    # This implementation does nothing.  Subclasses may override this method
    # to provide specialized production-wide setup.
    #
    def _specializedConfigure(self, specialConfigurationConfig):
        pass

    ##
    # @brief return the workflow names to be used for this set of workflows
    #
    def getWorkflowNames(self):
        return self.prodConfig.workflowNames
