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

import os
import os.path
import shutil
import stat
import lsst.ctrl.orca as orca

from lsst.ctrl.orca.Directories import Directories
import lsst.log as log

from lsst.ctrl.orca.EnvString import EnvString
from lsst.ctrl.orca.WorkflowConfigurator import WorkflowConfigurator
from lsst.ctrl.orca.GenericPipelineWorkflowLauncher import GenericPipelineWorkflowLauncher
from lsst.ctrl.orca.GenericFileWaiter import GenericFileWaiter

##
#
# GenericPipelineWorkflowConfigurator
#


class GenericPipelineWorkflowConfigurator(WorkflowConfigurator):

    # initializer
    def __init__(self, runid, repository, prodConfig, wfConfig, wfName):
        log.debug("GenericPipelineWorkflowConfigurator:__init__")

        # run id for this workflow
        self.runid = runid

        # production configuration
        self.prodConfig = prodConfig

        # workflow configuration
        self.wfConfig = wfConfig

        # workflow name
        self.wfName = wfName

        # repository location
        self.repository = repository

        # workflow logging verbosity level
        self.wfVerbosity = None

        # nodes used in this workflow
        self.nodes = None

        # directories specified
        self.directories = None

        # @deprecated directories specified
        self.dirs = None

        # the default directory used at the beginning of the run
        self.defaultRunDir = None

        # list of log files
        self.logFileNames = []

        # list of pipeline names
        self.pipelineNames = []

        # host name of the event broker
        self.eventBrokerHost = None

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param provSetup
    # @param wfVerbosity
    #
    def configure(self, provSetup, wfVerbosity):
        self.wfVerbosity = wfVerbosity
        self._configureDatabases(provSetup)
        return self._configureSpecialized(provSetup, self.wfConfig)

    ##
    # @brief Setup as much as possible in preparation to execute the workflow
    #            and return a WorkflowLauncher object that will launch the
    #            configured workflow.
    # @param provSetup provenance info
    # @param wfConfig the workflow config to use for configuration
    #

    def _configureSpecialized(self, provSetup, wfConfig):
        log.debug("GenericPipelineWorkflowConfigurator:configure")

        platformConfig = wfConfig.platform

        # default domain to which we're deploying
        self.defaultDomain = platformConfig.deploy.defaultDomain
        pipelineConfig = wfConfig.pipeline
        print("pipelineConfig = ", pipelineConfig)

        print(">self.wfName = ", self.wfName)
        expandedPipelineConfigs = self.expandConfigs(self.wfName)
        print("expandedPipelineConfig = ", expandedPipelineConfigs)
        launchCmd = []
        for pipelineConfigGroup in expandedPipelineConfigs:
            pipelineConfig = pipelineConfigGroup.getConfigName()
            num = pipelineConfigGroup.getConfigNumber()

            print("pipelineConfigGroup = ", pipelineConfigGroup)
            # TODO - no longer used, remote this, and the method
            self.nodes = self.createNodeList(pipelineConfigGroup.getConfig())
            self.createDirs(platformConfig, pipelineConfig)
            pipelineShortName = pipelineConfig
            launchName = "%s_%d" % (pipelineShortName, num)
            log.debug("GenericPipelineWorkflowConfigurator: launchName = %s" % launchName)
            val = self.deploySetup(provSetup, wfConfig, platformConfig, pipelineConfigGroup)
            launchCmd.append(val)
            log.debug("launchCmd = %s" % launchCmd)
        self.deployData(wfConfig)

        fileWaiter = GenericFileWaiter(self.logFileNames)
        workflowLauncher = GenericPipelineWorkflowLauncher(
            launchCmd, self.prodConfig, wfConfig, self.runid, fileWaiter, self.pipelineNames)
        return workflowLauncher

    ##
    # @brief creates a list of nodes from platform.deploy.nodes
    # @return the list of nodes
    #
    def createNodeList(self, pipelineConfig):
        log.debug("GenericPipelineWorkflowConfigurator:createNodeList")
        print("pipelineConfig = ", pipelineConfig)
        print("pipelineConfig.deploy = ", pipelineConfig.deploy)
        node = pipelineConfig.deploy.processesOnNode

        print("self.expandNodeHost", self.expandNodeHost)
        print("node", node)
        nodes = map(self.expandNodeHost, node)

        # by convention, the master node is the first node in the list
        # we use this later to launch things, so strip out the info past ":", if it's there.
        self.masterNode = nodes[0]

        colon = self.masterNode.find(':')
        if colon > 1:
            self.masterNode = self.masterNode[0:colon]
        return nodes

    # @return the name of this workflow
    def getWorkflowName(self):
        return self.workflow

    # @return the number of nodes to acquire
    def getNodeCount(self):
        return len(self.nodes)

    # deploy any required data specified in the configuration
    def deployData(self, wfConfig):
        log.debug("GenericPipelineWorkflowConfigurator:deployData")

        # add data deploy here

        if wfConfig.configuration["generic"] is not None:
            configuration = wfConfig.configuration["generic"]
            if configuration.deployData is not None:
                deployConfig = configuration.deployData
                dataRepository = deployConfig.dataRepository
                dataRepository = EnvString.resolve(dataRepository)
                deployScript = deployConfig.script
                deployScript = EnvString.resolve(deployScript)
                collection = deployConfig.collection

                if os.path.isfile(deployScript) is True:
                    runDir = self.directories.getDefaultRunDir()
                    deployCmd = [deployScript, runDir, dataRepository, collection]
                    print(">>> ", deployCmd)
                    pid = os.fork()
                    if not pid:
                        os.execvp(deployCmd[0], deployCmd)
                    os.wait()[0]
                else:
                    log.debug("GenericPipelineWorkflowConfigurator:deployData: ",
                              "warning: script '%s' doesn't exist" % deployScript)
        # end data deploy here

    ##
    # deploy the setup information for this workflow
    #
    def deploySetup(self, provSetup, wfConfig, platformConfig, pipelineConfigGroup):
        log.debug("GenericPipelineWorkflowConfigurator:deploySetup")

        pipelineConfig = pipelineConfigGroup.getConfigName()
        shortName = pipelineConfig

        pipelineConfigNumber = pipelineConfigGroup.getConfigNumber()
        pipelineName = "%s_%d" % (shortName, pipelineConfigNumber)

        workDir = self.dirs.get("workDir")

        # create the subdirectory for the pipeline specific files
        logDir = os.path.join(self.dirs.get("workDir"), pipelineName)
        if not os.path.exists(logDir):
            os.makedirs(logDir)

        self.pipelineNames.append(pipelineName)

        # create the list of launch.log files we'll watch for later.
        logFile = os.path.join(pipelineName, "launch.log")
        logFile = os.path.join(workDir, logFile)
        self.logFileNames.append(logFile)

        filename = shortName+"_config.py"

        definitionConfig = wfConfig.pipeline[shortName].definition
        if pipelineConfigNumber == 1:
            if platformConfig.dir is not None:
                print("platformConfig.dir ", platformConfig.dir)
                definitionConfig.execute.dir = platformConfig.dir
            if self.prodConfig.production.eventBrokerHost is not None:
                self.eventBrokerHost = self.prodConfig.production.eventBrokerHost
                definitionConfig.execute.eventBrokerHost = self.eventBrokerHost

            if self.wfConfig.shutdownTopic is not None:
                definitionConfig.execute.shutdownTopic = self.wfConfig.shutdownTopic
            if self.prodConfig.production.logThreshold is not None:
                definitionConfig.execute.logThreshold = self.prodConfig.production.logThreshold
            newConfigFile = os.path.join(workDir, shortName)

            definitionConfig.save(newConfigFile)

            # copy the workerdone.py utility over to the work directory
            script = EnvString.resolve("$CTRL_ORCA_DIR/bin/workerdone.py")
            remoteName = os.path.join(workDir, os.path.basename(script))
            shutil.copyfile(script, remoteName)
            shutil.copystat(script, remoteName)

        # copy /bin/sh script responsible for environment setting

        setupPath = definitionConfig.framework.environment
        if setupPath:
            setupPath = EnvString.resolve(setupPath)

        # the script used for seting up the environment
        self.script = setupPath

        if orca.envscript is None:
            log.info("Using configured setup.sh")
        else:
            self.script = orca.envscript
        if not self.script:
            raise RuntimeError("couldn't find framework.environment")

        # only copy the setup script once
        if pipelineConfigNumber == 1:
            shutil.copy(self.script, workDir)

        # now point at the new location for the setup script
        self.script = os.path.join(workDir, os.path.basename(self.script))

        # create the launch command
        execPath = definitionConfig.framework.script
        execCmd = execPath

        # write out the script we use to kick things off
        launchName = "launch_%s.sh" % pipelineName

        name = os.path.join(logDir, launchName)

        launcher = open(name, 'w')
        launcher.write("#!/bin/sh\n")
        launcher.write("export SHELL=/bin/sh\n")
        launcher.write("cd %s\n" % self.dirs.get("workDir"))
        launcher.write("source %s\n" % self.script)
        launcher.write("eups list --setup 2>/dev/null >%s/eups-env.txt\n" % logDir)

# TODO - insert new code here for provenance recording, once that work is
#        completed.
#        cmds = provSetup.getCmds()
#        workflowConfigs = self.prodConfig.workflow
#
#
#        # append the other information we previously didn't have access to, but need for recording.
#        for cmd in cmds:
#            print "wfConfig = ",wfConfig
#            wfShortName = wfConfig.get("shortName")
#            cmd.append("--activityname=%s_%s" % (wfShortName, pipelineName))
#            # TODO - write "getPath()
#            cmd.append("--platform=%s" % self.getPath(wfConfig.platform))
#            cmd.append("--localrepos=%s" % self.dirs.get("work"))
#            workflowIndex = 1
#            for wfConfig in workflowConfigs:
#                if wfConfig.shortName == wfShortName:
#                    #cmd.append("--activoffset=%s" % workflowIndex)
#                    cmd.append("--activoffset=%s" % globalPipelineOffset)
#                    break
#                workflowIndex = workflowIndex + 1
#            launchCmd = ' '.join(cmd)
#
#            # extract the pipeline config and all the files it includes, and add it to the command
#            filelist = provSetup.extractSinglePipelineFileNames(pipelineConfig, self.repository)
#            fileargs = ' '.join(filelist)
#            launcher.write("%s %s\n" % (launchCmd, fileargs))

        launcher.write("%s %s %s -L %s --logdir %s --workerid %s >%s/launch.log 2>&1\n" %
                       (execCmd, filename, self.runid, self.wfVerbosity, logDir, pipelineName, logDir))
        launcher.write("./workerdone.py %s %s %s\n" % (self.eventBrokerHost, self.runid, pipelineName))
        launcher.close()

        # make it executable
        os.chmod(name, stat.S_IRWXU)

        launchCmd = ["ssh", self.masterNode, name]

        # print "cmd to execute is: ",launchCmd
        return launchCmd

    ##
    # @brief create the platform.dir directories
    #
    def createDirs(self, platformConfig, pipelineConfig):
        log.debug("GenericPipelineWorkflowConfigurator:createDirs")

        print("pipelineConfig = ", pipelineConfig)
        dirConfig = platformConfig.dir
        dirName = pipelineConfig
        self.directories = Directories(dirConfig, dirName, self.runid)
        self.dirs = self.directories.getDirs()

        # default root directory for the platform
        self.defaultRootDir = self.directories.getDefaultRootDir()

        for name in self.dirs.names():
            localDirName = self.dirs.get(name)
            if not os.path.exists(localDirName):
                os.makedirs(localDirName)

    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        log.debug("GenericPipelineWorkflowConfigurator:setupDatabase")

    ##
    # @brief perform a node host name expansion
    #
    def expandNodeHost(self, nodeentry):
        """Add a default network domain to a node list entry if necessary """

        if nodeentry.find(".") < 0:
            node = nodeentry
            colon = nodeentry.find(":")
            if colon == 0:
                raise RuntimeError("bad nodelist format: " + nodeentry)
            elif colon > 0:
                node = nodeentry[0:colon]
                if len(node) < 3:
                    log.debug("Suspiciously short node name: " + node)
                log.debug("-> nodeentry  =" + nodeentry)
                log.debug("-> node  =" + node)

                if self.defaultDomain is not None:
                    node += "."+self.defaultDomain
                nodeentry = "%s:%s" % (node, nodeentry[colon+1:])
            else:
                if self.defaultDomain is not None:
                    nodeentry = "%s%s:1" % (node, self.defaultDomain)
                else:
                    nodeentry = "%s:1" % node
        return nodeentry
