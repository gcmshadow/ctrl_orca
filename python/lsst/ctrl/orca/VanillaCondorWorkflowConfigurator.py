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

import sys
import os
import os.path
import shutil
import sets
import stat
import socket
from sets import Set
import getpass
import lsst.ctrl.orca as orca
import lsst.pex.config as pexConfig

from lsst.ctrl.orca.Directories import Directories
import lsst.log as log

from lsst.ctrl.orca.EnvString import EnvString
#from lsst.ctrl.orca.ConfigUtils import ConfigUtils
from lsst.ctrl.orca.WorkflowConfigurator import WorkflowConfigurator
from lsst.ctrl.orca.VanillaCondorWorkflowLauncher import VanillaCondorWorkflowLauncher
from lsst.ctrl.orca.config.PlatformConfig import PlatformConfig
from lsst.ctrl.orca.TemplateWriter import TemplateWriter
from lsst.ctrl.orca.FileWaiter import FileWaiter

##
#
# @deprecated VanillaCondorWorkflowConfigurator
#


class VanillaCondorWorkflowConfigurator(WorkflowConfigurator):
    ##
    # @brief initalize the workflow configurator
    #

    def __init__(self, runid, repository, prodConfig, wfConfig, wfName):
        log.debug("VanillaCondorWorkflowConfigurator:__init__")

        ## run id for this workflow
        self.runid = runid
        ## repository directory
        self.repository = repository
        ## production configuration
        self.prodConfig = prodConfig
        ## workflow configuration
        self.wfConfig = wfConfig
        ## workflow name
        self.wfName = wfName

        ## the verbosity level for this workflow
        self.wfVerbosity = None

        ## the list of directories in the configuration
        self.dirs = None
        ## @deprecated the list of directories in the configuration
        self.directories = None
        ## the nodes used
        self.nodes = None
        ## the number of nodes requested
        self.numNodes = None
        ## names of the log files
        self.logFileNames = []
        ## names of the pipelines
        self.pipelineNames = []

        ## @deprecated directory list
        self.directoryList = {}
        ## initial remote working directory
        self.initialWorkDir = None
        ## the first remote working directory; (special case)
        self.firstRemoteWorkDir = None

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
    # @param provSetup
    # @param wfConfig
    #
    def _configureSpecialized(self, provSetup, wfConfig):
        log.debug("VanillaCondorWorkflowConfigurator:configure")

        vanConfig = wfConfig.configuration["vanilla"]
        ## remote login user
        self.remoteLoginName = vanConfig.condorData.loginNode
        ## remote ftp node
        self.remoteFTPName = vanConfig.condorData.ftpNode
        ## file transfer protocol prefix to use
        self.transferProtocolPrefix = vanConfig.condorData.transferProtocol+"://"+self.remoteFTPName
        ## local scratch directory
        self.localScratch = vanConfig.condorData.localScratch

        platformConfig = wfConfig.platform

        pipelineConfigs = wfConfig.pipeline
        #expandedPipelineConfigs = self.expandPolicies(self.wfName, pipelineConfigs)
        expandedPipelineConfigs = self.expandConfigs(self.wfName)

        # Collect the entire set of directories to create, and make them.
        # This avoids needlessly making connections to Abe for directories
        # that already exist.
        dirSet = Set()
        print("expandedPipelineConfigs = ", expandedPipelineConfigs)
        for pipelineConfigGroup in expandedPipelineConfigs:
            print("pipelineConfigGroup = ", str(pipelineConfigGroup))
            pipelineConfig = pipelineConfigGroup.getConfigName()
            num = pipelineConfigGroup.getConfigNumber()
            print("pipelineConfig = "+pipelineConfig)
            self.collectDirNames(dirSet, platformConfig, pipelineConfig, num)

        # create all the directories we've collected, plus any local
        # directories we need.
        self.createDirs(dirSet)

        jobs = []
        firstGroup = True
        glideinFileName = None
        remoteFileWaiterName = None
        linkScriptname = None

        ## local file staging directory
        self.localStagingDir = os.path.join(self.localScratch, self.runid)
        ## local workflow directory
        self.localWorkflowDir = os.path.join(self.localStagingDir, self.wfName)
        for pipelineConfigGroup in expandedPipelineConfigs:
            pipelineConfig = pipelineConfigGroup.getConfigName()
            num = pipelineConfigGroup.getConfigNumber()

            print("_configureSpecial = ", pipelineConfig)
            pipelineShortName = pipelineConfig

            # set this pipeline's self.directories and self.dirs
            indexName = "%s_%d" % (pipelineShortName, num)

            self.directories = self.directoryList[indexName]
            self.dirs = self.directories.getDirs()

            indexedNamedDir = os.path.join(self.localWorkflowDir, indexName)
            ## local working directory
            self.localWorkDir = os.path.join(indexedNamedDir, "workDir")

            if not os.path.exists(self.localWorkDir):
                os.makedirs(self.localWorkDir)

            log.debug("VanillaCondorWorkflowConfigurator: indexName = %s" % indexName)

            self.deploySetup(provSetup, wfConfig, platformConfig, pipelineConfigGroup)
            condorFile = self.writeCondorFile(indexName, "launch_%s.sh" % indexName)
            launchCmd = ["condor_submit", condorFile]
            jobs.append(condorFile)
            self.setupDatabase()

            # There are some things that are only added to the first work directory
            if firstGroup == True:
                self.initialWorkDir = self.localWorkDir
                self.createCondorDir(self.localWorkDir)

                # TODO: the term "config here predates pex-config.... change
                # this later, but leave it for now.
                # copy the $CTRL_ORCA_DIR/etc/condor_glidein_config to local
                # START
                condorGlideinConfig = EnvString.resolve("$CTRL_ORCA_DIR/etc/glidein_condor_config")
                condorDir = os.path.join(self.localWorkDir, "Condor_glidein")
                stagedGlideinConfigFile = os.path.join(condorDir, "glidein_condor_config")

                keypairs = wfConfig.configuration["vanilla"].glideinRequest.keyValuePairs
                writer = TemplateWriter()
                writer.rewrite(condorGlideinConfig, stagedGlideinConfigFile, keypairs)

                # write the glidein request script
                glideinFileName = self.writeGlideinRequest(wfConfig.configuration["vanilla"])

                # copy the file creation watching utility
                script = EnvString.resolve("$CTRL_ORCA_DIR/bin/filewaiter.py")
                remoteFileWaiterName = os.path.join(self.dirs.get("workDir"), os.path.basename(script))
                self.copyToRemote(script, remoteFileWaiterName)
                self.remoteChmodX(remoteFileWaiterName)

                # copy workerdone utility over
                script = EnvString.resolve("$CTRL_ORCA_DIR/bin/workerdone.py")
                remoteShutWorkName = os.path.join(self.dirs.get("workDir"), os.path.basename(script))
                self.copyToRemote(script, remoteShutWorkName)
                self.remoteChmodX(remoteShutWorkName)

                # copy the link script once, to the first working directory
                linkScriptName = self.copyLinkScript(wfConfig)

                # keep the first working directory name... we need this later.
                self.firstRemoteWorkDir = self.dirs.get("workDir")

                firstGroup = False
                # END

            log.debug("launchCmd = %s" % launchCmd)

            # after all the pipeline files are placed, copy them to the remote location
            # all at once.
            remoteDir = self.dirs.get("workDir")
            # the extra "/" is required below to copy the entire directory
            #self.copyToRemote(self.localStagingDir+"/*", remoteDir+"/")
            self.copyToRemote(self.localWorkDir+"/*", remoteDir+"/")

            filename = "launch_%s_%d.sh" % (pipelineShortName, num)
            remoteDir = os.path.join(self.dirs.get("workDir"), "%s_%d" % (pipelineShortName, num))
            remoteFilename = os.path.join(remoteDir, filename)
            self.remoteChmodX(remoteFilename)

        # Now that all the input directories are made, run the link script
        if linkScriptName != None:
            self.runLinkScript(wfConfig, linkScriptName)

        # write the logFiles, and copy them over for the filewaiter utility to use
        orcaLogFileName = os.path.join(self.initialWorkDir, 'orca_logfiles.txt')

        logFile = open(orcaLogFileName, 'w')
        for name in self.logFileNames:
            logFile.write("%s\n" % name)
        logFile.close()

        remoteLogNamesFile = os.path.join(self.firstRemoteWorkDir, 'orca_logfiles.txt')
        self.copyToRemote(orcaLogFileName, remoteLogNamesFile)

        # create the FileWaiter
        fileWaiter = FileWaiter(self.remoteLoginName, remoteFileWaiterName, remoteLogNamesFile)

        # create the Launcher

        workflowLauncher = VanillaCondorWorkflowLauncher(
            jobs, self.initialWorkDir, glideinFileName, self.prodConfig, self.wfConfig, self.runid, fileWaiter, self.pipelineNames)
        return workflowLauncher

    ##
    # @brief create the command which will launch the workflow
    # @return a string containing the shell commands to execute
    #
    def writeCondorFile(self, launchNamePrefix, launchScriptName):
        log.debug("VanillaCondorWorkflowConfigurator:writeCondorFile")

        condorJobFile = os.path.join(self.localWorkDir, launchNamePrefix)
        condorJobFile = os.path.join(condorJobFile, launchNamePrefix+".condor")

        clist = []
        clist.append("universe=vanilla\n")
        clist.append("executable=%s/%s\n" % (launchNamePrefix, launchScriptName))
        clist.append("transfer_executable=false\n")
        clist.append("output=%s/%s/Condor.out\n" % (self.localWorkDir, launchNamePrefix))
        clist.append("error=%s/%s/Condor.err\n" % (self.localWorkDir, launchNamePrefix))
        clist.append("log=%s/%s/Condor.log\n" % (self.localWorkDir, launchNamePrefix))
        clist.append("should_transfer_files = YES\n")
        clist.append("when_to_transfer_output = ON_EXIT\n")
        clist.append("remote_initialdir="+self.dirs.get("workDir")+"\n")
        clist.append(
            "Requirements = (FileSystemDomain != \"dummy\") && (Arch != \"dummy\") && (OpSys != \"dummy\") && (Disk != -1) && (Memory != -1)\n")
        clist.append("queue\n")

        # Create a file object: in "write" mode
        condorFILE = open(condorJobFile, "w")
        condorFILE.writelines(clist)
        condorFILE.close()

        return condorJobFile

    ##
    # @return the workflow name
    #
    def getWorkflowName(self):
        return self.wfName

    ##
    # perform local staging of files
    def stageLocally(self, localName, remoteName):
        log.debug("VanillaCondorWorkflowConfigurator:stageLocally")

        print("local name  = "+localName)
        print("remote name  = "+remoteName)

        localStageName = os.path.join(self.localStagingDir, remoteName)

        print("localName = %s, localStageName = %s\n", (localName, localStageName))
        if os.path.exists(os.path.dirname(localStageName)) == False:
            os.makedirs(os.path.dirname(localStageName))
        shutil.copyfile(localName, localStageName)

    ##
    # copy the locally staged directory structure to the remote site
    def copyToRemote(self, localName, remoteName):
        log.debug("VanillaCondorWorkflowConfigurator:copyToRemote")

        localNameURL = "%s%s" % ("file://", localName)
        remoteNameURL = "%s%s" % (self.transferProtocolPrefix, remoteName)

        cmd = "globus-url-copy -r -vb -cd %s %s " % (localNameURL, remoteNameURL)
        print(cmd)

        # perform this copy from the local machine to the remote machine
        pid = os.fork()
        if not pid:
            # when forking stuff, gotta close *BOTH* the python and C level
            # file descriptors. not strictly needed here, since we're just
            # shutting off stdout and stderr, but a good habit to be in.

            # TODO: Change this to add a check to not close file descriptors
            # if verbosity is set high enough, so you can see the output of the
            # globus-url-copy
            sys.stdin.close()
            sys.stdout.close()
            sys.stderr.close()
            os.close(0)
            os.close(1)
            os.close(2)
            os.execvp("globus-url-copy", cmd.split())
        os.wait()[0]

    ##
    # create remote directories
    def remoteMakeDirs(self, remoteName):
        log.debug("VanillaCondorWorkflowConfigurator:remoteMakeDirs")
        cmd = "gsissh %s mkdir -p %s" % (self.remoteLoginName, remoteName)
        print(cmd)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh", cmd.split())
        os.wait()[0]

    ##
    # chmod executables remotely
    # this needs to be done because the cp does not preserve the execution bit
    def remoteChmodX(self, remoteName):
        log.debug("VanillaCondorWorkflowConfigurator:remoteChmodX")
        cmd = "gsissh %s chmod +x %s" % (self.remoteLoginName, remoteName)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh", cmd.split())
        os.wait()[0]

    ##
    #  @deprecated create remote directories
    def remoteMkdir(self, remoteDir):
        log.debug("VanillaCondorWorkflowConfigurator:remoteMkdir")
        cmd = "gsissh %s mkdir -p %s" % (self.remoteLoginName, remoteDir)
        print("running: "+cmd)
        pid = os.fork()
        if not pid:
            os.execvp("gsissh", cmd.split())
        os.wait()[0]
    ##
    # @brief create setup configurations
    #

    def deploySetup(self, provSetup, wfConfig, platformConfig, pipelineConfigGroup):
        log.debug("VanillaCondorWorkflowConfigurator:deploySetup")

        pipelineConfig = pipelineConfigGroup.getConfigName()
        shortName = pipelineConfig
        #shortName = pipelineConfig.shortName

        pipelineConfigNumber = pipelineConfigGroup.getConfigNumber()
        pipelineName = "%s_%d" % (shortName, pipelineConfigNumber)

        globalPipelineOffset = pipelineConfigGroup.getGlobalOffset()

        logDir = os.path.join(self.localWorkDir, pipelineName)
        # create the log directories under the local scratch work
        if not os.path.exists(logDir):
            os.makedirs(logDir)

        self.pipelineNames.append(pipelineName)

        # create the list of launch.log file's we'll watch for later.
        logFile = os.path.join(pipelineName, "launch.log")
        logFile = os.path.join(self.dirs.get("workDir"), logFile)
        self.logFileNames.append(logFile)

        print("self.prodConfig = ", self.prodConfig)
        eventBrokerHost = self.prodConfig.production.eventBrokerHost
        if eventBrokerHost == None:
            print("warning: eventBrokerHost is not set")

        # only write out the config file once
# TODO - XXX - K-T says this is obsoleted, since we won't copy config info anymore
#        filename = pipelineConfig.definition
#        fullpath = None
#        if os.path.isabs(filename):
#            fullpath = filename
#        else:
#            fullpath = os.path.join(self.repository, filename)
#        pipelineDescriptionConfig = PipelineDescriptionConfig()
#        pipelineDescriptionConfig.load(fullpath)
#        if pipelineConfigNumber == 1:
#            if platformConfig.dir != None:
#                pipelineDescriptionConfig.execute.dir =  platformConfig.dir
#            if eventBrokerHost is not None:
#                pipelineDescriptionConfig.execute.eventBrokerHost = eventBrokerHost
#
#            if self.wfConfig.shutdownTopic != None:
#                pipelineDescriptionConfig.execute.shutdownTopic = self.wfConfig.shutdownTopic
#            if self.prodConfig.logThreshold != None:
#                pipelineDescriptionConfig.execute.logThreshold = self.prodConfig.logThreshold
#            # TODO - write this for config files
#            newConfigFile = os.path.join(self.localWorkDir, filename)
#            pw = pol.PAFWriter(newConfigFile)
#            pw.write(pipelineDescriptionConfig)
#            pw.close()

        # copy /bin/sh script responsible for environment setting

        print("shortName = ", shortName)
        pipelineDefinitionConfig = wfConfig.pipeline[shortName].definition
        setupPath = pipelineDefinitionConfig.framework.environment
        print("setupPath = ", setupPath)
        if setupPath:
            setupPath = EnvString.resolve(setupPath)
        ## path to the setup script
        self.script = setupPath

        if orca.envscript is None:
            log.info("Using configured setup.sh")
        else:
            self.script = orca.envscript
        if not self.script:
            raise RuntimeError("couldn't find framework.environment")

        # only copy the setup script once
        if pipelineConfigNumber == 1:
            shutil.copy(self.script, self.localWorkDir)

        # now point at the new location for the setup script
        self.script = os.path.join(self.dirs.get("workDir"), os.path.basename(self.script))

        #
        # Write all config files out to the work directory,
        # but only do it once.

#
# XXX - configs won't be copied they way policies were... ?
#
#       if pipelineConfigNumber == 1:
#
#            # first, grab all the file names, and throw them into a Set() to
#            # avoid duplication
#            pipelineConfigSet = sets.Set()
#
#            ConfigUtils.getAllFilenames(self.repository, pipelineDescriptionConfig, pipelineConfigSet)
#
#            # Cycle through the file names, creating subdirectories as required,
#            # and copy them to the destination directory
#            for configFile in pipelineConfigSet:
#                destName = configFile.replace(self.repository+"/","")
#                tokens = destName.split('/')
#                tokensLength = len(tokens)
#                destinationFile = tokens[len(tokens)-1]
#                destintationDir = self.localWorkDir
#                for newDestinationDir in tokens[:len(tokens)-1]:
#                    newDir = os.path.join(self.localWorkDir, newDestinationDir)
#                    if os.path.exists(newDir) == False:
#                        os.mkdir(newDir)
#                    destinationDir = newDir
#                shutil.copyfile(configFile, os.path.join(destinationDir, destinationFile))

        # create the launch command

        execPath = pipelineDefinitionConfig.framework.script
        execCmd = execPath

        # write out the script we use to kick things off
        launchName = "launch_%s.sh" % pipelineName

        name = os.path.join(logDir, launchName)

        remoteLogDir = os.path.join(self.dirs.get("workDir"), pipelineName)

        launcher = open(name, 'w')
        launcher.write("#!/bin/sh\n")
        launcher.write("export SHELL=/bin/sh\n")
        launcher.write("cd %s\n" % self.dirs.get("workDir"))
        launcher.write("/bin/rm -f %s/launch.log\n" % remoteLogDir)

        launcher.write("source %s\n" % self.script)

        launcher.write("eups list --setup 2>/dev/null >%s/eups-env.txt\n" % remoteLogDir)


# TODO - rework this when provenance is back in.
#        cmds = provSetup.getCmds()
#        workflowConfigs = self.prodConfig.workflow

#        # append the other information we previously didn't have access to, but need for recording.
#        for cmd in cmds:
#            wfShortName = wfConfig.shortName
#            cmd.append("--activityname=%s_%s" % (wfShortName, pipelineName))
#            cmd.append("--platform=%s" % self.getPath(wfConfig.platform))
#            cmd.append("--localrepos=%s" % self.dirs.get("workDir"))
#            workflowIndex = 1
#            workflowNames = self.prodConfig.workflowNames
#            for workflowName in workflowNames:
#                wfConfig = workflowConfigs[workflowName]
#                if wfConfig.shortName == wfShortName:
#                    #cmd.append("--activoffset=%s" % workflowIndex)
#                    cmd.append("--activoffset=%s" % globalPipelineOffset)
#                    break
#                workflowIndex = workflowIndex + 1
#            launchCmd = ' '.join(cmd)
#
#            # extract the pipeline config and all the files it includes, and add it to the command
#            filelist = provSetup.extractSinglePipelineFileNames(pipelineConfig, repository)
#            fileargs = ' '.join(filelist)
#            launcher.write("%s %s\n" % (launchCmd, fileargs))
        # TODO - re-add provenance command
        launcher.write("# provenance command was here...removed for now.")

        # TODO - "filename" needs to be a save() of pipelineDefinitionConfig
        filename = pipelineName+"_replace_this_with_a_config_file.py"
        # On condor, you have to launch the script, then wait until that
        # script exits.
        # TODO: remove the line below, and delete it when ticket 1398 is
        # verified to be working.
        #launcher.write("%s %s %s -L %s --logdir %s >%s/launch.log 2>&1 &\n" % (execCmd, filename, self.runid, self.wfVerbosity, remoteLogDir, remoteLogDir))
        launcher.write("%s %s %s -L %s --logdir %s --workerid %s >%s/launch.log 2>&1 &\n" %
                       (execCmd, filename, self.runid, self.wfVerbosity, remoteLogDir, pipelineName, remoteLogDir))
        launcher.write("wait\n")
        launcher.write("./workerdone.py %s %s %s\n" % (eventBrokerHost, self.runid, pipelineName))
        #launcher.write('echo "from launcher"\n')
        #launcher.write("ps -ef\n")

        launcher.close()

        return

    ##
    # @deprecated collect all directories names from the configuration
    #
    def collectDirNames(self, dirSet, platformConfig, pipelineConfig, num):
        log.debug("VanillaCondorWorkflowConfigurator:collectDirNames")

        dirConfig = platformConfig.dir
        dirName = pipelineConfig
        print("collectDirNames dirConfig = ", dirConfig)
        print("collectDirNames dirName = ", dirName)

        directories = Directories(dirConfig, dirName, self.runid)
        dirs = directories.getDirs()
        for name in dirs.names():
            remoteName = dirs.get(name)
            dirSet.add(remoteName)
        indexedDirName = "%s_%d" % (dirName, num)
        self.directoryList[indexedDirName] = directories
        return

    ##
    # @brief create the platform.dir directories
    #
    def createDirs(self, dirSet):
        log.debug("VanillaCondorWorkflowConfigurator:createDirs")
        for remoteName in dirSet:
            self.remoteMakeDirs(remoteName)

    ##
    # create the local condor glidein directory
    #
    def createCondorDir(self, workDir):
        # create Condor_glidein/local directory under "workDir"
        condorDir = os.path.join(workDir, "Condor_glidein")
        condorLocalDir = os.path.join(condorDir, "local")
        if not os.path.exists(condorLocalDir):
            os.makedirs(condorLocalDir)

    ##
    # @brief set up this workflow's database
    #
    def setupDatabase(self):
        log.debug("VanillaCondorWorkflowConfigurator:setupDatabase")

    ##
    # Copy the link script to the remote directory
    #
    def copyLinkScript(self, wfConfig):
        log.debug("VanillaPipelineWorkflowConfigurator:copyLinkScript")

        if wfConfig.configuration["vanilla"] == None:
            return None
        configuration = wfConfig.configuration["vanilla"]
        if configuration.deployData == None:
            return None
        deployConfig = configuration.deployData
        dataRepository = deployConfig.dataRepository
        deployScript = deployConfig.script
        deployScript = EnvString.resolve(deployScript)
        collection = deployConfig.collection

        if os.path.isfile(deployScript) == True:
            # copy the script to the remote side
            remoteName = os.path.join(self.dirs.get("workDir"), os.path.basename(deployScript))
            self.copyToRemote(deployScript, remoteName)
            self.remoteChmodX(remoteName)
            return remoteName
        log.debug("GenericPipelineWorkflowConfigurator:deployData: warning: script '%s' doesn't exist" % deployScript)
        return None

    ##
    # run the remote linking script
    #
    def runLinkScript(self, wfConfig, remoteName):
        log.debug("VanillaPipelineWorkflowConfigurator:runLinkScript")
        configuration = wfConfig.configuration["vanilla"]
        if configuration == None:
            return

        if configuration.deployData == None:
            return

        deployConfig = configuration.deployData
        dataRepository = deployConfig.dataRepository
        collection = deployConfig.collection

        runDir = self.directories.getDefaultRunDir()
        # run the linking script
        deployCmd = ["gsissh", self.remoteLoginName, remoteName, runDir, dataRepository, collection]
        pid = os.fork()
        if not pid:
            os.execvp(deployCmd[0], deployCmd)
        os.wait()[0]
        return

    ###
    # write the glidein request using the template
    #
    def writeGlideinRequest(self, config):
        log.debug("VanillaPipelineWorkflowConfigurator:writeGlideinRequest")
        glideinRequest = config.glideinRequest
        templateFileName = glideinRequest.templateFileName
        templateFileName = EnvString.resolve(templateFileName)
        outputFileName = glideinRequest.outputFileName

        keyValuePairs = glideinRequest.keyValuePairs

        realFileName = os.path.join(self.localWorkDir, outputFileName)

        # for glidein request, we add this additional keyword.
        keyValuePairs["ORCA_REMOTE_WORKDIR"] = self.dirs.get("workDir")
        if ("START_OWNER" in keyValuePairs) == False:
            keyValuePairs["START_OWNER"] = getpass.getuser()

        writer = TemplateWriter()
        writer.rewrite(templateFileName, realFileName, keyValuePairs)

        return realFileName
