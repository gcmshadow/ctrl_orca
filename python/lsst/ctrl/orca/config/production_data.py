config.production.shortName = "DataRelease"
config.production.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
config.production.repositoryDirectory = "./pipeline"
config.production.productionShutdownTopic = "productionShutdown"

config.databaseConfigNames = ["db1"]
config.database["db1"].name = "dc3bGlobal"
config.database["db1"].system.authInfo.host = "fester.ncsa.uiuc.edu"
config.database["db1"].system.authInfo.port = 3306
config.database["db1"].system.runCleanup.daysFirstNotice = 7
config.database["db1"].system.runCleanup.daysFinalNotice = 1

config.database["db1"].configurationClass = "lsst.ctrl.orca.db.DC3Configurator"
config.database["db1"].configuration["production"].globalDbName = "GlobalDB"
config.database["db1"].configuration["production"].dcVersion = "PT1_2"
config.database["db1"].configuration["production"].dcDbName = "DC3b_DB"
config.database["db1"].configuration["production"].minPercDiskSpaceReq = 10
config.database["db1"].configuration["production"].userRunLife = 2
config.database["db1"].logger.launch = True

config.workflowNames = ["PT1Workflow"]

config.workflow["PT1Workflow"].platform.dir.defaultRoot = "/scratch/00482/srp/datarel-runs"
config.workflow["PT1Workflow"].platform.dir.runDirPattern = "%(runid)s"
config.workflow["PT1Workflow"].platform.dir.workDir = "work"
config.workflow["PT1Workflow"].platform.dir.inputDir = "input"
config.workflow["PT1Workflow"].platform.dir.outputDir = "output"
config.workflow["PT1Workflow"].platform.dir.updateDir = "update"
config.workflow["PT1Workflow"].platform.dir.scratchDir = "scratch"

config.workflow["PT1Workflow"].platform.hw.nodeCount = 1000
config.workflow["PT1Workflow"].platform.hw.minCoresPerNode = 12
config.workflow["PT1Workflow"].platform.hw.maxCoresPerNode = 12
config.workflow["PT1Workflow"].platform.hw.minRamPerNode = 32.0
config.workflow["PT1Workflow"].platform.hw.maxRamPerNode = 32.0

config.workflow["PT1Workflow"].platform.deploy.defaultDomain = "tacc.utexas.edu"
config.workflow["PT1Workflow"].platform.deploy.nodes = ["remoteabe1:8", "remoteabe2:8",
                                                        "remoteabe3:8", "remoteabe4:8", "remoteabe5:8", "remoteabe6:8", "remoteabe7:8", "remoteabe8:8"]

config.workflow["PT1Workflow"].shutdownTopic = "workflowShutdown"

config.workflow["PT1Workflow"].configurationClass = "lsst.ctrl.orca.VanillaCondorWorkflowConfigurator"
config.workflow["PT1Workflow"].configuration[
    "vanilla"].deployData.dataRepository = "/scratch/00342/ux453102/base/lsst/pt1_2-410/data/obs"
config.workflow["PT1Workflow"].configuration["vanilla"].deployData.collection = "ImSim"
config.workflow["PT1Workflow"].configuration[
    "vanilla"].deployData.script = "$DATAREL_DIR/bin/runOrca/deployData-abe.sh"

config.workflow["PT1Workflow"].configuration["vanilla"].condorData.localScratch = "/home/daues/orca_scratch"
config.workflow["PT1Workflow"].configuration["vanilla"].condorData.loginNode = "login1.ls4.tacc.utexas.edu"
config.workflow["PT1Workflow"].configuration["vanilla"].condorData.ftpNode = "gridftp1.ls4.tacc.utexas.edu"
config.workflow["PT1Workflow"].configuration["vanilla"].condorData.transferProtocol = "gsiftp"
config.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyNames = [
    "CONDOR_SBIN", "CPU_COUNT", "MACHINE_COUNT", "QUEUE", "MAX_WALLTIME", "PROJECT"]
config.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs[
    "CONDOR_SBIN"] = "/share/apps/teragrid/condor-7.4.4.-r1/sbin"
config.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs["CPU_COUNT"] = "6"
config.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs["MACHINE_COUNT"] = "64"
config.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs["QUEUE"] = "normal"
config.workflow["PT1Workflow"].configuration["vanilla"].glideinRequest.keyValuePairs["MAX_WALLTIME"] = "180"
config.workflow["PT1Workflow"].configuration[
    "vanilla"].glideinRequest.keyValuePairs["PROJECT"] = "TG-AST100018"
config.workflow["PT1Workflow"].configuration[
    "vanilla"].glideinRequest.templateFileName = "$CTRL_ORCA_DIR/etc/lonestar/glidein_run.submit.dc3b"
config.workflow["PT1Workflow"].configuration[
    "vanilla"].glideinRequest.outputFileName = "glidein_request.condor"
config.workflow["PT1Workflow"].configuration[
    "vanilla"].announceData.script = "$CTRL_SCHED_DIR/bin/announceDataset.py"
config.workflow["PT1Workflow"].configuration["vanilla"].announceData.topic = "RawCcdAvailable"
config.workflow["PT1Workflow"].configuration[
    "vanilla"].announceData.inputdata = "/nfs/lsst3/remote/Wint-2012/pipeline/miniprod.input"
config.workflow["PT1Workflow"].configuration[
    "vanilla"].announceData.dataCompleted.script = "$CTRL_ORCA_DIR/bin/sendStatus.py"
config.workflow["PT1Workflow"].configuration["vanilla"].announceData.dataCompleted.topic = "JobOfficeTopic"
config.workflow["PT1Workflow"].configuration["vanilla"].announceData.dataCompleted.status = "data:completed"


config.workflow["PT1Workflow"].pipelineNames = ["joboffices", "PT1Pipe"]

config.workflow["PT1Workflow"].pipeline["joboffices"].definition.execute.shutdownTopic = "workflowShutdown"
config.workflow["PT1Workflow"].pipeline[
    "joboffices"].definition.execute.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
config.workflow["PT1Workflow"].pipeline[
    "joboffices"].definition.framework.script = "$DATAREL_DIR/pipeline/PT1Pipe/joboffice-ImSim-abe.sh"
config.workflow["PT1Workflow"].pipeline["joboffices"].definition.framework.type = "standard"
config.workflow["PT1Workflow"].pipeline[
    "joboffices"].definition.framework.environment = "$DATAREL_DIR/bin/runOrca/imsim-setupForOrcaUse-abe.sh"
config.workflow["PT1Workflow"].pipeline["joboffices"].runCount = 1
config.workflow["PT1Workflow"].pipeline["joboffices"].launch = True

#config.workflow["PT1Workflow"].pipeline["PT1Pipe"].definition = "@main-ImSim.paf"
#config.workflow["PT1Workflow"].pipeline["PT1Pipe"].runCount = 1
#config.workflow["PT1Workflow"].pipeline["PT1Pipe"].launch = True
