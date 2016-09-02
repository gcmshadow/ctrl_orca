#
# Example Orchestration Layer Config
#
config.production.shortName = "DataRelease"
config.production.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
config.production.repositoryDirectory = "/home/srp/working/pipeline_new"
config.production.productionShutdownTopic = "productionShutdown"

config.database["db1"].name = "dc3bGlobal"
config.database["db1"].system.authInfo.host = "fester.ncsa.uiuc.edu"
config.database["db1"].system.authInfo.port = 3306
config.database["db1"].system.runCleanup.daysFirstNotice = 7
config.database["db1"].system.runCleanup.daysFinalNotice = 1

config.database["db1"].configurationClass = "lsst.ctrl.orca.db.DC3Configurator"
config.database["db1"].configuration["production"].globalDbName = "GlobalDB"
config.database["db1"].configuration["production"].dcVersion = "S12_lsstsim"
config.database["db1"].configuration["production"].dcDbName = "DC3b_DB"
config.database["db1"].configuration["production"].minPercDiskSpaceReq = 10
config.database["db1"].configuration["production"].userRunLife = 2
config.database["db1"].logger.launch = True

config.workflow["association"].platform.dir.defaultRoot = "/lsst/DC3root"
config.workflow["association"].platform.dir.workDir = "work"
config.workflow["association"].platform.dir.inputDir = "input"
config.workflow["association"].platform.dir.outputDir = "output"
config.workflow["association"].platform.dir.updateDir = "update"
config.workflow["association"].platform.dir.scratchDir = "scratch"

config.workflow["association"].platform.deploy.defaultDomain = "ncsa.illinois.edu"

config.workflow["association"].shutdownTopic = "workflowShutdown"

config.workflow["association"].configurationType = "condor"
config.workflow["association"].configurationClass = "lsst.ctrl.orca.CondorWorkflowConfigurator"
config.workflow["association"].configuration["condor"].condorData.localScratch = "/home/srp/orca_scratch"


# where the script will be deposited in the localScratch

config.workflow["association"].task["isr"].scriptDir = "workers"
# this is used if we're running a script that runs LOCALLY before the
# preJob condor job is submitted.
# commented out for now
# config.workflow["association"].task["isr"].preScript.template =
#        "$CTRL_ORCA_DIR/etc/condor/templates/preScript.template"
# config.workflow["association"].task["isr"].preScript.outputFile = "pre.sh"

#
# There are two stages of templating.
# [preJob|postJob|workerJob].script.template is used to create
# [preJob|postJob|workerJob].script.output
# This "output" file is the executable script condor will run.
#
# That file is substituted in the [preJob|postJob|workerJob].template
# to create the [preJob|postJob|workerJob].outputFile, which is the
# actual Condor file used in the Condor DAG.


#
# preJob
#
config.workflow["association"].task[
    "isr"].preJob.script.template = "$CTRL_ORCA_DIR/etc/condor/templates/preJob.sh.template"
config.workflow["association"].task["isr"].preJob.script.outputFile = "preJob.sh"
config.workflow["association"].task[
    "isr"].preJob.template = "$CTRL_ORCA_DIR/etc/condor/templates/preJob.condor.template"
config.workflow["association"].task["isr"].preJob.outputFile = "W2012Pipe.pre"

#
# postJob
#
config.workflow["association"].task[
    "isr"].postJob.script.template = "$CTRL_ORCA_DIR/etc/condor/templates/postJob.sh.template"
config.workflow["association"].task["isr"].postJob.script.outputFile = "postJob.sh"
config.workflow["association"].task[
    "isr"].postJob.template = "$CTRL_ORCA_DIR/etc/condor/templates/postJob.condor.template"
config.workflow["association"].task["isr"].postJob.outputFile = "W2012Pipe.post"

#
# workerJob
#
config.workflow["association"].task[
    "isr"].workerJob.script.template = "$CTRL_ORCA_DIR/etc/condor/templates/helloworld.sh.template"
config.workflow["association"].task["isr"].workerJob.script.outputFile = "helloworld.sh"
config.workflow["association"].task[
    "isr"].workerJob.template = "$CTRL_ORCA_DIR/etc/condor/templates/localworkerJob.condor.template"
config.workflow["association"].task["isr"].workerJob.outputFile = "W2012Pipeline-template.condor"

#
# This configures a diamond condor DAG.
# The "preJob" is the head of the diamond
# The contents dagGenerator.input are used in conjunction with the workerJob
# to form the middle of the diamond and the "postJob" is the tail of the
# diamond
#
config.workflow["association"].task["isr"].dagGenerator.dagName = "W2012Pipe"
config.workflow["association"].task[
    "isr"].dagGenerator.script = "$CTRL_ORCA_DIR/etc/condor/scripts/generateDag.py"
config.workflow["association"].task["isr"].dagGenerator.input = "$CTRL_ORCA_DIR/etc/condor/input/short.input"
