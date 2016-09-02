#
# Example Orchestration Layer Config
#
config.production.shortName = "DataRelease"
config.production.eventBrokerHost = "lsst8.ncsa.uiuc.edu"
config.production.repositoryDirectory = "/home/srp/working/pipeline_new"
config.production.productionShutdownTopic = "productionShutdown"

config.database["db1"].name = "dc3bGlobal"
config.database["db1"].system.authInfo.host = "lsst10.ncsa.uiuc.edu"
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

config.workflow["workflow1"].platform.dir.defaultRoot = "/lsst/DC3root"

config.workflow["workflow1"].platform.deploy.defaultDomain = "ncsa.illinois.edu"

config.workflow["workflow1"].configurationType = "condor"
config.workflow["workflow1"].configurationClass = "lsst.ctrl.orca.CondorWorkflowConfigurator"
config.workflow["workflow1"].configuration["condor"].condorData.localScratch = "/home/srp/orca_scratch"
config.workflow["workflow1"].configuration[
    "condor"].glidein.template.inputFile = "$CTRL_ORCA_DIR/etc/condor/templates/lonestar_glidein.template"
config.workflow["workflow1"].configuration["condor"].glidein.template.keywords[
    "CONDOR_SBIN"] = "/share1/apps/teragrid/condor-7.4.4-r1/sbin"
config.workflow["workflow1"].configuration["condor"].glidein.template.keywords["CPU_COUNT"] = "6"
config.workflow["workflow1"].configuration["condor"].glidein.template.keywords["MACHINE_COUNT"] = "2"
config.workflow["workflow1"].configuration["condor"].glidein.template.keywords["MAX_WALLTIME"] = "30"
config.workflow["workflow1"].configuration["condor"].glidein.template.keywords["QUEUE"] = "normal"
config.workflow["workflow1"].configuration["condor"].glidein.template.keywords["PROJECT"] = "TG-AST100018"
config.workflow["workflow1"].configuration["condor"].glidein.template.outputFile = "lonestar_glidein.condor"


# where the script will be deposited in the localScratch

config.workflow["workflow1"].task["task1"].scriptDir = "workers"
# this is used if we're running a script that runs LOCALLY before the
# preJob condor job is submitted.
# config.workflow["workflow1"].task["task1"].preScript.script.inputFile =
#    "$CTRL_ORCA_DIR/etc/condor/templates/preScript.template"
# config.workflow["workflow1"].task["task1"].preScript.script.keywords["A"] = "A"
# config.workflow["workflow1"].task["task1"].preScript.script.keywords["B"] = "B"
# config.workflow["workflow1"].task["task1"].preScript.script.outputFile = "pre.sh"

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
config.workflow["workflow1"].task[
    "task1"].preJob.script.inputFile = "$CTRL_ORCA_DIR/etc/condor/templates/preJob.sh.template"
config.workflow["workflow1"].task["task1"].preJob.script.outputFile = "preJob.sh"

config.workflow["workflow1"].task[
    "task1"].preJob.condor.inputFile = "$CTRL_ORCA_DIR/etc/condor/templates/preJob.condor.template"
config.workflow["workflow1"].task["task1"].preJob.condor.outputFile = "S2012Pipe.pre"

#
# postJob
#
config.workflow["workflow1"].task[
    "task1"].postJob.script.inputFile = "$CTRL_ORCA_DIR/etc/condor/templates/postJob.sh.template"
config.workflow["workflow1"].task["task1"].postJob.script.keywords["DATADIR"] = "/oasis/blah"
config.workflow["workflow1"].task["task1"].postJob.script.keywords["SRP_HOME"] = "/real/path/to/lsst"
config.workflow["workflow1"].task["task1"].postJob.script.keywords["LSST_HOME"] = "/real/path/to/lsst2"
config.workflow["workflow1"].task["task1"].postJob.script.outputFile = "postJob.sh"
config.workflow["workflow1"].task[
    "task1"].postJob.condor.inputFile = "$CTRL_ORCA_DIR/etc/condor/templates/postJob.condor.template"
config.workflow["workflow1"].task["task1"].postJob.condor.keywords["EXAMPLE_A"] = "A"
config.workflow["workflow1"].task["task1"].postJob.condor.keywords["EXAMPLE_B"] = "B"
config.workflow["workflow1"].task["task1"].postJob.condor.outputFile = "S2012Pipe.post"

#
# workerJob
#
config.workflow["workflow1"].task[
    "task1"].workerJob.script.inputFile = "$CTRL_ORCA_DIR/etc/condor/templates/helloworld.sh.template"
config.workflow["workflow1"].task["task1"].workerJob.script.keywords["SRP_TEST"] = "srp_test"
config.workflow["workflow1"].task["task1"].workerJob.script.keywords["SRP_HOME"] = "/real/path/to/lsst"
config.workflow["workflow1"].task["task1"].workerJob.script.keywords["LSST_HOME"] = "/real/path/to/lsst2"
config.workflow["workflow1"].task["task1"].workerJob.script.outputFile = "helloworld.sh"
config.workflow["workflow1"].task[
    "task1"].workerJob.condor.inputFile = "$CTRL_ORCA_DIR/etc/condor/templates/workerJob.condor.template"
config.workflow["workflow1"].task["task1"].workerJob.condor.outputFile = "S2012Pipeline-template.condor"

#
# This configures a diamond condor DAG.
# The "preJob" is the head of the diamond
# The contents dagGenerator.input are used in conjunction with the workerJob
# to form the middle of the diamond and the "postJob" is the tail of the
# diamond
#
config.workflow["workflow1"].task["task1"].dagGenerator.dagName = "S2012Pipe"
config.workflow["workflow1"].task[
    "task1"].dagGenerator.script = "$CTRL_ORCA_DIR/etc/condor/scripts/generateDag.py"
config.workflow["workflow1"].task["task1"].dagGenerator.input = "$CTRL_ORCA_DIR/etc/condor/input/short.input"
config.workflow["workflow1"].task["task1"].dagGenerator.idsPerJob = 1

config.workflow["workflow1"].monitor.statusCheckInterval = 15
