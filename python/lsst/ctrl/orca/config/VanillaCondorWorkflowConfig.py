import lsst.pex.config as pexConfig
import PipelineConfig as pipe
import FakeTypeMap as fake

## deploy data configuration


class DeployDataConfig(pexConfig.Config):
    ## data repository
    dataRepository = pexConfig.Field("data repository", str)
    ## collection name
    collection = pexConfig.Field("collection name", str)
    ## script name
    script = pexConfig.Field("script name", str)

## configuration to perform transfer of information to a remote site


class CondorDataConfig(pexConfig.Config):
    ## local scratch area
    localScratch = pexConfig.Field("temp data area", str)
    ## login node to preform SSH commands to
    loginNode = pexConfig.Field("node to use to perform SSH commands", str)
    ## ftp node to use to transfer files
    ftpNode = pexConfig.Field("node to use to transfer files", str)
    ## protocol used to transfer files
    transferProtocol = pexConfig.Field("method of file transfer", str)

## HTCondor glide-in request configuration


class GlideinRequestConfig(pexConfig.Config):
    #keyNames = pexConfig.ListField("keys",str)
    ## key value pairs for glide-in template
    keyValuePairs = pexConfig.DictField("key value pairs", keytype=str, itemtype=str, default=dict())
    ## name of template file
    templateFileName = pexConfig.Field("name of template to fill in", str)
    ## output file name
    outputFileName = pexConfig.Field("name of file to write", str)

## script to run to announce that data has completed


class DataCompletedConfig(pexConfig.Config):
    ## shell script for data completed
    script = pexConfig.Field("shell script", str)
    ## ctrl_events topic used to announce data
    topic = pexConfig.Field("event topic to announce on", str)
    ## status to announce
    status = pexConfig.Field("status to announce", str)

## configuration information used to announce data to the workflow


class AnnounceDataConfig(pexConfig.Config):
    ## data announcement script to execute
    script = pexConfig.Field("announce script", str)
    ## topic to send announcements on
    topic = pexConfig.Field("event topic to broadcast on", str)
    ## input data file used to announce
    inputdata = pexConfig.Field("input data file", str)
    ## script to execute when data announcement is finished
    dataCompleted = pexConfig.ConfigField("data completed script", DataCompletedConfig)

## htcondor configuration for vanilla universe, uses data announcements for the job office


class VanillaCondorWorkflowConfig(pexConfig.Config):
    ## depoloy data configuration
    deployData = pexConfig.ConfigField("deploy data", DeployDataConfig)
    ## condor data configuration
    condorData = pexConfig.ConfigField("condor data", CondorDataConfig)
    ## glidein request configuration
    glideinRequest = pexConfig.ConfigField("glidein info", GlideinRequestConfig)
    ## data announcement configuration
    announceData = pexConfig.ConfigField("data announcement", AnnounceDataConfig)
