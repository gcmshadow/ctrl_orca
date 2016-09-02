from __future__ import absolute_import
import sys
import lsst.pex.config as pexConfig
from . import PipelineConfig as pipe
from . import FakeTypeMap as fake

## @deprecated deploy data configuration


class DeployDataConfig(pexConfig.Config):
    ## data repository directory
    dataRepository = pexConfig.Field("data repository", str)
    ## collection name
    collection = pexConfig.Field("collection name", str)
    ## deployment script name
    script = pexConfig.Field("script name", str)

## @deprecated data completed configuration


class DataCompletedConfig(pexConfig.Config):
    ## script name to execute on completion
    script = pexConfig.Field("completed script", str)
    ## event topic
    topic = pexConfig.Field("event topic to broadcast on", str)
    ## status message when data is completed
    status = pexConfig.Field("status message", str)

## @deprecated data announcement configuration


class AnnounceDataConfig(pexConfig.Config):
    ## script name to execute to announce data
    script = pexConfig.Field("announce script", str)
    ## event topic
    topic = pexConfig.Field("event topic to broadcast on", str)
    ## data input file
    inputdata = pexConfig.Field("input data file", str)
    ## data completed configuration
    dataCompleted = pexConfig.ConfigField("data completion", DataCompletedConfig)

## @deprecated generic workflow configuration


class GenericWorkflowConfig(pexConfig.Config):
    ## data deployment configuration
    deployData = pexConfig.ConfigField("deploy data", DeployDataConfig)
    ## data announcement configuration
    announceData = pexConfig.ConfigField("data announcement", AnnounceDataConfig)
