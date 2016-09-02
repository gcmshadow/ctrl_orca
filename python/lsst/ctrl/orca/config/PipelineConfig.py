from __future__ import absolute_import
import sys
import lsst.pex.config as pexConfig
from . import PipelineDefinitionConfig as pipe
from . import FakeTypeMap as fake
from lsst.ctrl.orca.config.DirectoryConfig import DirectoryConfig

## information about app stages


class AppStageConfig(pexConfig.Config):
    ## class to invoke for parallel execution
    parallelClass = pexConfig.Field("class", str)
    ## event topic
    eventTopic = pexConfig.Field("topic name", str)
    ## stage name
    stageConfig = pexConfig.Field("config name", str)

## execution information


class ExecuteConfig(pexConfig.Config):
    ## directory configuration
    dir = pexConfig.ConfigField("directories", DirectoryConfig)
    ## environment used to execute programs
    environment = pexConfig.Field("environment", str)
    ## task name
    task = pexConfig.Field("task", str)

## framework


class FrameworkConfig(pexConfig.Config):
    ## script to invoke to execute application
    script = pexConfig.Field("program to execute", str)
    ## type
    type = pexConfig.Field("type", str)
    ## environment used to execute programs
    environment = pexConfig.Field("environment to set up", str)

## deployment information


class DeployConfig(pexConfig.Config):
    ## number of processes per node
    processesOnNode = pexConfig.ListField("processes", str)

## definition of pipeline


class PipelineDefinitionConfig(pexConfig.Config):
    ## execution configuration
    execute = pexConfig.ConfigField("execute", ExecuteConfig)
    ## framework configuration
    framework = pexConfig.ConfigField("execute", FrameworkConfig)


## pipeline configuration
class PipelineConfig(pexConfig.Config):
    ## pipeline definition configuration
    definition = pexConfig.ConfigField("definition", PipelineDefinitionConfig)
    ## deployment configuration
    deploy = pexConfig.ConfigField("deployment info", DeployConfig)
    ## job number
    runCount = pexConfig.Field("job definition", int)
