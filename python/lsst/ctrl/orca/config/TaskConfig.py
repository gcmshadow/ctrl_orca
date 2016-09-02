from __future__ import absolute_import
import sys
import lsst.pex.config as pexConfig
from . import PipelineDefinitionConfig as pipe
from . import FakeTypeMap as fake

## script template


class ScriptTemplateConfig(pexConfig.Config):
    ## input file
    inputFile = pexConfig.Field("input file", str)
    ## key value pars to substitute for the template
    keywords = pexConfig.DictField("key value pairs", keytype=str, itemtype=str, default=dict())
    ## output file for results of template substitution
    outputFile = pexConfig.Field("output file", str)


## job template
class JobTemplateConfig(pexConfig.Config):
    ## job script template configuration
    script = pexConfig.ConfigField("job script", ScriptTemplateConfig)
    ## condor template configuration
    condor = pexConfig.ConfigField("template", ScriptTemplateConfig)

## script


class ScriptConfig(pexConfig.Config):
    ## job script template
    script = pexConfig.ConfigField("job script", ScriptTemplateConfig)

## DAG generation script


class DagGeneratorConfig(pexConfig.Config):
    ## DAG name
    dagName = pexConfig.Field("dag name", str)
    ## script name
    script = pexConfig.Field("script", str)
    ## input file
    input = pexConfig.Field("input", str)
    ## number of ids per job given to execute
    idsPerJob = pexConfig.Field("the number of ids that will be handled per job", int)

## task


class TaskConfig(pexConfig.Config):
    ## script directory
    scriptDir = pexConfig.Field("script directory", str)
    ## pre script  (run before any jobs)
    preScript = pexConfig.ConfigField("pre script", ScriptConfig)
    ## pre job script (run before each job)
    preJob = pexConfig.ConfigField("pre job", JobTemplateConfig)
    ## post job script (run after each job)
    postJob = pexConfig.ConfigField("post job", JobTemplateConfig)
    ## worker job configuration
    workerJob = pexConfig.ConfigField("worker job", JobTemplateConfig)
    ## DAG generator script to use to create DAG submission file
    dagGenerator = pexConfig.ConfigField("dagGenerator", DagGeneratorConfig)
