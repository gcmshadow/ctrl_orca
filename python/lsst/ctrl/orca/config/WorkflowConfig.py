import sys
import lsst.pex.config as pexConfig
import PipelineConfig as pipe
import CondorWorkflowConfig as condor
import VanillaCondorWorkflowConfig as van
import GenericWorkflowConfig as gen
import FakeTypeMap as fake
import DatabaseConfig as data
import PlatformConfig as plat
import MonitorConfig as mon
import TaskConfig as task

typemap = {"generic": gen.GenericWorkflowConfig,
           "vanilla": van.VanillaCondorWorkflowConfig, "condor": condor.CondorWorkflowConfig}

##
# definition of a workflow


class WorkflowConfig(pexConfig.Config):
    ## name of this workflow
    shortName = pexConfig.Field("name of this workflow", str)
    ## platform configuration file
    platform = pexConfig.ConfigField("platform configuration file", plat.PlatformConfig)
    ## topic used for shutdown events
    shutdownTopic = pexConfig.Field("topic used for shutdown events", str)

    ## plugin type
    configurationType = pexConfig.Field("plugin type", str)
    ## plugin class name
    configurationClass = pexConfig.Field("orca plugin class", str)
    ## configuration
    configuration = pexConfig.ConfigChoiceField("configuration", typemap)

    # this usually isn't used, but is here because the design calls for this
    # possibility.
    ## database name
    database = pexConfig.ConfigChoiceField("database", fake.FakeTypeMap(data.DatabaseConfig))

    #pipeline = pexConfig.ConfigChoiceField("pipeline",fake.FakeTypeMap(pipe.PipelineConfig))
    ## task
    task = pexConfig.ConfigChoiceField("task", fake.FakeTypeMap(task.TaskConfig))
    ## monitor configuration
    monitor = pexConfig.ConfigField("monitor configuration", mon.MonitorConfig)
