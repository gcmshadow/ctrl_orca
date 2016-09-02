from __future__ import absolute_import
import sys
import lsst.pex.config as pexConfig
from . import PipelineConfig as pipe
from . import CondorWorkflowConfig as condor
from . import VanillaCondorWorkflowConfig as van
from . import GenericWorkflowConfig as gen
from . import FakeTypeMap as fake
from . import DatabaseConfig as data
from . import PlatformConfig as plat
from . import MonitorConfig as mon
from . import TaskConfig as task

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
