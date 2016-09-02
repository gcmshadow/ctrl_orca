import lsst.pex.config as pexConfig

## execution configuration


class ExecuteConfig(pexConfig.Config):
    ## shutdown topic
    shutdownTopic = pexConfig.Field("shutdown topic", str)
    ## host name of event broker
    eventBrokerHost = pexConfig.Field("event broker host", str)

## framework configuration


class FrameworkConfig(pexConfig.Config):
    ## script to invoke for program execution
    script = pexConfig.Field("program to execute", str)
    ## type name
    type = pexConfig.Field("type", str)
    ## environment to set up for program execution
    environment = pexConfig.Field("environment to set up", str)

## pipeline definition configuration


class PipelineDefinitionConfig(pexConfig.Config):
    ## execute configuration
    execute = pexConfig.ConfigField("execute", ExecuteConfig)
    ## framework configuration
    framework = pexConfig.ConfigField("execute", FrameworkConfig)
