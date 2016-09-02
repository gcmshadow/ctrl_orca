import lsst.pex.config as pexConfig

# directory configuration


class DirectoryConfig(pexConfig.Config):
    # path to default root
    defaultRoot = pexConfig.Field("default root", str)
    # pattern for run directories
    runDirPattern = pexConfig.Field("pattern", str)
    # working directory
    workDir = pexConfig.Field("work directory", str)
    # input directory
    inputDir = pexConfig.Field("input directory", str)
    # output directory
    outputDir = pexConfig.Field("output directory", str)
    # update directory
    updateDir = pexConfig.Field("update directory", str)
    # scratch directory
    scratchDir = pexConfig.Field("scratch directory", str)
