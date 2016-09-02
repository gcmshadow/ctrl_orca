from __future__ import absolute_import
import sys
import lsst.pex.config as pexConfig
from . import FakeTypeMap as fake

## workflow monitor configuration


class MonitorConfig(pexConfig.Config):
    ## number of seconds to wait between status checks
    statusCheckInterval = pexConfig.Field("interval to wait for condor_q status checks", int, default=5)
