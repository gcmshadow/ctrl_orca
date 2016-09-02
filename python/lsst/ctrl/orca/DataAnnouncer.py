from __future__ import print_function
#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import os
import lsst.log as log
from lsst.ctrl.orca.EnvString import EnvString

##
# @deprecated DataAnnouncer
# this class is used to send events with available data


class DataAnnouncer:

    ## initialize
    def __init__(self, runid, prodConfig, wfConfig, logger=None):
        log.debug("DataAnnouncer: __init__")
        ## run id
        self.runid = runid
        ## production configuration
        self.prodConfig = prodConfig
        ## workflow configuration
        self.wfConfig = wfConfig

    ## send events announcing data to consumers
    def announce(self):
        log.debug("DataAnnouncer: announce")
        broker = self.prodConfig.production.eventBrokerHost

        configType = self.wfConfig.configurationType
        print("configType", configType)
        config = self.wfConfig.configuration[configType]
        if config == None:
            print("configuration for workflow was not found")
            return False

        if config.announceData != None:
            annData = config.announceData
            script = annData.script
            script = EnvString.resolve(script)
            topic = annData.topic
            inputdata = annData.inputdata
            inputdata = EnvString.resolve(inputdata)
            cmd = "%s -r %s -b %s -t %s %s" % (script, self.runid, broker, topic, inputdata)
            print(cmd)
            cmdSplit = cmd.split()
            pid = os.fork()
            if not pid:
                os.execvp(cmdSplit[0], cmdSplit)
            os.wait()[0]

            if config.announceData.dataCompleted != None:
                dataComp = config.announceData.dataCompleted
                script = dataComp.script
                script = EnvString.resolve(script)
                topic = dataComp.topic
                status = dataComp.status
                cmd = "%s %s %s %s %s" % (script, broker, topic, self.runid, status)
                print(cmd)
                cmdSplit = cmd.split()
                pid = os.fork()
                if not pid:
                    os.execvp(cmdSplit[0], cmdSplit)
                os.wait()[0]
            else:
                print("not announcing that data has been completing sent; automatic shutdown will not occur")

            return True
        else:
            return False
