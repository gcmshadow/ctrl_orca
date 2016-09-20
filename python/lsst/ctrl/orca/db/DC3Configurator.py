from builtins import str
from builtins import object
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
import stat
import lsst.log as log
from lsst.ctrl.orca.db.MySQLConfigurator import MySQLConfigurator
from lsst.ctrl.orca.config.AuthConfig import AuthConfig

# @deprecated DC3Configurator


class DC3Configurator(object):

    def __init__(self, runid, dbConfig, prodConfig=None, wfConfig=None):
        """
        create a generic
        @param type      the category of configurator
        @param dbConfig  the database config
        """
        log.debug("DC3Configurator:__init__")
        # schema type
        self.type = "mysql"
        # run id
        self.runid = runid
        # database configuraition
        self.dbConfig = dbConfig
        # delegate method to call at end of run
        self.delegate = None
        # the per-Production database
        self.perProductionRunDatabase = None

        # the name of the platform list in the database provenance
        self.platformName = ""
        # production configuration
        self.prodConfig = prodConfig
        if self.prodConfig is None:
            self.platformName = "production"
        # workflow configuration
        self.wfConfig = wfConfig

        #
        # extract the databaseConfig.database policy to get required
        # parameters from it.

        # the database host name
        self.dbHostName = dbConfig.system.authInfo.host
        # the database port number
        self.dbPort = dbConfig.system.authInfo.port
        # global database name
        globalDbName = dbConfig.configuration["production"].globalDbName
        # data challenge version
        dcVersion = dbConfig.configuration["production"].dcVersion
        # data challenge database name
        dcDbName = dbConfig.configuration["production"].dcDbName
        # minimum percent database space required
        minPercDiskSpaceReq = dbConfig.configuration["production"].minPercDiskSpaceReq
        userRunLife = dbConfig.configuration["production"].userRunLife

        self.delegate = MySQLConfigurator(self.dbHostName, self.dbPort,
                                          globalDbName, dcVersion, dcDbName, minPercDiskSpaceReq, userRunLife)

    # setup for a new run
    def setup(self, provSetup):
        log.debug("DC3Configurator:setup")

        # TODO: use provSetup when it's implemented

        dbNames = self.setupInternal()

        # construct dbRun and dbGlobal URLs here

        self.perProductionRunDatabase = dbNames[0]

        # TODO - Provenance
        # dbBaseURL = self.getHostURL()
        # dbRun = dbBaseURL+"/"+dbNames[0]
        # dbGlobal = dbBaseURL+"/"+dbNames[1]
        # recorder = dc3.Recorder(self.runid, self.prodConfig.shortName, self.platformName,
        #                         dbRun, dbGlobal, 0, None)
        # provSetup.addProductionRecorder(recorder)

        # arglist = []
        # arglist.append("--runid=%s" % self.runid)
        # arglist.append("--dbrun=%s" % dbRun)
        # arglist.append("--dbglobal=%s" % dbGlobal)
        # arglist.append("--runoffset=%s" % recorder.getRunOffset())
        # provSetup.addWorkflowRecordCmd("PipelineProvenanceRecorder.py", arglist)
        # end TODO

    # @return a dictory containing the "host", "port", "runid" and "dbrun" (database)
    def getDBInfo(self):
        dbInfo = {}
        dbInfo["host"] = self.dbHostName
        dbInfo["port"] = self.dbPort
        dbInfo["runid"] = self.runid
        dbInfo["dbrun"] = self.perProductionRunDatabase
        return dbInfo

    # internal configuration
    def setupInternal(self):
        log.debug("DC3Configurator:setupInternal")

        self.checkConfiguration(self.dbConfig)
        dbNames = self.prepareForNewRun(self.runid)
        return dbNames

    # validate configuration
    def checkConfiguration(self, val):
        log.debug("DC3Configurator:checkConfiguration")
        # TODO: use val when implemented
        self.checkConfigurationInternal()

    # validate configuration
    def checkConfigurationInternal(self):
        log.debug("DC3Configurator:checkConfigurationInternal")
        #
        # first, check that the $HOME/.lsst directory is protected
        #
        dbPolicyDir = os.path.join(os.environ["HOME"], ".lsst")
        self.checkUserOnlyPermissions(dbPolicyDir)

        #
        # next, check that the $HOME/.lsst/db-auth.config file is protected
        #
        dbConfigCredentialsFile = os.path.join(os.environ["HOME"], ".lsst/db-auth.py")
        self.checkUserOnlyPermissions(dbConfigCredentialsFile)

        #
        # now, look up and initialize the authorization information for host and port
        #
        self.initAuthInfo(self.dbConfig)

    # return a URL to the database host, including port number
    def getHostURL(self):
        schema = self.type.lower()
        retVal = schema+"://"+self.dbHost+":"+str(self.dbPort)
        return retVal

    # get database user
    def getUser(self):
        return self.dbUser

    # check to see that the file is accessible only to the user
    def checkUserOnlyPermissions(self, checkFile):
        mode = os.stat(checkFile)[stat.ST_MODE]

        errorText = ("File permissions on ", checkFile,
                     " should not be readable, writable, or executable  by 'group' or 'other'.",
                     " Use chmod to fix this. (chmod 700 ~/.lsst)")

        if (mode & getattr(stat, "S_IRWXG")) != 0:
            raise RuntimeError(errorText)
        if (mode & getattr(stat, "S_IRWXO")) != 0:
            raise RuntimeError(errorText)

    # prepare the database for a new run
    def prepareForNewRun(self, runName, runType='u'):
        return self.delegate.prepareForNewRun(runName, self.dbUser, self.dbPassword, runType)

    # call the delegate
    def runFinished(self, dbName):
        self.delegate(dbName)

    #
    # initAuthInfo - given a Config object with specifies "database.host" and
    # "database.port", match it against the credential Config
    # file $HOME/.lsst/db-auth.paf
    #
    # The credential Config has the following format:
    #
    # config.database.authInfo.names = ["cred1", "cred2]
    #
    # config.database.authInfo["cred1"].host = "lsst-db.ncsa.illinois.edu"
    # config.database.authInfo["cred1"].user = "moose"
    # config.database.authInfo["cred1"].password = "squirrel"
    # config.database.authInfo["cred1"].port = 3306
    #
    # config.database.authInfo["cred2"].host = "lsst-db.ncsa.illinois.edu"
    # config.database.authInfo["cred2"].user = "boris"
    # config.database.authInfo["cred2"].password = "natasha"
    # config.database.authInfo["cred2"].port = 3306
    #
    # Terms "database.host" and "database.port" must be specified,
    # and will match against the first "database.authInfo.host" and
    # "database.authInfo.port"  in the credentials config.
    #
    # If there is no match, an exception is thrown.
    #
    def initAuthInfo(self, dbConfig):
        host = dbConfig.system.authInfo.host
        if host is None:
            raise RuntimeError("database host must be specified in config")
        port = dbConfig.system.authInfo.port
        if port is None:
            raise RuntimeError("database port must be specified in config")
        dbAuthFile = os.path.join(os.environ["HOME"], ".lsst/db-auth.py")

        authConfig = AuthConfig()
        authConfig.load(dbAuthFile)

        for authName in authConfig.database.authInfo:
            auth = authConfig.database.authInfo[authName]
            if (auth.host == host) and (auth.port == port):
                log.debug("using host %s at port %d" % (host, port))
                # database host name
                self.dbHost = auth.host
                # database server port number
                self.dbPort = auth.port
                # database user name
                self.dbUser = auth.user
                # database authentication
                self.dbPassword = auth.password
                return
        raise RuntimeError("couldn't find any matching authorization for host %s and port %d " % (host, port))
