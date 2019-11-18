#
# LSST Data Management System
# Copyright 2008-2017 LSST Corporation.
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

##
# DatabaseConfigurator
#
# Note: this is currently unused, but being kept in place for when
# orchestration records provenance before runs
#


class DatabaseConfigurator:
    """Configures a database for use.

    Parameters
    ----------
    runid : `str`
        run identifier
    databaseConfig : Config
        database configuration object
    prodConfig : Config
        production configuration object

    Note
    ----
    This is currently unused, but being kept in place for when
    orchestration records provenance for runs
    """

    def __init__(self, runid, databaseConfig, prodConfig):
        self.runid = runid
        self.databaseConfig = databaseConfig
        self.prodConfig = prodConfig
        return

    def setup(self, provSetup):
        """Setup for a new run, and record provenance information

        Parameters
        ----------
        provSetup : Config
            provenance configuration object
        """
        return
