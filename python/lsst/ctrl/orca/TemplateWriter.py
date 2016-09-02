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

import socket

##
# This class takes template files and substitutes the values for the given
# keys, writing a new file generated from the template.
#


class TemplateWriter:

    # initializer
    def __init__(self):
        # local values that are always set
        self.orcaValues = dict()
        self.orcaValues["ORCA_LOCAL_HOSTNAME"] = socket.gethostname()
        return

    ##
    # given a input template, take the keys from the key/values in the config
    # object and substitute the values, and write those to the output file.
    #
    def rewrite(self, input, output, pairs):
        fpInput = open(input, 'r')
        fpOutput = open(output, 'w')

        while True:
            line = fpInput.readline()
            if len(line) == 0:
                break

            # replace the "standard" orca names first
            for name in self.orcaValues:
                key = "$"+name
                val = str(self.orcaValues[name])
                line = line.replace(key, val)

            # replace the user defined names
            for name in pairs:
                key = "$"+name
                val = str(pairs[name])
                line = line.replace(key, val)
            fpOutput.write(line)
        fpInput.close()
        fpOutput.close()
