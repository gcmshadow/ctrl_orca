from __future__ import print_function
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

import sets
import os.path
import lsst.pex.policy as pol

##
# @brief policy file utilities
# @deprecated policy file utilities


class PolicyUtils(object):

    ##
    # @brief given a policy, recursively add all child policies to a policy set
    #

    def getAllFilenames(repos, policy, policySet):
        names = policy.names(True)
        for name in names:
            if policy.isArray(name):
                if policy.isPolicy(name):
                    # name is a policy array
                    policyArray = policy.getPolicyArray(name)
                    for p in policyArray:
                        PolicyUtils.getAllFilenames(repos, p, policySet)
                else:
                    # name is an array, but not a policy array
                    array = policy.getArray(name)
                    for val in array:
                        if type(val) is pol.PolicyFile:
                            filename = val.getPath()
                            filename = os.path.join(repos, filename)
                            if (filename in policySet) is False:
                                policySet.add(filename)
                            newPolicy = pol.Policy.createPolicy(filename, False)
                            PolicyUtils.getAllFilenames(repos, newPolicy, policySet)
            else:
                if policy.isPolicy(name):
                    # name is not an array, but is a Policy
                    p = policy.getPolicy(name)
                    PolicyUtils.getAllFilenames(repos, p, policySet)
                else:
                    if policy.isFile(name):
                        # name is a File value
                        filename = policy.getFile(name).getPath()
                        filename = os.path.join(repos, filename)
                        if (filename in policySet) is False:
                            policySet.add(filename)
                        newPolicy = pol.Policy.createPolicy(filename, False)
                        PolicyUtils.getAllFilenames(repos, newPolicy, policySet)
                    # else name is a regular value

    # return all file names from a policy set
    getAllFilenames = staticmethod(getAllFilenames)

if __name__ == "__main__":
    pset = sets.Set()

    myPolicy = pol.Policy.createPolicy("main.paf", False)
    PolicyUtils.getAllFilenames("/home/srp/temp_merge/datarel/trunk/pipeline", myPolicy, pset)

    for i in pset:
        print(i)
