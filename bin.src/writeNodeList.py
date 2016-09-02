#!/usr/bin/env python

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

from __future__ import print_function
import os
import os.path
import sys
import lsst.pex.policy as pol


# two args:
# the first one is the working directory, the second the policy
# filename
work = sys.argv[1]
name = sys.argv[2]
filename = os.path.join(work, name)

# read the policy in and get the count of the number of entries
p = pol.Policy(filename)
num = len(p.paramNames())

# write the "nodelist.scr" file out, preserving the order
nodelist = open(os.path.join(work, "nodelist.scr"), "w")

for x in range(0, num):
    val = p.get("node%d" % x)
    print(val, file=nodelist)

nodelist.close()
