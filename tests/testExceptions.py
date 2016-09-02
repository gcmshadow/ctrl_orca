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

"""
Tests of orca exceptions
"""
import unittest

from lsst.ctrl.orca.exceptions import MultiIssueConfigurationError


class MultiIssueTestCase(unittest.TestCase):
    unspecified = "Unspecified configuration problems encountered"
    generic = "Multiple configuration problems encountered"

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testNoProb(self):
        err = MultiIssueConfigurationError()
        self.assert_(not err.hasProblems(), "no problems added yet")
        self.assertEquals(str(err), self.unspecified)

        probs = err.getProblems()
        self.assertEquals(len(probs), 0)

    def testOneProb(self):
        msg = "first problem"
        err = MultiIssueConfigurationError(problem=msg)
        self.assertEquals(str(err), msg)

        probs = err.getProblems()
        self.assertEquals(len(probs), 1)
        self.assertEquals(probs[0], msg)

    def testTwoProbs(self):
        msg = "first problem"
        err = MultiIssueConfigurationError(problem=msg)
        self.assertEquals(str(err), msg)
        msg2 = "second problem"
        err.addProblem(msg2)
        self.assertEquals(str(err), self.generic)

        probs = err.getProblems()
        self.assertEquals(len(probs), 2)
        self.assertEquals(probs[0], msg)
        self.assertEquals(probs[1], msg2)

    def testGenericMsg(self):
        msg = "problems encountered while checking configuration"
        err = MultiIssueConfigurationError(msg)
        self.assertEquals(str(err), self.unspecified)

        msg1 = "first problem"
        err.addProblem(msg1)
        self.assertEquals(str(err), msg1)

        err.addProblem("2nd problem")
        self.assertEquals(str(err), msg)


__all__ = "MultiIssueTestCase".split()

if __name__ == "__main__":
    unittest.main()
