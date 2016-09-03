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


import eups
import hashlib
import os
import re
import time
import unittest

import lsst.ctrl.orca.provenance as orcaProv
import lsst.pex.policy as pexPolicy
from lsst.daf.persistence import DbStorage, LogicalLocation
from lsst.daf.base import DateTime


class ProvenanceTestCase(unittest.TestCase):
    """A test case for Provenance."""

    def setUp(self):
        self.user = "test"
        self.runId = "test_" + repr(time.time())
        self.dbLoc = "mysql://lsst-db.ncsa.illinois.edu:3306/provenance"
        self.globalDbLoc = "mysql://lsst-db.ncsa.illinois.edu:3306/provglobal"
        db = DbStorage()
        globalDb = DbStorage()
        db.setPersistLocation(LogicalLocation(self.dbLoc))
        globalDb.setPersistLocation(LogicalLocation(self.globalDbLoc))
        for table in ("prv_SoftwarePackage", "prv_cnf_SoftwarePackage",
                      "prv_PolicyFile", "prv_PolicyKey", "prv_cnf_PolicyKey"):
            db.truncateTable(table)
            globalDb.truncateTable(table)
        globalDb.truncateTable("prv_Run")

    def testConstruct(self):
        ps = orcaProv.Provenance(self.user, self.runId, self.dbLoc,
                                 self.globalDbLoc)
        self.assert_(ps is not None)
        self.assert_(ps.db is not None)

    def testEnvironment(self):
        ps = orcaProv.Provenance(self.user, self.runId, self.dbLoc,
                                 self.globalDbLoc)
        ps.recordEnvironment()

        db = DbStorage()
        db.setRetrieveLocation(LogicalLocation(self.dbLoc))

        db.startTransaction()

        db.setTableListForQuery(
            ("prv_SoftwarePackage", "prv_cnf_SoftwarePackage"))
        db.outColumn("prv_SoftwarePackage.packageId")
        db.outColumn("packageName")
        db.outColumn("version")
        db.outColumn("directory")
        db.orderBy("packageName")
        db.setQueryWhere("prv_SoftwarePackage.packageId = prv_cnf_SoftwarePackage.packageId")
        db.query()

        i = 1
        pkgs = eups.Eups().listProducts(setup=True)
        while next(db):
            self.assert_(not db.columnIsNull(0))
            self.assert_(not db.columnIsNull(1))
            self.assert_(not db.columnIsNull(2))
            self.assert_(not db.columnIsNull(3))
            self.assertEqual(db.getColumnByPosInt(0), (1 << 16) + i)
            self.assertEqual(db.getColumnByPosString(1), pkgs[i - 1][0])
            self.assertEqual(db.getColumnByPosString(2), pkgs[i - 1][1])
            self.assertEqual(db.getColumnByPosString(3), pkgs[i - 1][3])
            i += 1
        db.finishQuery()

        db.endTransaction()

    def testPolicies(self):
        ps = orcaProv.Provenance(self.user, self.runId, self.dbLoc,
                                 self.globalDbLoc)
        paths = ("tests/policy/dc2pipe.paf",
                 "tests/policy/imageSubtractionDetection.paf")
        for p in paths:
            ps.recordPolicy(p)

        db = DbStorage()
        db.setRetrieveLocation(LogicalLocation(self.dbLoc))

        for p in paths:
            md5 = hashlib.md5()
            f = open(p, 'r')
            for line in f:
                md5.update(line)
            f.close()
            hash = md5.hexdigest()
            mod = DateTime(os.stat(p)[8] * 1000000000, DateTime.UTC)

            pol = pexPolicy.Policy(p)
            names = pol.paramNames()

            db.startTransaction()

            db.setTableListForQuery(
                ("prv_PolicyFile", "prv_PolicyKey", "prv_cnf_PolicyKey"))
            db.outColumn("hashValue")
            db.outColumn("modifiedDate")
            db.outColumn("keyName")
            db.outColumn("keyType")
            db.outColumn("value")
            db.setQueryWhere(
                """pathname = '%s'
                AND prv_PolicyFile.policyFileId = prv_PolicyKey.policyFileId
                AND prv_PolicyKey.policyKeyId = prv_cnf_PolicyKey.policyKeyId
                """ % (p))
            db.query()

            while next(db):
                self.assert_(not db.columnIsNull(0))
                self.assert_(not db.columnIsNull(1))
                self.assert_(not db.columnIsNull(2))
                self.assert_(not db.columnIsNull(3))
                self.assert_(not db.columnIsNull(4))
                self.assertEqual(db.getColumnByPosString(0), hash)
                self.assertEqual(db.getColumnByPosInt64(1), mod.nsecs())
                key = db.getColumnByPosString(2)
                self.assert_(pol.exists(key))
                self.assert_(key in names)
                names.remove(key)
                self.assertEqual(db.getColumnByPosString(3),
                                 pol.getTypeName(key))
                correct = pol.str(key)
                correct = re.sub(r'\0', r'', correct)
                self.assertEqual(db.getColumnByPosString(4), correct)

            db.finishQuery()

            db.endTransaction()

            self.assertEqual(len(names), 0)

if __name__ == '__main__':
    unittest.main()
