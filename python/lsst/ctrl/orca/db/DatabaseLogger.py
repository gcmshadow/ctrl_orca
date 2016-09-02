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


from lsst.cat.MySQLBase import MySQLBase
import MySQLdb
import os
import sys
import subprocess

##
# @deprecated insert logging records into the database.  Note that this is
# completely depedent on the CAT schema for this database, depends on writing
# to a file, and needs to be completely replaced.


class DatabaseLogger(MySQLBase):

    ## initialize
    def __init__(self, dbHostName, portNumber):
        MySQLBase.__init__(self, dbHostName, portNumber)

        ## table keywords
        self.keywords = ['HOSTID', 'RUNID', 'LOG', 'workerid', 'stagename', 'SLICEID', 'STAGEID', 'LOOPNUM', 'STATUS', 'LEVEL', 'DATE',
                         'TIMESTAMP', 'node', 'custom', 'timereceived', 'visitid', 'COMMENT', 'PIPELINE', 'TYPE', 'EVENTTIME', 'PUBTIME', 'usertime', 'systemtime']

        ## set of table keywords
        self.keywordSet = set(self.keywords)
        ## @deprecated high water mark
        self.highwater = 10

    ## insert the messages into the database, using a file
    def insertRecords(self, dbTable, msgs, filename):
        cnt = len(msgs)

        file = open(filename, "w")
        for i in range(0, cnt):
            #event = msgs.pop(0)
            #ins = self.createInsertString(dbTable, event.getPropertySet())
            propSet = msgs.pop(0)
            ins = self.createInsertString(dbTable, propSet)
            file.write(ins)
        file.close()
        cmd = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n' SET timereceived=NOW();" % (filename,
                                                                                                                                                                    dbTable)
        self.execCommand0(cmd)

    ## insert a record into the table "dbTable" , given the property set "ps"
    def insertRecord(self, dbTable, ps):
        ins = self.createInsertString(dbTable, ps)
        self.execCommand0(ins)

    ## create an insert string based on the keywords for the table, filling
    # in defaults as needed.
    def createInsertString(self, dbTable, ps):
        hostId = ps.getString("HOSTID")
        runId = ps.getString("RUNID")
        sliceId = ps.getInt("SLICEID")
        level = ps.getInt("LEVEL")
        log = ps.getString("LOG")
        date = ps.getString("DATE")
        ts = ps.get("TIMESTAMP")
        eventtime = ps.get("EVENTTIME")
        pubtime = ps.get("PUBTIME")
        eventtype = ps.getString("TYPE")

        if ps.exists("node"):
            node = ps.getInt("node")
        else:
            node = -1

        timestamp = ts.nsecs()

        commentList = ps.getString("COMMENT")
        comment = ""

        if ps.valueCount("COMMENT") == 1:
            comment = commentList
        else:
            for i in commentList:
                if comment == "":
                    comment = i
                else:
                    comment = comment+";"+i

        if (ps.exists("TOPIC")):
            ps.remove("TOPIC")

        if ps.exists("STATUS"):
            status = ps.getString("STATUS")
        else:
            status = "NULL"

        if ps.exists("PIPELINE"):
            pipeline = ps.getString("PIPELINE")
        else:
            pipeline = "NULL"

        if ps.exists("STAGEID"):
            stageid = ps.getInt("STAGEID")
        else:
            stageid = "-1"

        if ps.exists("LOOPNUM"):
            loopnum = ps.getInt("LOOPNUM")
        else:
            loopnum = "-1"

        if ps.exists("workerid"):
            workerid = ps.getString("workerid")
        else:
            workerid = "NULL"

        if ps.exists("usertime"):
            usertime = ps.getDouble("usertime")
        else:
            usertime = 0

        if ps.exists("systemtime"):
            systemtime = ps.getDouble("systemtime")
        else:
            systemtime = 0

        if ps.exists("stagename"):
            stagename = ps.getString("stagename")
        else:
            stagename = "unknown"

        visitid = "-1"

        names = ps.names()
        namesSet = set(names)

        diff = namesSet.difference(self.keywordSet)

        custom = ""
        for name in diff:
            if custom == "":
                custom = "%s : %s;" % (name, ps.get(name))
            else:
                custom = custom + "%s : %s;" % (name, ps.get(name))
        if custom == "":
            custom = "NULL"

        #custom = MySQLdb.escape_string(custom[0:256])
        #comment = MySQLdb.escape_string(comment[0:256])
        custom = MySQLdb.escape_string(custom[0:4096])
        comment = MySQLdb.escape_string(comment)

        #custom = custom[0:256]
        #comment = comment[0:256]

        timereceived = "0000-00-00 00:00:00"

        datalist = []
        datalist.append(0)
        datalist.append(hostId)
        datalist.append(runId)
        datalist.append(log)
        datalist.append(workerid)
        datalist.append(stagename)
        datalist.append(sliceId)
        datalist.append(stageid)
        datalist.append(loopnum)
        datalist.append(status)
        datalist.append(level)
        datalist.append(date)
        datalist.append(timestamp)
        datalist.append(node)
        datalist.append(custom)
        datalist.append(timereceived)
        datalist.append(visitid)
        datalist.append(comment)
        datalist.append(pipeline)
        datalist.append(eventtype)
        datalist.append(eventtime)
        datalist.append(pubtime)
        datalist.append(usertime)
        datalist.append(systemtime)

        cmd = ""
        for i in datalist:
            if cmd == "":
                cmd = str(i)
            else:
                cmd = cmd + "\t" + str(i)
        cmd = cmd + "\n"

        return cmd
