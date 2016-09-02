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

import lsst.ctrl.events as events
from lsst.daf.base import PropertySet
import sys

# usage:  python bin/shutlog.py runid
if __name__ == "__main__":
    host = "lsst8.ncsa.uiuc.edu"

    topic = events.LogEvent.LOGGING_TOPIC

    runid = sys.argv[1]

    eventSystem = events.EventSystem.getDefaultEventSystem()
    eventSystem.createTransmitter(host, topic)

    props = PropertySet()
    props.set("LOGGER", "orca.control")
    props.set("STATUS", "eol")

    e = events.Event(runid, props)

    eventSystem.publishEvent(topic, e)
