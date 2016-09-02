#! /usr/bin/env python

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


import lsst.pex.exceptions as pexExcept

import lsst.daf.base as dafBase
from lsst.daf.base import *

import os

##
# @brief   a determination of the various directory roots that can be
#              used by a pipeline.
#
# This class takes a "dir" config and a run identifier and converts the
# values into a set of directory paths that a pipeline is allowed to use.
# A typical use might be:
# @code
#   lookup = lsst.daf.base.PropertySet()
#   dirs = Directories(dirConfig, "rlp0220")
#   lookup = dirs.getDirs()
# @endcode
#
# The schema of the input config is expected to have the following keys:
# @verbatim
#   defaultRoot        the default root directory all files read or
#                         written by pipelines deployed on this platform.
#                         This must be an absolute directory.  This can be
#                         overriden by any of the "named role" directories
#                         below.
#   runDirPattern:     the pattern to use for setting the root directory
#                         for a production run.  The result is a directory
#                         relative to the default root directory (set via
#                         defaultRoot).  The format is a python formatting
#                         string using the following dictionary keys:
#                           runid     the unique identifier for the
#                                     production run
#   workDir               a named directory representing the working directory
#                         where pipeline config files are deployed and the
#                         pipeline is started from
#   inputDir              a named directory representing the directory to cache
#                         or find input data
#   outputDir             a named directory representing the directory to write
#                         output data
#   updateDir             a named directory where updatable data is deployed
#   scratchDir            a named directory for temporary files that may be
#                         deleted upon completion ofthe pipeline
# @endverbatim


class Directories(object):

    ##
    # @brief determine the directories from the config input
    # @param dirConfig   the "dir" config
    # @param shortName   the short name of the pipeline
    # @param runId       the run ID for the pipeline run (default: "no-id")
    def __init__(self, dirConfig, shortName, runId="no-id"):
        ## directory config
        self.config = dirConfig
        ## run id
        self.runid = runId
        ## alias for this pipeline
        self.shortname = shortName
        ## data pattern
        self.patdata = {"runid": self.runid, "shortname": self.shortname}
        ## default root directory
        self.defroot = None

    ##
    # @brief return the default root directory
    def getDefaultRootDir(self):
        if self.defroot is not None:
            return self.defroot

        root = self.config.defaultRoot
        if root == ".":
            root = os.environ["PWD"]
        elif not os.path.isabs(root):
            root = os.path.join(os.environ["PWD"], root)
        self.defroot = root
        return root

    ##
    # @brief return the default run directory.
    # This a subdirectory of the default root directory used specifically
    # for the current run of the pipeline (given as an absolute path).
    def getDefaultRunDir(self):
        root = self.getDefaultRootDir()

        fmt = self.config.runDirPattern
        runDir = fmt % self.patdata

        if os.path.isabs(runDir):
            runDir = os.path.splitdrive(runDir)[1]
            if runDir[0] == os.sep:
                runDir = runDir[1:]

        return os.path.join(root, runDir)

    ##
    # @brief  return the absolute path to "named" directory.
    # A named directory is one that is intended for a particular role
    # and accessible via a logical name.  These include:
    # @verbatim
    #   work             the working directory (where the pipeline is started)
    #   input            the directory to cache or find input data
    #   output           the directory to write output data
    #   update           the directory where updateable data is deployed
    #   scratch          a directory for temporary files that may be
    #                       deleted upon completion of the pipeline.
    # @endverbatim
    # This function does not check that the name is one of these, so other
    # names are supported.  If a name is give that was not specified in the
    # config, the update directory is returned.
    def getNamedDirectory(self, name):
        configDict = self.config.toDict()
        try:
            dir = configDict[name] % self.patdata
        except pexExcept.Exception as e:
            dir = configDict["updateDir"] % self.patdata

        if not os.path.isabs(dir):
            dir = os.path.join(self.getDefaultRunDir(), dir)

        return dir

    ##
    # return the absolute paths for the standard named directories as a
    # PropertySet.  The keys will include "work", "input", "output",
    # "update", and "scratch".
    def getDirs(self):
        out = lsst.daf.base.PropertySet()
        for name in "workDir inputDir outputDir updateDir scratchDir".split():
            out.set(name, self.getNamedDirectory(name))
        return out
