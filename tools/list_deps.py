#!/usr/bin/env python
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import xml.etree.ElementTree as ET
import os
import fnmatch
import sys

# This lists all dependencies in the Maven Project root given as first
# argument. If a dependency is included in several versions it is listed once
# for every version. The result output is sorted. So this can be used
# to get a diff between the Maven dependencies of two versions of a project.

path = sys.argv[1]

pomfiles = [os.path.join(dirpath, f)
    for dirpath, dirnames, files in os.walk(path)
    for f in fnmatch.filter(files, 'pom.xml')]

deps = set()

for pom in pomfiles:

    root = ET.parse(pom).getroot()

    for dep in root.iter('{http://maven.apache.org/POM/4.0.0}dependency'):
        groupId = dep.find("{http://maven.apache.org/POM/4.0.0}groupId").text
        artifactId = dep.find("{http://maven.apache.org/POM/4.0.0}artifactId").text
        version = dep.find("{http://maven.apache.org/POM/4.0.0}version")
        # if it has no version tag it must be in dependencyManagement somewhere, with
        # a version tag, so we already have it in our list
        if version != None:
            if "flink" in groupId: continue

            depstring = groupId + " " + artifactId + " " + version.text
            deps.add(depstring)

deplist = list(deps)
deplist.sort()

for dep in deplist:
    print dep



#for atype in e.findall('dependency'):
#    print(atype.get('foobar'))
