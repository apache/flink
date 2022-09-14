#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

##
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}

if [ -z "${NEW_VERSION:-}" ]; then
    echo "NEW_VERSION was not set."
    exit 1
fi

# fail immediately
set -o errexit
set -o nounset
# print command before executing
set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

###########################

cd ..

#change version in all pom files
$MVN org.codehaus.mojo:versions-maven-plugin:2.8.1:set -DnewVersion=$NEW_VERSION -DgenerateBackupPoms=false --quiet


#change version of documentation
cd docs
perl -pi -e "s#^  Version = .*#  Version = \"${NEW_VERSION}\"#" config.toml
perl -pi -e "s#^  VersionTitle = .*#  VersionTitle = \"${NEW_VERSION}\"#" config.toml
cd ..

#change version of pyflink
cd flink-python/pyflink
perl -pi -e "s#^__version__ = \".*\"#__version__ = \"${NEW_VERSION}\"#" version.py
perl -pi -e "s#-SNAPSHOT#\\.dev0#" version.py
cd ../..

git commit -am "Update version to $NEW_VERSION"

echo "Don't forget to push the change."
