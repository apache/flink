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
RELEASE_CANDIDATE=${RELEASE_CANDIDATE:-none}
MVN=${MVN:-mvn}

if [ -z "${OLD_VERSION}" ]; then
    echo "OLD_VERSION was not set."
    exit 1
fi

if [ -z "${NEW_VERSION}" ]; then
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

target_branch=release-$NEW_VERSION
if [ "$RELEASE_CANDIDATE" != "none" ]; then
  target_branch=$target_branch-rc$RELEASE_CANDIDATE
fi

git checkout -b $target_branch

#change version in all pom files
find . -name 'pom.xml' -type f -exec perl -pi -e 's#<version>(.*)'$OLD_VERSION'(.*)</version>#<version>${1}'$NEW_VERSION'${2}</version>#' {} \;

#change version of documentation
cd docs
perl -pi -e "s#^version: .*#version: \"${NEW_VERSION}\"#" _config.yml

# The version in the title should not contain the bugfix version (e.g. 1.3)
VERSION_TITLE=$(echo $NEW_VERSION | sed 's/\.[^.]*$//')
perl -pi -e "s#^version_title: .*#version_title: ${VERSION_TITLE}#" _config.yml
cd ..

git commit -am "Commit for release $NEW_VERSION"

RELEASE_HASH=`git rev-parse HEAD`
echo "Echo created release hash $RELEASE_HASH"

echo "Done. Don't forget to create the signed release tag at the end and push the changes."
