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

# check arguments
MVN=${MVN:-mvn}

if [ -z "${SHORT_RELEASE_VERSION:-}" ]; then
    echo "SHORT_RELEASE_VERSION was not set."
    exit 1
fi

if [ -z "${PREVIEW:-}" ]; then
    echo "PREVIEW was not set."
    exit 1
fi

if [ -z "${RELEASE_CANDIDATE:-}" ]; then
    echo "RELEASE_CANDIDATE was not set."
    exit 1
fi

NEW_VERSION=${SHORT_RELEASE_VERSION}-preview${PREVIEW}
TARGET_BRANCH=release-${SHORT_RELEASE_VERSION}-preview${PREVIEW}-rc${RELEASE_CANDIDATE}

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

# create branch
git checkout master
git checkout -b ${TARGET_BRANCH}

# change version in all pom files
$MVN org.codehaus.mojo:versions-maven-plugin:2.8.1:set -DnewVersion=${NEW_VERSION} -DgenerateBackupPoms=false --quiet

# change version of documentation
config_file=docs/config.toml

perl -pi -e "s#^  Version = .*#  Version = \"${NEW_VERSION}\"#" ${config_file}
perl -pi -e "s#^  VersionTitle = .*#  VersionTitle = \"${NEW_VERSION}\"#" ${config_file}
perl -pi -e "s#^  Branch = .*#  Branch = \"${TARGET_BRANCH}\"#" ${config_file}

url_base="//nightlies.apache.org/flink/flink-docs-release-"${NEW_VERSION}
perl -pi -e "s#^baseURL = .*#baseURL = \'${url_base}\'#" ${config_file}
perl -pi -e "s#^  JavaDocs = .*#  JavaDocs = \"${url_base}/api/java/\"#" ${config_file}
perl -pi -e "s#^    \[\"JavaDocs\", .*#    \[\"JavaDocs\", \"${url_base}/api/java/\"\],#" ${config_file}
perl -pi -e "s#^  ScalaDocs = .*#  ScalaDocs = \"${url_base}/api/scala/index.html\#org.apache.flink.api.scala.package\"#" ${config_file}
perl -pi -e "s#^    \[\"ScalaDocs\", .*#    \[\"ScalaDocs\", \"${url_base}/api/scala/index.html\#org.apache.flink.api.scala.package/\"\],#" ${config_file}
perl -pi -e "s#^  PyDocs = .*#  PyDocs = \"${url_base}/api/python/\"#" ${config_file}
perl -pi -e "s#^    \[\"PyDocs\", .*#    \[\"PyDocs\", \"${url_base}/api/python/\"\]#" ${config_file}

perl -pi -e "s#^  PreviousDocs = \[#  PreviousDocs = \[\n    \[\"${NEW_VERSION}\", \"http:${url_base}\"\],#" ${config_file}

perl -pi -e "s#^  IsStable = .*#  IsStable = true#" ${config_file}

# change version of pyflink
pushd flink-python/pyflink
perl -pi -e "s#^__version__ = \".*\"#__version__ = \"${NEW_VERSION}\"#" version.py
popd

# change version for docker
perl -pi -e "s#dev-master#dev-${SHORT_RELEASE_VERSION}#" flink-end-to-end-tests/test-scripts/common_docker.sh

git commit -am "Commit for release $NEW_VERSION"

RELEASE_HASH=`git rev-parse HEAD`
echo "Echo created release hash $RELEASE_HASH"

echo "Done. Don't forget to create the signed release tag at the end and push the changes."
