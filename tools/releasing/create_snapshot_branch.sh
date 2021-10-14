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

if [ -z "${SHORT_RELEASE_VERSION}" ]; then
    echo "SHORT_RELEASE_VERSION was not set."
    exit 1
fi

if [ -z "${RELEASE_VERSION}" ]; then
    echo "RELEASE_VERSION was not set."
    exit 1
fi

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

# fail immediately
set -o errexit
set -o nounset

git checkout master
git checkout -b release-${SHORT_RELEASE_VERSION}

config_file=../docs/config.toml

#change version of documentation
perl -pi -e "s#^  Version = .*#  Version = \"${RELEASE_VERSION}\"#" ${config_file}

# The version in the title should not contain the bugfix version (e.g. 1.3)
perl -pi -e "s#^  VersionTitle = .*#  VersionTitle = \"${SHORT_RELEASE_VERSION}\"#" ${config_file}

perl -pi -e "s#^  Branch = .*#  Branch = \"release-${SHORT_RELEASE_VERSION}\"#" ${config_file}

url_base="//nightlies.apache.org/flink/flink-docs-release-"
perl -pi -e "s#^baseURL = .*#baseURL = \'${url_base}${SHORT_RELEASE_VERSION}\'#" ${config_file}
perl -pi -e "s#^  JavaDocs = .*#  JavaDocs = \"${url_base}${SHORT_RELEASE_VERSION}/api/java/\"#" ${config_file}
perl -pi -e "s#^    \[\"JavaDocs\", .*#    \[\"JavaDocs\", \"${url_base}${SHORT_RELEASE_VERSION}/api/java/\"\],#" ${config_file}
perl -pi -e "s#^  ScalaDocs = .*#  ScalaDocs = \"${url_base}${SHORT_RELEASE_VERSION}/api/scala/index.html\#org.apache.flink.api.scala.package\"#" ${config_file}
perl -pi -e "s#^    \[\"ScalaDocs\", .*#    \[\"ScalaDocs\", \"${url_base}${SHORT_RELEASE_VERSION}/api/scala/index.html\#org.apache.flink.api.scala.package/\"\]#" ${config_file}
perl -pi -e "s#^  PyDocs = .*#  PyDocs = \"${url_base}${SHORT_RELEASE_VERSION}/api/python/\"#" ${config_file}
perl -pi -e "s#^    \[\"PyDocs\", .*#    \[\"PyDocs\", \"${url_base}${SHORT_RELEASE_VERSION}/api/python/\"\]#" ${config_file}

perl -pi -e "s#^  PreviousDocs = \[#  PreviousDocs = \[\n    \[\"${SHORT_RELEASE_VERSION}\", \"http:${url_base}${SHORT_RELEASE_VERSION}\"\],#" ${config_file}

perl -pi -e "s#^  IsStable = .*#  IsStable = true#" ${config_file}

perl -pi -e "s#dev-master#dev-${SHORT_RELEASE_VERSION}#" ../flink-end-to-end-tests/test-scripts/common_docker.sh

git commit -am "Update for ${RELEASE_VERSION}"

echo "Done. Don't forget to push the branch."
