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

config_yml=../docs/_config.yml

#change version of documentation
perl -pi -e "s#^version: .*#version: \"${RELEASE_VERSION}\"#" ${config_yml}

# The version in the title should not contain the bugfix version (e.g. 1.3)
perl -pi -e "s#^version_title: .*#version_title: ${SHORT_RELEASE_VERSION}#" ${config_yml}

perl -pi -e "s#^github_branch: .*#github_branch: release-${SHORT_RELEASE_VERSION}#" ${config_yml}

url_base="//ci.apache.org/projects/flink/flink-docs-release-"
perl -pi -e "s#^baseurl: .*#baseurl: ${url_base}${SHORT_RELEASE_VERSION}#" ${config_yml}
perl -pi -e "s#^javadocs_baseurl: .*#javadocs_baseurl: ${url_base}${SHORT_RELEASE_VERSION}#" ${config_yml}
perl -pi -e "s#^pythondocs_baseurl: .*#pythondocs_baseurl: ${url_base}${SHORT_RELEASE_VERSION}#" ${config_yml}

perl -pi -e "s#^is_stable: .*#is_stable: true#" ${config_yml}

perl -pi -e "s#^__version__ = \".*\"#__version__ = \"${RELEASE_VERSION}\"#" ../flink-python/pyflink/version.py

git commit -am "Update for ${RELEASE_VERSION}"

echo "Done. Don't forget to push the branch."
