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
PUBLISH_SNAPSHOTS=${PUBLISH_SNAPSHOTS:-false}

if [ -z "${MAVEN_DEPLOY_USER:-}" ] || [ -z "${MAVEN_DEPLOY_PASS:-}" ]; then
    echo "MAVEN_DEPLOY_USER / MAVEN_DEPLOY_PASS were not set."
    exit 1
fi
if [ -z "${MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR:-}" ]; then
    echo "MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR was not set. Source tools/ci/maven-utils.sh first."
    exit 1
fi
if [ "${PUBLISH_SNAPSHOTS}" != "true" ] && [ -z "${STAGING_JARS_DIR:-}" ]; then
    echo "STAGING_JARS_DIR was not set."
    exit 1
fi

# fail immediately
set -o errexit
set -o nounset
# print command before executing
set -o xtrace

trap 'echo "deploy_nightly_maven_snapshot.sh failed (line ${LINENO})" 1>&2' ERR

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

cat << EOF > deploy-settings.xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                              https://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>apache.snapshots.https</id>
      <username>${MAVEN_DEPLOY_USER}</username>
      <password>${MAVEN_DEPLOY_PASS}</password>
    </server>
  </servers>
  <mirrors>
    <mirror>
      <id>google-maven-central</id>
      <name>GCS Maven Central mirror</name>
      <url>https://maven-central-eu.storage-download.googleapis.com/maven2/</url>
      <mirrorOf>central</mirrorOf>
    </mirror>
  </mirrors>
</settings>
EOF

export CUSTOM_OPTIONS="${MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR} -Dgpg.skip -Drat.skip -Dcheckstyle.skip --settings $(pwd)/deploy-settings.xml"

if [ "${PUBLISH_SNAPSHOTS}" != "true" ]; then
  # this branch's snapshot jars are published by the Azure pipeline, so
  #  deploy into a local repository that is kept as a build artifact
  #  instead of pushing to repository.apache.org
  echo "Not publishing snapshots - deploying to ${STAGING_JARS_DIR} instead of repository.apache.org"
  export CUSTOM_OPTIONS="${CUSTOM_OPTIONS} -DaltDeploymentRepository=staging::file://${STAGING_JARS_DIR}"
fi

export MVN_RUN_VERBOSE=true
./releasing/deploy_staging_jars.sh
