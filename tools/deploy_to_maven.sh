#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
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

# fail on errors
set -e

#
# Deploys snapshot builds to Apache's snapshot repository.
#

function getVersion() {
    here="`dirname \"$0\"`"              # relative
    here="`( cd \"$here\" && pwd )`"  # absolutized and normalized
    if [ -z "$here" ] ; then
        # error; for some reason, the path is not accessible
        # to the script (e.g. permissions re-evaled after suid)
        exit 1  # fail
    fi
    flink_home="`dirname \"$here\"`"
    cd "$flink_home"
	echo `mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -E '^([0-9]+.[0-9]+(.[0-9]+)?(-[a-zA-Z0-9]+)?)$'`
}

function deploy_to_s3() {
	local CURRENT_FLINK_VERSION=$1
	local HD=$2

	echo "Installing artifacts deployment script"
	export ARTIFACTS_DEST="$HOME/bin/artifacts"
	curl -sL https://raw.githubusercontent.com/travis-ci/artifacts/master/install | bash
	PATH="$(dirname "$ARTIFACTS_DEST"):$PATH"

	echo "Deploying flink version $CURRENT_FLINK_VERSION (hadoop=$HD) to s3:"
	mkdir flink-$CURRENT_FLINK_VERSION
	cp -r flink-dist/target/flink-*-bin/flink-*/* flink-$CURRENT_FLINK_VERSION/
	tar -czf flink-$CURRENT_FLINK_VERSION-bin-$HD.tgz flink-$CURRENT_FLINK_VERSION

	artifacts upload \
		  --bucket $ARTIFACTS_S3_BUCKET \
		  --key $ARTIFACTS_AWS_ACCESS_KEY_ID \
		  --secret $ARTIFACTS_AWS_SECRET_ACCESS_KEY \
		  --target-paths / \
		  flink-$CURRENT_FLINK_VERSION-bin-$HD.tgz

	# delete files again
	rm -rf flink-$CURRENT_FLINK_VERSION
	rm flink-$CURRENT_FLINK_VERSION-bin-$HD.tgz
}

pwd


echo "install lifecycle mapping fake plugin"
git clone https://github.com/mfriedenhagen/dummy-lifecycle-mapping-plugin.git
cd dummy-lifecycle-mapping-plugin
mvn -B install
cd ..
rm -rf dummy-lifecycle-mapping-plugin


CURRENT_FLINK_VERSION=`getVersion`

echo "detected current version as: '$CURRENT_FLINK_VERSION'"

#
# This script deploys our project to sonatype SNAPSHOTS.
# It will deploy a hadoop v2 (yarn) artifact
#

if [[ $CURRENT_FLINK_VERSION == *SNAPSHOT* ]] ; then
    MVN_SNAPSHOT_OPTS="-B -Pdocs-and-source -DskipTests -Drat.skip=true -Drat.ignoreErrors=true -Dcheckstyle.skip=true \
        -DretryFailedDeploymentCount=10 clean deploy"

    # hadoop2 scala 2.11
    echo "deploy standard version (hadoop2) for scala 2.11"
    mvn ${MVN_SNAPSHOT_OPTS}
    deploy_to_s3 $CURRENT_FLINK_VERSION "hadoop2"

    exit 0
else
    exit 1
fi

