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


#Please ask @rmetzger (on GitHub) before changing anything here. It contains some magic.

# Build Responsibilities
# 1. Nothing
# 2. Nothing
# 3. Nothing
# 4. Deploy snapshot & S3 (hadoop2)
# 5. Deploy snapshot & S3 (hadoop1)


function getVersion() {
	here="`dirname \"$0\"`"              # relative
	here="`( cd \"$here\" && pwd )`"  # absolutized and normalized
	if [ -z "$here" ] ; then
		# error; for some reason, the path is not accessible
		# to the script (e.g. permissions re-evaled after suid)
		exit 1  # fail
	fi
	flink_home="`dirname \"$here\"`"
	cd $flink_home
	echo `mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -E '^([0-9]+.[0-9]+(.[0-9]+)?(-[a-zA-Z0-9]+)?)$'`
}

function deploy_to_s3() {
	CURRENT_FLINK_VERSION=$1
	HD=$2

	echo "Installing artifacts deployment script"
	export ARTIFACTS_DEST="$HOME/bin/artifacts"
	curl -sL https://raw.githubusercontent.com/travis-ci/artifacts/master/install | bash
	PATH="$(dirname "$ARTIFACTS_DEST"):$PATH"

	echo "Deploying flink version $CURRENT_FLINK_VERSION (hadoop=$HD) to s3:"
	mkdir flink-$CURRENT_FLINK_VERSION
	cp -r flink-dist/target/flink-*-bin/flink-$CURRENT_FLINK_VERSION*/* flink-$CURRENT_FLINK_VERSION/
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


# Check if push/commit is eligible for deploying
echo "Job: $TRAVIS_JOB_NUMBER ; isPR: $TRAVIS_PULL_REQUEST ; repo slug : $TRAVIS_REPO_SLUG "
if [[ $TRAVIS_PULL_REQUEST == "false" ]] && [[ $TRAVIS_REPO_SLUG == "apache/flink" ]] ; then

	echo "install lifecylce mapping fake plugin"
	git clone https://github.com/mfriedenhagen/dummy-lifecycle-mapping-plugin.git
	cd dummy-lifecycle-mapping-plugin
	mvn -B install
	cd ..

	# this will take a while
	CURRENT_FLINK_VERSION=`getVersion`
	if [[ "$CURRENT_FLINK_VERSION" == *-SNAPSHOT ]]; then
		CURRENT_FLINK_VERSION_HADOOP1=${CURRENT_FLINK_VERSION/-SNAPSHOT/-hadoop1-SNAPSHOT}
	else
		CURRENT_FLINK_VERSION_HADOOP1="$CURRENT_FLINK_VERSION-hadoop1"
	fi

	echo "detected current version as: '$CURRENT_FLINK_VERSION' ; hadoop1: $CURRENT_FLINK_VERSION_HADOOP1 "

	#
	# This script is called by travis to deploy our project to sonatype SNAPSHOTS.
	# It will deploy both a hadoop v1 and a hadoop v2 (yarn) artifact
	# 

	if [[ $TRAVIS_JOB_NUMBER == *5 ]] &&  [[ $CURRENT_FLINK_VERSION == *SNAPSHOT* ]] ; then 
		# Deploy hadoop v1 to maven
		echo "Generating poms for hadoop1"
		./tools/generate_specific_pom.sh $CURRENT_FLINK_VERSION $CURRENT_FLINK_VERSION_HADOOP1 pom.hadoop1.xml
		mvn -B -f pom.hadoop1.xml -DskipTests -Drat.ignoreErrors=true deploy --settings deploysettings.xml; 

		# deploy to s3
		deploy_to_s3 $CURRENT_FLINK_VERSION "hadoop1"
	fi

	if [[ $TRAVIS_JOB_NUMBER == *4 ]] && [[ $CURRENT_FLINK_VERSION == *SNAPSHOT* ]] ; then 
		# the time to build and upload flink twice (scala 2.10 and scala 2.11) takes
		# too much time. That's why we are going to do it in parallel
		# Note that the parallel execution will cause the output to be interleaved
		mkdir ../flink2
		ls ../
		cp -r . ../flink2
		cd ../flink2
		# deploy hadoop v2 (yarn)
		echo "deploy standard version (hadoop2) for scala 2.10 from flink2 directory"
		# do the hadoop2 scala 2.10 in the background
		(mvn -B -DskipTests -Drat.skip=true -Drat.ignoreErrors=true clean deploy --settings deploysettings.xml; deploy_to_s3 $CURRENT_FLINK_VERSION "hadoop2" ) &

		# switch back to the regular flink directory
		cd ../flink
		echo "deploy hadoop2 version (standard) for scala 2.11 from flink directory"
		./tools/change-scala-version.sh 2.11
		mvn -B -DskipTests -Drat.skip=true -Drat.ignoreErrors=true clean deploy --settings deploysettings.xml;

		deploy_to_s3 $CURRENT_FLINK_VERSION "hadoop2_2.11"

		echo "Changing back to scala 2.10"
		./tools/change-scala-version.sh 2.10
	fi
fi # pull request check


