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
# 1. Deploy snapshot & S3 (hadoop1)
# 2. Deploy snapshot & S3 (hadoop2)
# 3. Nothing
# 4. Nothing
# 5. Nothing


echo "install lifecylce mapping fake plugin"
git clone https://github.com/mfriedenhagen/dummy-lifecycle-mapping-plugin.git
cd dummy-lifecycle-mapping-plugin
mvn -B install
cd ..

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

pwd

# this will take a while
CURRENT_FLINK_VERSION=`getVersion`
if [[ "$CURRENT_FLINK_VERSION" == *-SNAPSHOT ]]; then
	CURRENT_FLINK_VERSION_HADOOP1=${CURRENT_FLINK_VERSION/-SNAPSHOT/-hadoop1-SNAPSHOT}
else
	CURRENT_FLINK_VERSION_HADOOP1="$CURRENT_FLINK_VERSION-hadoop1"
fi

echo "detected current version as: '$CURRENT_FLINK_VERSION' ; hadoop1: $CURRENT_FLINK_VERSION_HADOOP1 "


# Check if push/commit is eligible for pushing
echo "Job: $TRAVIS_JOB_NUMBER ; isPR: $TRAVIS_PULL_REQUEST"
if [[ $TRAVIS_PULL_REQUEST == "false" ]] ; then

	#
	# This script is called by travis to deploy our project to sonatype SNAPSHOTS.
	# It will deploy both a hadoop v1 and a hadoop v2 (yarn) artifact
	# 

	if [[ $TRAVIS_JOB_NUMBER == *1 ]] && [[ $TRAVIS_PULL_REQUEST == "false" ]] && [[ $CURRENT_FLINK_VERSION == *SNAPSHOT* ]] ; then 
		# Deploy hadoop v1 to maven
		echo "Generating poms for hadoop1"
		./tools/generate_specific_pom.sh $CURRENT_FLINK_VERSION $CURRENT_FLINK_VERSION_HADOOP1 pom.hadoop1.xml
		mvn -B -f pom.hadoop1.xml -Pdocs-and-source -DskipTests -Drat.ignoreErrors=true deploy --settings deploysettings.xml; 
	fi

	if [[ $TRAVIS_JOB_NUMBER == *2 ]] && [[ $TRAVIS_PULL_REQUEST == "false" ]] && [[ $CURRENT_FLINK_VERSION == *SNAPSHOT* ]] ; then 
		# deploy hadoop v2 (yarn)
		echo "deploy standard version (hadoop2)"
		mvn -B -DskipTests -Pdocs-and-source -Drat.ignoreErrors=true clean deploy --settings deploysettings.xml;
	fi

	if [[ $TRAVIS_JOB_NUMBER == *1 ]] || [[ $TRAVIS_JOB_NUMBER == *2 ]] ; then
		echo "Uploading build to amazon s3. Job Number: $TRAVIS_JOB_NUMBER"
		
		HD="hadoop1"
		
		# job nr 2 is hadoop 2
		if [[ $TRAVIS_JOB_NUMBER == *2 ]] ; then
			HD="hadoop2"
		fi

		mkdir flink-$CURRENT_FLINK_VERSION
		cp -r flink-dist/target/flink-*-bin/flink-$CURRENT_FLINK_VERSION*/* flink-$CURRENT_FLINK_VERSION/
		tar -czf flink-$CURRENT_FLINK_VERSION-bin-$HD.tgz flink-$CURRENT_FLINK_VERSION
		travis-artifacts upload --path flink-$CURRENT_FLINK_VERSION-bin-$HD.tgz   --target-path / 
		echo "doing a ls -lisah:"
		ls -lisah
	fi
fi # pull request check


