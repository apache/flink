#!/usr/bin/env bash


CURRENT_STRATOSPHERE_VERSION=0.4-SNAPSHOT
CURRENT_STRATOSPHERE_VERSION_YARN=0.4-hadoop2-SNAPSHOT

# Check if push/commit is eligible for pushing
echo "Job: $TRAVIS_JOB_NUMBER ; isPR: $TRAVIS_PULL_REQUEST"
if [[ $TRAVIS_PULL_REQUEST == "false" ]] ; then

	#
	# This script is called by travis to deploy our project to sonatype SNAPSHOTS.
	# It will deploy both a hadoop v1 and a hadoop v2 (yarn) artifact
	# 

	if [[ $TRAVIS_JOB_NUMBER == *1 ]] && [[ $TRAVIS_PULL_REQUEST == "false" ]] ; then 
		# Deploy regular hadoop v1 to maven
		mvn -DskipTests deploy --settings deploysettings.xml; 

		# deploy hadoop v2 (yarn)
		echo "Generating poms for hadoop-yarn."
		./tools/generate_specific_pom.sh $CURRENT_STRATOSPHERE_VERSION $CURRENT_STRATOSPHERE_VERSION_YARN
		mvn -f pom.hadoop2.xml -DskipTests clean deploy --settings deploysettings.xml; 
	fi

	#
	# Deploy binaries to DOPA
	# The TRAVIS_JOB_NUMBER here is kinda hacked. 
	# Currently, there are Builds 1-6. Build 1 is deploying to maven sonatype
	# Build 2 has no special meaning, it is the openjdk7, hadoop 1.2.1 build
	# Build 5 is openjdk7, hadoop yarn (2.0.5-beta) build.
	# Please be sure not to use Build 1 as it will always be the yarn build.
	#

	if [[ $TRAVIS_JOB_NUMBER == *5 ]] ; then 
		#generate yarn poms & build for yarn.
		./tools/generate_specific_pom.sh $CURRENT_STRATOSPHERE_VERSION $CURRENT_STRATOSPHERE_VERSION_YARN pom.xml
		mvn -DskipTests clean package
		CURRENT_STRATOSPHERE_VERSION=$CURRENT_STRATOSPHERE_VERSION_YARN
	fi
	if [[ $TRAVIS_JOB_NUMBER == *2 ]] || [[ $TRAVIS_JOB_NUMBER == *5 ]] ; then 
		sudo apt-get install sshpass
		echo "Uploading build to dopa.dima.tu-berlin.de. Job Number: $TRAVIS_JOB_NUMBER"
		mkdir stratosphere
		cp -r stratosphere-dist/target/stratosphere-dist-$CURRENT_STRATOSPHERE_VERSION-bin/stratosphere-$CURRENT_STRATOSPHERE_VERSION/* stratosphere/
		tar -czf stratosphere-$CURRENT_STRATOSPHERE_VERSION.tgz stratosphere
		sshpass -p "$DOPA_PASS" scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r stratosphere-$CURRENT_STRATOSPHERE_VERSION.tgz stratosphere-dist $DOPA_USER@dopa.dima.tu-berlin.de:bin/
	fi

fi # pull request check


