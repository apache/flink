#!/usr/bin/env bash


#
# This script is called by travis to deploy our project to sonatype SNAPSHOTS.
# It will deploy both a hadoop v1 and a hadoop v2 (yarn) artifact
# 

# Check if push/commit is eligible for pushing
echo "Job: $TRAVIS_JOB_NUMBER ; isPR: $TRAVIS_PULL_REQUEST"
if [[ $TRAVIS_JOB_NUMBER == *1 ]] && [[ $TRAVIS_PULL_REQUEST == "false" ]] ; then 
	# Deploy regular hadoop v1 to maven
	mvn -DskipTests deploy --settings deploysettings.xml; 

	# deploy hadoop v2 (yarn)
	echo "Generating poms for hadoop-yarn."
	./tools/generate_specific_pom.sh 0.4-SNAPSHOT 0.4-hadoop2-SNAPSHOT
	mvn -f pom.hadoop2.xml -DskipTests clean deploy --settings deploysettings.xml; 
fi
