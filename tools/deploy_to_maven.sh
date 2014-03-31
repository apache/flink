#!/usr/bin/env bash
########################################################################################################################
# Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
########################################################################################################################

#Please ask @rmetzger (on GitHub) before changing anything here. It contains some magic.

# Build Responsibilities
# 1. Deploy to sonatype (old hadoop)
# 2. Nothing
# 3. Deploy to s3 (old hadoop)
# 4. deploy to sonatype (yarn hadoop) (this build will also generate specific poms for yarn hadoop)
# 5. Deploy Javadocs.
# 6. deploy to s3 (yarn hadoop)

# Changes (since travis changed the id assignment)
# switched 2. with 3.
# switched 5. with 6.

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
	stratosphere_home="`dirname \"$here\"`"
	cd $stratosphere_home
	echo `mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version|grep -Ev '(^\[|Download\w+:)'`
}

# this will take a while
CURRENT_STRATOSPHERE_VERSION=`getVersion`
if [[ "$CURRENT_STRATOSPHERE_VERSION" == *-SNAPSHOT ]]; then
	CURRENT_STRATOSPHERE_VERSION_YARN=${CURRENT_STRATOSPHERE_VERSION/-SNAPSHOT/-hadoop2-SNAPSHOT}
else
	CURRENT_STRATOSPHERE_VERSION_YARN="$CURRENT_STRATOSPHERE_VERSION-hadoop2"
fi

echo "detected current version as: '$CURRENT_STRATOSPHERE_VERSION' ; yarn: $CURRENT_STRATOSPHERE_VERSION_YARN "

# Check if push/commit is eligible for pushing
echo "Job: $TRAVIS_JOB_NUMBER ; isPR: $TRAVIS_PULL_REQUEST"
if [[ $TRAVIS_PULL_REQUEST == "false" ]] ; then

	#
	# This script is called by travis to deploy our project to sonatype SNAPSHOTS.
	# It will deploy both a hadoop v1 and a hadoop v2 (yarn) artifact
	# 

	if [[ $TRAVIS_JOB_NUMBER == *1 ]] && [[ $TRAVIS_PULL_REQUEST == "false" ]] && [[ $CURRENT_STRATOSPHERE_VERSION == *SNAPSHOT* ]] ; then 
		# Deploy regular hadoop v1 to maven
		mvn -DskipTests deploy --settings deploysettings.xml; 
	fi

	if [[ $TRAVIS_JOB_NUMBER == *4 ]] && [[ $TRAVIS_PULL_REQUEST == "false" ]] && [[ $CURRENT_STRATOSPHERE_VERSION == *SNAPSHOT* ]] ; then 
		# deploy hadoop v2 (yarn)
		echo "Generating poms for hadoop-yarn."
		./tools/generate_specific_pom.sh $CURRENT_STRATOSPHERE_VERSION $CURRENT_STRATOSPHERE_VERSION_YARN
		# all these tweaks assume a yarn build.
		# performance tweaks here: no "clean deploy" so that actually nothing is being rebuild (could cause wrong poms inside the jars?)
		# skip tests (they were running already)
		# skip javadocs generation (already generated)
		mvn -B -f pom.hadoop2.xml -DskipTests -Dmaven.javadoc.skip=true deploy --settings deploysettings.xml; 
	fi

	if [[ $TRAVIS_JOB_NUMBER == *5 ]] && [[ $TRAVIS_PULL_REQUEST == "false" ]] && [[ $CURRENT_STRATOSPHERE_VERSION == *SNAPSHOT* ]] ; then 
		cd stratosphere-java
		mvn javadoc:javadoc
		cd target
		cd apidocs
		git init
		git config --global user.email "metzgerr@web.de"
		git config --global user.name "Travis-CI"
		git add *
		git commit -am "Javadocs from '$(date)'"
		git config credential.helper "store --file=.git/credentials"
		echo "https://$JAVADOCS_DEPLOY:@github.com" > .git/credentials
		git push -f https://github.com/stratosphere-javadocs/stratosphere-javadocs.github.io.git master:master
		rm .git/credentials
		cd ..
		cd ..
		cd ..
	fi

	#
	# Deploy binaries to DOPA
	# The TRAVIS_JOB_NUMBER here is kinda hacked. 
	# Currently, there are Builds 1-6. Build 1 is deploying to maven sonatype
	# Build 2 has no special meaning, it is the openjdk7, hadoop 1.2.1 build
	# Build 5 is openjdk7, hadoop yarn (2.0.5-beta) build.
	# Please be sure not to use Build 1 as it will always be the yarn build.
	#

	YARN_ARCHIVE=""
	if [[ $TRAVIS_JOB_NUMBER == *6 ]] ; then 
		#generate yarn poms & build for yarn.
		# it is not required to generate poms for this build.
		#./tools/generate_specific_pom.sh $CURRENT_STRATOSPHERE_VERSION $CURRENT_STRATOSPHERE_VERSION_YARN pom.xml
		#mvn -B -DskipTests clean install
		CURRENT_STRATOSPHERE_VERSION=$CURRENT_STRATOSPHERE_VERSION_YARN
		YARN_ARCHIVE="stratosphere-dist/target/*yarn.tar.gz"
	fi
	if [[ $TRAVIS_JOB_NUMBER == *3 ]] || [[ $TRAVIS_JOB_NUMBER == *6 ]] ; then 
	#	cd stratosphere-dist
	#	mvn -B -DskipTests -Pdebian-package package
	#	cd ..
		echo "Uploading build to amazon s3. Job Number: $TRAVIS_JOB_NUMBER"
		mkdir stratosphere
		cp -r stratosphere-dist/target/stratosphere-dist-*-bin/stratosphere*/* stratosphere/
		tar -czf stratosphere-$CURRENT_STRATOSPHERE_VERSION.tgz stratosphere
		
		# upload the two in parallel
		if [[ $TRAVIS_JOB_NUMBER == *6 ]] ; then
			# move to current dir
			mv $YARN_ARCHIVE .
			travis-artifacts upload --path *yarn.tar.gz --target-path / 
		fi
		travis-artifacts upload --path stratosphere-$CURRENT_STRATOSPHERE_VERSION.tgz   --target-path / 
	fi

fi # pull request check


