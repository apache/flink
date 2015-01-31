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


#
# QA check your changes.
# Possible options:
# BRANCH set a another branch as the "check" reference
#
#
# Use the tool like this "BRANCH=release-0.8 ./tools/qa-check.sh"
#


BRANCH=${BRANCH:-master}



here="`dirname \"$0\"`"				# relative
here="`( cd \"$here\" && pwd )`" 	# absolutized and normalized
if [ -z "$here" ] ; then
	# error; for some reason, the path is not accessible
	# to the script (e.g. permissions re-evaled after suid)
	exit 1  # fail
fi
flink_home="`dirname \"$here\"`"

echo "flink_home=$flink_home here=$here"
cd $here

if [ ! -d  "_qa_workdir" ] ; then
	echo "_qa_workdir doesnt exist. Creating it"
	mkdir _qa_workdir
fi
# attention, it overwrites
echo "_qa_workdir" > .gitignore

cd _qa_workdir

if [ ! -d  "flink" ] ; then
	echo "There is no flink copy in the workdir. Cloning flink"
	git clone git@github.com:rmetzger/flink.git flink
	cd flink
	git checkout $BRANCH
fi
cd $here
# go to refrence flink directory

cd _qa_workdir
VAR_DIR=`pwd`
cd flink

# Initialize variables
export TESTS_PASSED=true
export MESSAGES="Flink QA-Check results:"

goToTestDirectory() {
	cd $flink_home
}

############################ Methods ############################

referenceJavadocsErrors() {
	mvn javadoc:aggregate -Pdocs-and-source -Dmaven.javadoc.failOnError=false -Dquiet=false  | grep "WARNING" | wc -l >> "$VAR_DIR/_JAVADOCS_NUM_WARNINGS"
}


checkJavadocsErrors() {
	OLD_JAVADOC_ERR_CNT=`cat $VAR_DIR/_JAVADOCS_NUM_WARNINGS` 
	NEW_JAVADOC_ERR_CNT=`mvn javadoc:aggregate -Pdocs-and-source -Dmaven.javadoc.failOnError=false -Dquiet=false  | grep "WARNING" | wc -l`
	if ["$NEW_JAVADOC_ERR_CNT" -gt "$OLD_JAVADOC_ERR_CNT"]; then
		MESSAGES+=":-1: The change increases the number of javadoc errors from $OLD_JAVADOC_ERR_CNT to $NEW_JAVADOC_ERR_CNT"
		TESTS_PASSED=false
	else
		MESSAGES+=":+1: The number of javadoc errors was $OLD_JAVADOC_ERR_CNT and is now $NEW_JAVADOC_ERR_CNT"
	fi
}


################################### QA checks ###################################

##### Methods to be executed on the current 'master'
referenceJavadocsErrors


goToTestDirectory
##### Methods to be executed on the changes (in home dir)
checkJavadocsErrors


MESSAGES+="Test finished."
if ["$TESTS_PASSED" -eq "true"]; then
	MESSAGES+="Overall result: :+1:. All tests passed"
else 
	MESSAGES+="Overall result: :-1:. Some tests failed. Please check messages above"
fi

echo "$MESSAGES"


