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

if [ -f  "_qa_workdir"] ; then
	echo "_qa_workdir doesnt exist. Creating it"
	mkdir _qa_workdir
fi

cd _qa_workdir
if [ -f  "flink"] ; then
	echo "There is no flink copy in the workdir. Cloning flink"
	git clone https://github.com/apache/flink.git flink
	cd flink
	git checkout $BRANCH
fi
cd $here
# go to refrence flink directory
cd _qa_workdir/flink

TESTS_PASSED=true
MESSAGES="Flink QA-Check results:"

goToTestDirectory() {
	cd $flink_home
}

################################### QA checks ###################################

##### Methods to be executed on the current 'master'
referenceJavadocsErrors()

##### Methods to be executed on the changes
checkJavadocsErrors()



############################ Methods ############################

referenceJavadocsErrors() {
	 mvn javadoc:aggregate -Pdocs-and-source -Dmaven.javadoc.failOnError=false -Dquiet=false  | grep "WARNING" | wc -l >> _JAVADOCS_NUM_WARNINGS
}

