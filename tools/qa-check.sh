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


BRANCH=${BRANCH:-origin/master}



here="`dirname \"$0\"`"				# relative
here="`( cd \"$here\" && pwd )`" 	# absolutized and normalized
if [ -z "$here" ] ; then
	# error; for some reason, the path is not accessible
	# to the script (e.g. permissions re-evaled after suid)
	exit 1  # fail
fi
flink_home="`dirname \"$here\"`"

cd $here

if [ ! -d  "_qa_workdir" ] ; then
	echo "_qa_workdir doesnt exist. Creating it"
	mkdir _qa_workdir
fi

cd _qa_workdir

if [ ! -d  "flink" ] ; then
	echo "There is no flink copy in the workdir. Cloning flink"
	git clone http://git-wip-us.apache.org/repos/asf/flink.git flink
fi

cd flink
# fetch and checkout quietly
git fetch -q origin
git checkout -q $BRANCH
cd $here
# go to refrence flink directory

cd _qa_workdir
VAR_DIR=`pwd`
cd flink

# Initialize variables
export TESTS_PASSED=true
# Store output of results in a file in the qa dir
QA_OUTPUT="$VAR_DIR/qa_results.txt"
rm -f "$QA_OUTPUT"

append_output() {
	echo "$1"
	echo "$1" >> "$QA_OUTPUT"
}

goToTestDirectory() {
	cd $flink_home
}

############################ Methods ############################

############ Javadocs ############
JAVADOC_MVN_COMMAND="mvn javadoc:aggregate -Pdocs-and-source -Dmaven.javadoc.failOnError=false -Dquiet=false | grep  \"WARNING\|warning\|error\" | wc -l"

referenceJavadocsErrors() {
	eval $JAVADOC_MVN_COMMAND > "$VAR_DIR/_JAVADOCS_NUM_WARNINGS"
}


checkJavadocsErrors() {
	OLD_JAVADOC_ERR_CNT=`cat $VAR_DIR/_JAVADOCS_NUM_WARNINGS` 
	NEW_JAVADOC_ERR_CNT=`eval $JAVADOC_MVN_COMMAND`
	if [ "$NEW_JAVADOC_ERR_CNT" -gt "$OLD_JAVADOC_ERR_CNT" ]; then
		append_output ":-1: The change increases the number of javadoc errors from $OLD_JAVADOC_ERR_CNT to $NEW_JAVADOC_ERR_CNT"
		TESTS_PASSED=false
	else
		append_output ":+1: The number of javadoc errors was $OLD_JAVADOC_ERR_CNT and is now $NEW_JAVADOC_ERR_CNT"
	fi
}


############ Compiler warnings ############
COMPILER_WARN_MVN_COMMAND="mvn clean compile -Dmaven.compiler.showWarning=true -Dmaven.compiler.showDeprecation=true | grep \"WARNING\""
referenceCompilerWarnings() {
	eval "$COMPILER_WARN_MVN_COMMAND | tee $VAR_DIR/_COMPILER_REFERENCE_WARNINGS | wc -l" > "$VAR_DIR/_COMPILER_NUM_WARNINGS"
}

checkCompilerWarnings() {
	OLD_COMPILER_ERR_CNT=`cat $VAR_DIR/_COMPILER_NUM_WARNINGS` 
	NEW_COMPILER_ERR_CNT=`eval $COMPILER_WARN_MVN_COMMAND | tee $VAR_DIR/_COMPILER_NEW_WARNINGS | wc -l`
	if [ "$NEW_COMPILER_ERR_CNT" -gt "$OLD_COMPILER_ERR_CNT" ]; then
		append_output ":-1: The change increases the number of compiler warnings from $OLD_COMPILER_ERR_CNT to $NEW_COMPILER_ERR_CNT"
		append_output '```diff'
		append_output "First 100 warnings:"
		append_output "`diff $VAR_DIR/_COMPILER_REFERENCE_WARNINGS $VAR_DIR/_COMPILER_NEW_WARNINGS | head -n 100`"
		append_output '```'
		TESTS_PASSED=false
	else
		append_output ":+1: The number of compiler warnings was $OLD_COMPILER_ERR_CNT and is now $NEW_COMPILER_ERR_CNT"
	fi
}

############ Files in lib ############
BUILD_MVN_COMMAND="mvn clean package -DskipTests -Dmaven.javadoc.skip=true"
COUNT_LIB_FILES="find . | grep \"\/lib\/\" | grep -v \"_qa_workdir\" | wc -l"
referenceLibFiles() {
	eval $BUILD_MVN_COMMAND > /dev/null
	eval $COUNT_LIB_FILES > "$VAR_DIR/_NUM_LIB_FILES"
}

checkLibFiles() {
	OLD_LIB_FILES_CNT=`cat $VAR_DIR/_NUM_LIB_FILES` 
	eval $BUILD_MVN_COMMAND > /dev/null
	NEW_LIB_FILES_CNT=`eval $COUNT_LIB_FILES`
	if [ "$NEW_LIB_FILES_CNT" -gt "$OLD_LIB_FILES_CNT" ]; then
		append_output ":-1: The change increases the number of dependencies in the lib/ folder from $OLD_LIB_FILES_CNT to $NEW_LIB_FILES_CNT"
		TESTS_PASSED=false
	else
		append_output ":+1: The number of files in the lib/ folder was $OLD_LIB_FILES_CNT before the change and is now $NEW_LIB_FILES_CNT"
	fi
}

############ @author tag ############

checkAuthorTag() {
	# we are grep-ing for "java" but we've messed up the string a bit so that it doesn't find exactly this line.
	if [ `grep -r "@author" . | grep "ja""va" | wc -l` -gt "0" ]; then
		append_output ":-1: The change contains @author tags"
		TESTS_PASSED=false
	fi
}


################################### QA checks ###################################

append_output "Computing Flink QA-Check results (please be patient)."

##### Methods to be executed on the current 'master'
referenceJavadocsErrors
referenceCompilerWarnings
referenceLibFiles


goToTestDirectory
## Methods to be executed on the changes (flink root dir)
checkJavadocsErrors
checkCompilerWarnings
checkLibFiles
checkAuthorTag


append_output "QA-Check finished."
if [ "$TESTS_PASSED" == "true" ]; then
	append_output "Overall result: :+1:. All tests passed"
else 
	append_output "Overall result: :-1:. Some tests failed. Please check messages above"
fi
