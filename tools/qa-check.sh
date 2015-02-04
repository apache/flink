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
	git clone https://github.com/apache/flink.git flink
	cd flink
	
fi
cd flink
git fetch origin
git checkout $BRANCH
cd $here
# go to refrence flink directory

cd _qa_workdir
VAR_DIR=`pwd`
cd flink

# Initialize variables
export TESTS_PASSED=true
export MESSAGES="Flink QA-Check results:"$'\n'

goToTestDirectory() {
	cd $flink_home
}

############################ Methods ############################

############ Javadocs ############
JAVADOC_MVN_COMMAND="mvn javadoc:aggregate -Pdocs-and-source -Dmaven.javadoc.failOnError=false -Dquiet=false | grep  \"WARNING\|warning\|error\" | wc -l"
echo $JAVADOC_MVN_COMMAND
referenceJavadocsErrors() {
	eval $JAVADOC_MVN_COMMAND > "$VAR_DIR/_JAVADOCS_NUM_WARNINGS"
}


checkJavadocsErrors() {
	OLD_JAVADOC_ERR_CNT=`cat $VAR_DIR/_JAVADOCS_NUM_WARNINGS` 
	NEW_JAVADOC_ERR_CNT=`eval $JAVADOC_MVN_COMMAND`
	if [ "$NEW_JAVADOC_ERR_CNT" -gt "$OLD_JAVADOC_ERR_CNT" ]; then
		MESSAGES+=":-1: The change increases the number of javadoc errors from $OLD_JAVADOC_ERR_CNT to $NEW_JAVADOC_ERR_CNT"$'\n'
		TESTS_PASSED=false
	else
		MESSAGES+=":+1: The number of javadoc errors was $OLD_JAVADOC_ERR_CNT and is now $NEW_JAVADOC_ERR_CNT"$'\n'
	fi
}


############ Compiler warnings ############
COMPILER_WARN_MVN_COMMAND="mvn clean compile -Dmaven.compiler.showWarning=true -Dmaven.compiler.showDeprecation=true | grep \"WARNING\" | wc -l"
referenceCompilerWarnings() {
	eval $COMPILER_WARN_MVN_COMMAND > "$VAR_DIR/_COMPILER_NUM_WARNINGS"
}

checkCompilerWarnings() {
	OLD_COMPILER_ERR_CNT=`cat $VAR_DIR/_COMPILER_NUM_WARNINGS` 
	NEW_COMPILER_ERR_CNT=`eval $COMPILER_WARN_MVN_COMMAND`
	if [ "$NEW_COMPILER_ERR_CNT" -gt "$OLD_COMPILER_ERR_CNT" ]; then
		MESSAGES+=":-1: The change increases the number of compiler warnings from $OLD_COMPILER_ERR_CNT to $NEW_COMPILER_ERR_CNT"$'\n'
		TESTS_PASSED=false
	else
		MESSAGES+=":+1: The number of compiler warnings was $OLD_COMPILER_ERR_CNT and is now $NEW_COMPILER_ERR_CNT"$'\n'
	fi
}

############ Files in lib ############
BUILD_MVN_COMMAND="mvn clean package -DskipTests"
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
		MESSAGES+=":-1: The change increases the number of dependencies in the lib/ folder from $OLD_LIB_FILES_CNT to $NEW_LIB_FILES_CNT"$'\n'
		TESTS_PASSED=false
	else
		MESSAGES+=":+1: The number of files in the lib/ folder was $OLD_LIB_FILES_CNT before the change and is now $NEW_LIB_FILES_CNT"$'\n'
	fi
}

############ @author tag ############

checkAuthorTag() {
	if [ `grep -r "@author" . | grep java | wc -l` -gt "0" ]; then
		MESSAGES+=":-1: The change contains @author tags"$'\n'
		TESTS_PASSED=false
	fi
}


################################### QA checks ###################################

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


MESSAGES+=$'\n'"Test finished."$'\n'
if [ "$TESTS_PASSED" == "true" ]; then
	MESSAGES+="Overall result: :+1:. All tests passed"$'\n'
else 
	MESSAGES+="Overall result: :-1:. Some tests failed. Please check messages above"$'\n'
fi

echo "$MESSAGES"


