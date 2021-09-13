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

#
# This file contains generic control over the test execution.
#

HERE="`dirname \"$0\"`"             # relative
HERE="`( cd \"$HERE\" && pwd )`"    # absolutized and normalized
if [ -z "$HERE" ] ; then
	exit 1
fi

source "${HERE}/stage.sh"
source "${HERE}/maven-utils.sh"
source "${HERE}/controller_utils.sh"

STAGE=$1

# =============================================================================
# Step 0: Check & print environment information & configure env
# =============================================================================

# check preconditions
if [ -z "$DEBUG_FILES_OUTPUT_DIR" ] ; then
	echo "ERROR: Environment variable 'DEBUG_FILES_OUTPUT_DIR' is not set but expected by test_controller.sh. Tests may use this location to store debugging files."
	exit 1
fi

if [ ! -d "$DEBUG_FILES_OUTPUT_DIR" ] ; then
	echo "ERROR: Environment variable DEBUG_FILES_OUTPUT_DIR=$DEBUG_FILES_OUTPUT_DIR points to a directory that does not exist"
	exit 1
fi

if [ -z "$STAGE" ] ; then
	echo "ERROR: Environment variable 'STAGE' is not set but expected by test_controller.sh. THe variable refers to the stage being executed."
	exit 1
fi

echo "Printing environment information"

echo "PATH=$PATH"
run_mvn -version
echo "Commit: $(git rev-parse HEAD)"
print_system_info

# enable coredumps for this process
ulimit -c unlimited

# configure JVMs to produce heap dumps
export JAVA_TOOL_OPTIONS="-XX:+HeapDumpOnOutOfMemoryError"

# some tests provide additional logs if they find this variable
export IS_CI=true

export WATCHDOG_ADDITIONAL_MONITORING_FILES="$DEBUG_FILES_OUTPUT_DIR/mvn-*.log"

source "${HERE}/watchdog.sh"

# =============================================================================
# Step 1: Rebuild jars and install Flink to local maven repository
# =============================================================================

export LOG4J_PROPERTIES=${HERE}/log4j.properties
MVN_LOGGING_OPTIONS="-Dlog.dir=${DEBUG_FILES_OUTPUT_DIR} -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES"

MVN_COMMON_OPTIONS="-Dflink.forkCount=2 -Dflink.forkCountTestPackage=2 -Dfast -Pskip-webui-build $MVN_LOGGING_OPTIONS"
MVN_COMPILE_OPTIONS="-DskipTests"
MVN_COMPILE_MODULES=$(get_compile_modules_for_stage ${STAGE})

CALLBACK_ON_TIMEOUT="print_stacktraces | tee ${DEBUG_FILES_OUTPUT_DIR}/jps-traces.out"
run_with_watchdog "run_mvn $MVN_COMMON_OPTIONS $MVN_COMPILE_OPTIONS $PROFILE $MVN_COMPILE_MODULES install" $CALLBACK_ON_TIMEOUT
EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
	echo "=============================================================================="
	echo "Compilation failure detected, skipping test execution."
	echo "=============================================================================="
	exit $EXIT_CODE
fi


# =============================================================================
# Step 2: Run tests
# =============================================================================

if [ $STAGE == $STAGE_PYTHON ]; then
	sed -i "s/\(^appender\.file\.fileName = \).*$/\1\$\{sys:log\.file\}/g" ${HERE}/log4j.properties
	run_with_watchdog "./flink-python/dev/lint-python.sh" $CALLBACK_ON_TIMEOUT
	EXIT_CODE=$?
else
	MVN_TEST_OPTIONS="-Dflink.tests.with-openssl"
	if [ $STAGE = $STAGE_LEGACY_SLOT_MANAGEMENT ]; then
		if [[ ${PROFILE} == *"enable-adaptive-scheduler"* ]]; then
			echo "Skipping legacy slot management test stage in adaptive scheduler job"
			exit 0
		fi
		MVN_TEST_OPTIONS="$MVN_TEST_OPTIONS -Dflink.tests.disable-declarative"
	fi
	if [ $STAGE = $STAGE_FINEGRAINED_RESOURCE_MANAGEMENT ]; then
		if [[ ${PROFILE} == *"enable-adaptive-scheduler"* ]]; then
			echo "Skipping fine grained resource management test stage in adaptive scheduler job"
			exit 0
		fi
		MVN_TEST_OPTIONS="$MVN_TEST_OPTIONS -Dflink.tests.enable-fine-grained"
	fi
	MVN_TEST_MODULES=$(get_test_modules_for_stage ${STAGE})

	run_with_watchdog "run_mvn $MVN_COMMON_OPTIONS $MVN_TEST_OPTIONS $PROFILE $MVN_TEST_MODULES verify" $CALLBACK_ON_TIMEOUT
	EXIT_CODE=$?
fi

# =============================================================================
# Step 3: Put extra logs into $DEBUG_FILES_OUTPUT_DIR
# =============================================================================

# only misc builds flink-yarn-tests
case $STAGE in
	(misc)
		put_yarn_logs_to_artifacts
	;;
esac

collect_coredumps $(pwd) $DEBUG_FILES_OUTPUT_DIR

# Exit code for CI build success/failure
exit $EXIT_CODE
