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
source "${HERE}/watchdog.sh"
TEST=$1

# =============================================================================
# Step 0: Check & print environment information & configure env
# =============================================================================

# check preconditions
if [ -z "$DEBUG_FILES" ] ; then
	echo "ERROR: Environment variable 'DEBUG_FILES' is not set but expected by test_controller.sh"
	exit 1
fi

if [ ! -d "$DEBUG_FILES" ] ; then
	echo "ERROR: Environment variable DEBUG_FILES=$DEBUG_FILES points to a directory that does not exist"
	exit 1
fi

if [ -z "$TEST" ] ; then
	echo "ERROR: Environment variable 'TEST' is not set but expected by test_controller.sh"
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

# =============================================================================
# Step 1: Compile Flink (again)
# =============================================================================

WATCHDOG_CALLBACK_ON_TIMEOUT="print_stacktraces | tee ${DEBUG_FILES}/jps-traces.out"

LOG4J_PROPERTIES=${HERE}/log4j-ci.properties
MVN_LOGGING_OPTIONS="-Dlog.dir=${DEBUG_FILES} -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES"
# Maven command to run. We set the forkCount manually, because otherwise Maven sees too many cores
# on some CI environments. Set forkCountTestPackage to 1 for container-based environment (4 GiB memory)
# and 2 for sudo-enabled environment (7.5 GiB memory).
MVN_COMMON_OPTIONS="-Dflink.forkCount=2 -Dflink.forkCountTestPackage=2 -Dfast -Pskip-webui-build $MVN_LOGGING_OPTIONS"
MVN_COMPILE_OPTIONS="-DskipTests"
MVN_COMPILE_MODULES=$(get_compile_modules_for_stage ${TEST})

run_with_watchdog "run_mvn $MVN_COMMON_OPTIONS $MVN_COMPILE_OPTIONS $PROFILE $MVN_COMPILE_MODULES install"
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

if [ $TEST == $STAGE_PYTHON ]; then
	run_with_watchdog "./flink-python/dev/lint-python.sh"
	EXIT_CODE=$?
else
	MVN_TEST_OPTIONS="-Dflink.tests.with-openssl"
	MVN_TEST_MODULES=$(get_test_modules_for_stage ${TEST})

	run_with_watchdog "run_mvn $MVN_COMMON_OPTIONS $MVN_TEST_OPTIONS $PROFILE $MVN_TEST_MODULES verify"
	EXIT_CODE=$?
fi

# =============================================================================
# Step 3: Put extra logs into $DEBUG_FILES
# =============================================================================

# only misc builds flink-yarn-tests
case $TEST in
	(misc)
		put_yarn_logs_to_artifacts
	;;
esac

collect_coredumps `pwd` $DEBUG_FILES

# Exit code for CI build success/failure
exit $EXIT_CODE
