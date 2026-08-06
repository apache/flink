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
if [ -z "${DEBUG_FILES_OUTPUT_DIR:-}" ] ; then
	echo "ERROR: Environment variable 'DEBUG_FILES_OUTPUT_DIR' is not set but expected by test_controller.sh. Tests may use this location to store debugging files."
	exit 1
fi

if [ ! -d "$DEBUG_FILES_OUTPUT_DIR" ] ; then
	echo "ERROR: Environment variable DEBUG_FILES_OUTPUT_DIR=$DEBUG_FILES_OUTPUT_DIR points to a directory that does not exist"
	exit 1
fi

if [ -z "${STAGE:-}" ] ; then
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

# Sample host load (CPU %steal, iowait, disk util) during the whole build+test, to measure whether the
# agent is contended. Output goes to the debug-files artifact; the sampler is killed when this exits.
chmod +x "${HERE}/sample_load.sh" 2>/dev/null || true
"${HERE}/sample_load.sh" 5 "$DEBUG_FILES_OUTPUT_DIR/load-sample.log" &
LOAD_SAMPLER_PID=$!
trap 'kill "$LOAD_SAMPLER_PID" 2>/dev/null' EXIT

# =============================================================================
# Step 1: Rebuild jars and install Flink to local maven repository
# =============================================================================

export LOG4J_PROPERTIES=${HERE}/log4j.properties
MVN_LOGGING_OPTIONS="-Dlog.dir=${DEBUG_FILES_OUTPUT_DIR} -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES"

MVN_COMMON_OPTIONS="-Dfast -Pskip-webui-build $MVN_LOGGING_OPTIONS"
CALLBACK_ON_TIMEOUT="print_stacktraces | tee ${DEBUG_FILES_OUTPUT_DIR}/jps-traces.out"

# Reuse the jars the compile job handed off (REUSE_INSTALLED_ARTIFACTS + jars actually present) and skip
# the rebuild. Only stages that resolve everything from .m2 can do this; stages needing build outputs
# under module target/ dirs (e.g. python's test classpath, the assembled dist) must do a full rebuild.
# Fall back to a full rebuild if no jars are present (e.g. an isolated retry after the artifact expired).
FLINK_INSTALLED_JARS_DIR="${MAVEN_CACHE_FOLDER:-$HOME/.m2/repository}/org/apache/flink"
if [[ "${REUSE_INSTALLED_ARTIFACTS:-false}" == "true" && -d "$FLINK_INSTALLED_JARS_DIR" && -n "$(ls -A "$FLINK_INSTALLED_JARS_DIR" 2>/dev/null)" ]]; then
	echo "Reusing jars installed by the compile job; skipping rebuild/install."
	# relink build-target when a prebuilt distribution was handed off (e.g. the python stage)
	if [ ! -e build-target ]; then
		dist_dir=$(ls -d flink-dist/target/flink-*-bin/flink-* 2>/dev/null | head -n 1)
		if [ -n "$dist_dir" ]; then
			ln -sfn "$dist_dir" build-target
			echo "Linked build-target -> $dist_dir"
		fi
	fi
	EXIT_CODE=0
else
	if [[ "${REUSE_INSTALLED_ARTIFACTS:-false}" == "true" ]]; then
		echo "REUSE_INSTALLED_ARTIFACTS set but no handed-off jars in ${FLINK_INSTALLED_JARS_DIR}; doing a full rebuild."
	fi
	MVN_COMPILE_OPTIONS="-DskipTests"
	MVN_COMPILE_MODULES=$(get_compile_modules_for_stage ${STAGE})
	# the install step is a -DskipTests build (no test-output log parsing here), so it can run multi-threaded
	run_with_watchdog "run_mvn $MVN_COMMON_OPTIONS $MVN_COMPILE_OPTIONS $PROFILE $MVN_COMPILE_MODULES -T${MVN_COMPILE_THREADS:-1C} install" $CALLBACK_ON_TIMEOUT
	EXIT_CODE=$?
fi

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
	MVN_TEST_OPTIONS="-Dflink.tests.with-openssl -Dflink.tests.check-segment-multiple-free -Darchunit.freeze.store.default.allowStoreUpdate=false -Dpekko.rpc.force-invocation-serialization"
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
