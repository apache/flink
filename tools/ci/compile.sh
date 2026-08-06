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
# This script compiles Flink and runs all QA checks apart from tests.
#
# This script should not contain any CI-specific logic; put these into compile_ci.sh instead.
#
# Usage: [MVN=/path/to/maven] tools/ci/compile.sh [additional maven args]
# - Use the MVN environment variable to point the script to another maven installation.
# - Any script argument is forwarded to the Flink maven build. Use it to skip/modify parts of the build process.
#
# Tips:
# - '-Pskip-webui-build' skips the WebUI build.
# - '-Dfast' skips Maven QA checks.
# - '-Dmaven.clean.skip' skips recompilation of classes.
# Example: tools/ci/compile.sh -Dmaven.clean.skip -Dfast -Pskip-webui-build, use -Dmaven.clean.skip to avoid recompiling classes.
#
# Warnings:
# - Skipping modules via '-pl [!]<module>' is not recommended because checks may assume/require a full build.
#

CI_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
MVN_CLEAN_COMPILE_OUT="/tmp/clean_compile.out"
MVN=${MVN:-./mvnw}

# Deploy into this directory, to run license checks on all jars staged for deployment.
# This helps us ensure that ALL artifacts we deploy to maven central adhere to our license conditions.
MVN_VALIDATION_DIR="/tmp/flink-validation-deployment"
rm -rf ${MVN_VALIDATION_DIR}

# source required ci scripts
source "${CI_DIR}/stage.sh"
source "${CI_DIR}/shade.sh"

# Sample host load (CPU %steal / disk util) during the build to detect agent contention; killed on exit.
# Written to stdout (the job log) because compile/qa/e2e don't publish a debug-files artifact.
chmod +x "${CI_DIR}/sample_load.sh" 2>/dev/null || true
"${CI_DIR}/sample_load.sh" 5 /dev/stdout &
LOAD_SAMPLER_PID=$!
trap 'kill "$LOAD_SAMPLER_PID" 2>/dev/null' EXIT

echo "Maven version:"
$MVN -version

echo "=============================================================================="
echo "Compiling Flink"
echo "=============================================================================="

EXIT_CODE=0

# QA builds add shade DEBUG output and deploy to a local repo for the license check; other builds build fast and only install
if [[ "${SKIP_QA_CHECKS:-false}" != "true" ]]; then
    BUILD_MODE_ARGS="-Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=DEBUG"
    DEPLOY_ARGS="deploy -DaltDeploymentRepository=validation_repository::default::file:$MVN_VALIDATION_DIR -Dflink.convergence.phase=install -Pcheck-convergence"
else
    BUILD_MODE_ARGS="-Pfast"
    DEPLOY_ARGS="install"
fi
# Only force snapshot updates (-U) unless the global Maven options already disable them (--no-snapshot-updates)
UPDATE_SNAPSHOTS_ARG="-U"
if [[ "${MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR}" == *"--no-snapshot-updates"* ]]; then
    UPDATE_SNAPSHOTS_ARG=""
fi
# The bundled/license QA checks parse single-threaded output (-T1); QA-skipping builds may set MVN_COMPILE_THREADS (e.g. 1C)
$MVN clean ${DEPLOY_ARGS} \
    -Dmaven.javadoc.skip=true ${UPDATE_SNAPSHOTS_ARG} -DskipTests ${BUILD_MODE_ARGS} "${@}" -T${MVN_COMPILE_THREADS:-1} | tee $MVN_CLEAN_COMPILE_OUT

EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE != 0 ]; then
    echo "=============================================================================="
    echo "Compiling Flink failed."
    echo "=============================================================================="

    grep "0 Unknown Licenses" target/rat.txt > /dev/null

    if [ $? != 0 ]; then
        echo "License header check failure detected. Printing first 50 lines for convenience:"
        head -n 50 target/rat.txt
    fi

    exit $EXIT_CODE
fi

# All QA checks run only in the dedicated QA job (and local runs); compile/e2e skip them via SKIP_QA_CHECKS
if [[ "${SKIP_QA_CHECKS:-false}" != "true" ]]; then

echo "============ Checking Javadocs ============"

javadoc_output=/tmp/javadoc.out

# use the same invocation as .github/workflows/docs.sh
$MVN javadoc:aggregate -DadditionalJOption='-Xdoclint:none' \
      -Dmaven.javadoc.failOnError=false -Dcheckstyle.skip=true -Denforcer.skip=true -Dspotless.skip=true -Drat.skip=true \
      -Dheader=someTestHeader -Pskip-webui-build > ${javadoc_output}
EXIT_CODE=$?
if [ $EXIT_CODE != 0 ] ; then
  echo "ERROR in Javadocs. Printing full output:"
  cat ${javadoc_output}
  exit $EXIT_CODE
fi

echo "============ Checking scala suffixes ============"

MVN=$MVN ${CI_DIR}/verify_scala_suffixes.sh || exit $?

echo "============ Checking shaded dependencies ============"

check_shaded_artifacts
EXIT_CODE=$(($EXIT_CODE+$?))
check_shaded_artifacts_s3_fs hadoop
EXIT_CODE=$(($EXIT_CODE+$?))
check_shaded_artifacts_s3_fs presto
EXIT_CODE=$(($EXIT_CODE+$?))

echo "============ Checking bundled dependencies marked as optional ============"

MVN=$MVN ${CI_DIR}/verify_bundled_optional.sh $MVN_CLEAN_COMPILE_OUT || exit $?

echo "============ Run license check ============"

find $MVN_VALIDATION_DIR
MVN=$MVN ${CI_DIR}/license_check.sh $MVN_CLEAN_COMPILE_OUT $MVN_VALIDATION_DIR || exit $?

fi

exit $EXIT_CODE

