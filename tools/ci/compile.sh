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

echo "Maven version:"
$MVN -version

echo "=============================================================================="
echo "Compiling Flink"
echo "=============================================================================="

EXIT_CODE=0

# Maven QA-check args: dependency convergence, the deploy that stages jars for the license check, and the shade DEBUG log the bundled-optional check parses. Default runs full QA; the fast compile job and e2e pass MVN_LICENSE_CHECK_ARGS="" to build only (QA runs in the license_check job). Non-empty also enables the structural QA + license blocks below.
MVN_LICENSE_CHECK_ARGS="${MVN_LICENSE_CHECK_ARGS-deploy -DaltDeploymentRepository=validation_repository::default::file:$MVN_VALIDATION_DIR -Dflink.convergence.phase=install -Pcheck-convergence -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugins.shade=DEBUG}"
if [[ -n "$MVN_LICENSE_CHECK_ARGS" ]]; then
    BUILD_GOAL_ARGS="$MVN_LICENSE_CHECK_ARGS"
    MVN_COMPILE_THREADS="${MVN_COMPILE_THREADS:-1}"
else
    BUILD_GOAL_ARGS="install"
    MVN_COMPILE_THREADS="${MVN_COMPILE_THREADS:-1C}"
fi

# Default -T1 because our output parsers (bundled-optional/shade) don't support multi-threaded builds; the fast compile job sets MVN_COMPILE_THREADS=1C together with MVN_LICENSE_CHECK_ARGS="" so those parsing checks are skipped.
$MVN clean ${BUILD_GOAL_ARGS} \
    -Dmaven.javadoc.skip=true -U -DskipTests "${@}" -T${MVN_COMPILE_THREADS} | tee $MVN_CLEAN_COMPILE_OUT

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

# Structural QA (javadoc/scala/shaded/bundled) runs only in QA mode (MVN_LICENSE_CHECK_ARGS non-empty); the fast compile job and e2e skip it.
if [[ -n "$MVN_LICENSE_CHECK_ARGS" ]]; then

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

echo "============ Checking bundled dependencies marked as optional ============"

MVN=$MVN ${CI_DIR}/verify_bundled_optional.sh $MVN_CLEAN_COMPILE_OUT || exit $?

echo "============ Checking scala suffixes ============"

MVN=$MVN ${CI_DIR}/verify_scala_suffixes.sh || exit $?

echo "============ Checking shaded dependencies ============"

check_shaded_artifacts
EXIT_CODE=$(($EXIT_CODE+$?))
check_shaded_artifacts_s3_fs hadoop
EXIT_CODE=$(($EXIT_CODE+$?))
check_shaded_artifacts_s3_fs presto
EXIT_CODE=$(($EXIT_CODE+$?))

fi

# License check needs the staged deploy dir; runs only in QA mode (MVN_LICENSE_CHECK_ARGS non-empty).
if [[ -n "$MVN_LICENSE_CHECK_ARGS" ]]; then

echo "============ Run license check ============"

find $MVN_VALIDATION_DIR
MVN=$MVN ${CI_DIR}/license_check.sh $MVN_CLEAN_COMPILE_OUT $MVN_VALIDATION_DIR || exit $?

fi

exit $EXIT_CODE

