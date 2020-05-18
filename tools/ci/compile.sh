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
# This file contains tooling for compiling Flink
#

HERE="`dirname \"$0\"`"             # relative
HERE="`( cd \"$HERE\" && pwd )`"    # absolutized and normalized
if [ -z "$HERE" ] ; then
    exit 1  # fail
fi
CI_DIR="$HERE/../ci"

# source required ci scripts
source "${CI_DIR}/stage.sh"
source "${CI_DIR}/shade.sh"
source "${CI_DIR}/maven-utils.sh"

echo "Maven version:"
run_mvn -version

echo "=============================================================================="
echo "Compiling Flink"
echo "=============================================================================="

EXIT_CODE=0

run_mvn clean install $MAVEN_OPTS -Dflink.convergence.phase=install -Pcheck-convergence -Dflink.forkCount=2 \
    -Dflink.forkCountTestPackage=2 -Dmaven.javadoc.skip=true -U -DskipTests

EXIT_CODE=$?

if [ $EXIT_CODE == 0 ]; then
    echo "=============================================================================="
    echo "Checking scala suffixes"
    echo "=============================================================================="

    ${CI_DIR}/verify_scala_suffixes.sh "${PROFILE}"
    EXIT_CODE=$?
else
    echo "=============================================================================="
    echo "Previous build failure detected, skipping scala-suffixes check."
    echo "=============================================================================="
fi

if [ $EXIT_CODE == 0 ]; then
    check_shaded_artifacts
    EXIT_CODE=$(($EXIT_CODE+$?))
    check_shaded_artifacts_s3_fs hadoop
    EXIT_CODE=$(($EXIT_CODE+$?))
    check_shaded_artifacts_s3_fs presto
    EXIT_CODE=$(($EXIT_CODE+$?))
    check_shaded_artifacts_connector_elasticsearch 5
    EXIT_CODE=$(($EXIT_CODE+$?))
    check_shaded_artifacts_connector_elasticsearch 6
    EXIT_CODE=$(($EXIT_CODE+$?))
else
    echo "=============================================================================="
    echo "Previous build failure detected, skipping shaded dependency check."
    echo "=============================================================================="
fi

exit $EXIT_CODE

