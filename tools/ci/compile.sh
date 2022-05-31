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
MVN_CLEAN_COMPILE_OUT="/tmp/clean_compile.out"

# Deploy into this directory, to run license checks on all jars staged for deployment.
# This helps us ensure that ALL artifacts we deploy to maven central adhere to our license conditions.
MVN_VALIDATION_DIR="/tmp/flink-validation-deployment"

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

run_mvn clean deploy -DaltDeploymentRepository=validation_repository::default::file:$MVN_VALIDATION_DIR -Dflink.convergence.phase=install -Pcheck-convergence \
    -Dmaven.javadoc.skip=true -U -DskipTests -DfailIfNoTests=false -pl flink-yarn-tests -am | tee $MVN_CLEAN_COMPILE_OUT

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

exit $EXIT_CODE

