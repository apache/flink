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

MVN_CLEAN_COMPILE_OUT=$1
CI_DIR=$2
FLINK_ROOT=$3
FLINK_DEPLOYED_ROOT=$4

source "${CI_DIR}/maven-utils.sh"

cd $CI_DIR/java-ci-tools/

run_mvn exec:java -Dexec.mainClass=org.apache.flink.tools.ci.licensecheck.LicenseChecker -Dexec.args=\"$MVN_CLEAN_COMPILE_OUT $FLINK_ROOT $FLINK_DEPLOYED_ROOT\"
EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
    echo "=============================================================================="
    echo "License Check failed. See previous output for details."
    echo "=============================================================================="
    exit $EXIT_CODE
fi

exit 0