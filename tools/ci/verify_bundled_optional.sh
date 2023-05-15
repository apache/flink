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

## Checks that all bundled dependencies are marked as optional in the poms
MVN_CLEAN_COMPILE_OUT=$1
CI_DIR=$2
FLINK_ROOT=$3

source "${CI_DIR}/maven-utils.sh"

cd "$FLINK_ROOT" || exit

dependency_plugin_output=${CI_DIR}/optional_dep.txt

run_mvn dependency:tree -B > "${dependency_plugin_output}"

cat "${dependency_plugin_output}"

cd "${CI_DIR}/flink-ci-tools/" || exit

run_mvn exec:java -Dexec.mainClass=org.apache.flink.tools.ci.optional.ShadeOptionalChecker -Dexec.args=\""${MVN_CLEAN_COMPILE_OUT}" "${dependency_plugin_output}"\"
EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
    echo "=============================================================================="
    echo "Optional Check failed. See previous output for details."
    echo "=============================================================================="
    exit 1
fi

exit 0

