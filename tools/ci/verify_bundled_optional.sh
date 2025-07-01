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

usage() {
  echo "Usage: $0 <maven-build-output>"
  echo "    <maven-build-output>                 A file containing the output of the Maven build."
  echo ""
  echo "mvnw clean package > <maven-build-output>"
  echo ""
  echo "The environment variable MVN is used to specify the Maven binaries; defaults to 'mvnw'."
  echo "See further details in the JavaDoc of ShadeOptionalChecker."
}

while getopts 'h' o; do
  case "${o}" in
    h)
      usage
      exit 0
      ;;
  esac
done

if [[ "$#" != "1" ]]; then
  usage
  exit 1
fi

## Checks that all bundled dependencies are marked as optional in the poms
MVN_CLEAN_COMPILE_OUT=$1

MVN=${MVN:-./mvnw}

dependency_plugin_output=/tmp/dependency_tree_optional.txt

# run with -T1 because our maven output parsers don't support multi-threaded builds
$MVN dependency:tree -B -T1 > "${dependency_plugin_output}"
EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
    cat ${dependency_plugin_output}
    echo "=============================================================================="
    echo "Optional Check failed. The dependency tree could not be determined. See previous output for details."
    echo "=============================================================================="
    exit $EXIT_CODE
fi

cat "${dependency_plugin_output}"

$MVN -pl tools/ci/flink-ci-tools exec:java -Dexec.mainClass=org.apache.flink.tools.ci.optional.ShadeOptionalChecker -Dexec.args="${MVN_CLEAN_COMPILE_OUT} ${dependency_plugin_output}"
EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
    echo "=============================================================================="
    echo "Optional Check failed. See previous output for details."
    echo "=============================================================================="
    exit $EXIT_CODE
fi

exit 0

