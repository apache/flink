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

usage() {
  echo "Usage: $0 <maven-build-output> <deployed-artifacts-folder>"
  echo "    <maven-build-output>                 A file containing the output of the Maven build."
  echo "    <deployed-artifacts-folder>          A directory containing a Maven repository into which the Flink artifacts were deployed."
  echo ""
  echo "Example preparation:"
  echo "    mvnw clean deploy -DaltDeploymentRepository=validation_repository::default::file:<deployed-artifacts-folder> > <maven-build-output>"
  echo ""
  echo "The environment variable MVN is used to specify the Maven binaries; defaults to 'mvnw'."
  echo "See further details in the JavaDoc of LicenseChecker."
}

while getopts 'h' o; do
  case "${o}" in
    h)
      usage
      exit 0
      ;;
  esac
done

if [[ "$#" != "2" ]]; then
  usage
  exit 1
fi

MVN_CLEAN_COMPILE_OUT=$1
FLINK_DEPLOYED_ROOT=$2

MVN=${MVN:-./mvnw}

$MVN -pl tools/ci/flink-ci-tools exec:java -Dexec.mainClass=org.apache.flink.tools.ci.licensecheck.LicenseChecker -Dexec.args="$MVN_CLEAN_COMPILE_OUT $(pwd) $FLINK_DEPLOYED_ROOT"
EXIT_CODE=$?

if [ $EXIT_CODE != 0 ]; then
    echo "=============================================================================="
    echo "License Check failed. See previous output for details."
    echo "=============================================================================="
    exit $EXIT_CODE
fi

exit 0