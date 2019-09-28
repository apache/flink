#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# End to end test for quick starts test.
# Usage:
# FLINK_DIR=<flink dir> flink-end-to-end-tests/test-scripts/test_table_walkthroughs.sh <Type (java or scala)>

source "$(dirname "$0")"/common.sh

TEST_TYPE=$1

mkdir -p "${TEST_DATA_DIR}"
cd "${TEST_DATA_DIR}"

ARTIFACT_ID=flink-walkthrough-table-${TEST_TYPE}
ARTIFACT_VERSION=0.1

mvn archetype:generate                                          \
    -DarchetypeGroupId=org.apache.flink                         \
    -DarchetypeArtifactId=flink-walkthrough-table-${TEST_TYPE}  \
    -DarchetypeVersion=${FLINK_VERSION}                         \
    -DgroupId=org.apache.flink.walkthrough                      \
    -DartifactId=${ARTIFACT_ID}                                 \
    -Dversion=${ARTIFACT_VERSION}                               \
    -Dpackage=org.apache.flink.walkthrough                      \
    -DinteractiveMode=false

cd "${ARTIFACT_ID}"

mvn clean package -nsu > compile-output.txt

if [[ `grep -c "BUILD FAILURE" compile-output.txt` -eq '1' ]]; then
    echo "Failure: The walk-through did not successfully compile"
    cat compile-output.txt
    exit 1
fi

cd target
jar tvf ${ARTIFACT_ID}-${ARTIFACT_VERSION}.jar > contentsInJar.txt

if [[ `grep -c "org/apache/flink/api/java" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/streaming/api" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/streaming/experimental" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/streaming/runtime" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/streaming/util" contentsInJar.txt` -eq '0' ]]; then

    echo "Success: There are no flink core classes are contained in the jar."
else
    echo "Failure: There are flink core classes are contained in the jar."
    exit 1
fi

TEST_PROGRAM_JAR=${TEST_DATA_DIR}/${ARTIFACT_ID}/target/${ARTIFACT_ID}-${ARTIFACT_VERSION}.jar

add_optional_lib "table"

start_cluster

${FLINK_DIR}/bin/flink run "$TEST_PROGRAM_JAR"
