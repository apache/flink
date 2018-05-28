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

source "$(dirname "$0")"/common.sh

TEST_JAR_JAR=$TEST_INFRA_DIR/../../flink-end-to-end-tests/flink-api-test/target/PeriodicStreamingJob.jar
TEST_PROGRAM_JAR=$TEST_INFRA_DIR/../../flink-end-to-end-tests/flink-rest-api-test/target/RestApiTest.jar

echo "Run Rest-Api-Test Program"

start_cluster

# Start periodic streaming job to test against
$FLINK_DIR/bin/flink run -d $TEST_JAR_JAR -outputPath file://${TEST_DATA_DIR}/out/result

# Wait for the job to come up and checkpoint has been finished.
sleep 3s

# Start the REST API test job and go through all REST APIs
$FLINK_DIR/bin/flink run $TEST_PROGRAM_JAR -savepointPath file://${TEST_DATA_DIR}/out/savepoint
# java -jar $TEST_PROGRAM_JAR -savepointPath file://${TEST_DATA_DIR}/out/savepoint
EXIT_CODE=$?

stop_cluster

if [ $EXIT_CODE == 0 ];
    then
        echo "REST API test passed!";
    else
        echo "REST API test failed: $EXIT_CODE";
        PASS=""
        exit 1
fi
