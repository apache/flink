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

# End to end test for streaming examples. It only validates that the job graph can be successfully generated
# and submitted to a standalone session cluster.
# Usage:
# FLINK_DIR=<flink dir> TEST_DATA_DIR-<test data dir> flink-end-to-end-tests/test-scripts/test_streaming_examples.sh

source "$(dirname "$0")"/common.sh

start_cluster
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start

EXIT_CODE=0

function run_example() {
    printf "\n==============================================================================\n"
    printf "Test operation on running $1 example\n"
    printf "==============================================================================\n"
    if [ $EXIT_CODE == 0 ]; then
        TEST_PROGRAM_JAR=$FLINK_DIR/examples/streaming/$1.jar
        RETURN=`$FLINK_DIR/bin/flink run -d $TEST_PROGRAM_JAR --output file:///${TEST_DATA_DIR}/result1`
        EXIT_CODE=$?
        echo "$RETURN"
    fi
}

run_example "Iteration"
run_example "SessionWindowing"
run_example "StateMachineExample"
run_example "TopSpeedWindowing"
run_example "WindowJoin"
run_example "WordCount"

exit $EXIT_CODE
