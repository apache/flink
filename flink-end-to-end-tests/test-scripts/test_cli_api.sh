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

start_cluster
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start

# Test for CLI commands.
# verify only the return code the content correctness of the API results.
PERIODIC_JOB_JAR=$TEST_INFRA_DIR/../../flink-end-to-end-tests/flink-api-test/target/PeriodicStreamingJob.jar
JOB_ID_REGEX_EXTRACTOR=".*JobID ([0-9,a-f]*)"
SAVE_POINT_REGEX_EXTRACTOR=".*Savepoint stored in (.*)\\."

EXIT_CODE=0

function extract_job_id_from_job_submission_return() {
    if [[ $1 =~ $JOB_ID_REGEX_EXTRACTOR ]];
        then
            JOB_ID="${BASH_REMATCH[1]}";
        else
            JOB_ID=""
        fi
    echo "$JOB_ID"
}

function extract_savepoint_path_from_savepoint_return() {
    if [[ $1 =~ $SAVE_POINT_REGEX_EXTRACTOR ]];
        then
            SAVEPOINT_PATH="${BASH_REMATCH[1]}";
        else
            SAVEPOINT_PATH=""
        fi
    echo "$SAVEPOINT_PATH"
}

function cleanup_cli_test() {
  stop_cluster
  $FLINK_DIR/bin/taskmanager.sh stop-all

  cleanup
}

printf "\n==============================================================================\n"
printf "Test default job launch with non-detach mode\n"
printf "==============================================================================\n"
if [ $EXIT_CODE == 0 ]; then
    eval "$FLINK_DIR/bin/flink run $FLINK_DIR/examples/batch/WordCount.jar"
    EXIT_CODE=$?
fi

printf "\n==============================================================================\n"
printf "Test run with complex parameter set\n"
printf "==============================================================================\n"
if [ $EXIT_CODE == 0 ]; then
    eval "$FLINK_DIR/bin/flink run -m localhost:8081 -p 4 -q -d \
      -c org.apache.flink.examples.java.wordcount.WordCount \
      $FLINK_DIR/examples/batch/WordCount.jar \
      --input file:///$FLINK_DIR/README.txt \
      --output file:///${TEST_DATA_DIR}/out/result"
    EXIT_CODE=$?
fi

printf "\n==============================================================================\n"
printf "Test information APIs\n"
printf "==============================================================================\n"
if [ $EXIT_CODE == 0 ]; then
    eval "$FLINK_DIR/bin/flink info $FLINK_DIR/examples/batch/WordCount.jar"
    EXIT_CODE=$?
fi
if [ $EXIT_CODE == 0 ]; then
    eval "$FLINK_DIR/bin/flink list"
    EXIT_CODE=$?
fi
if [ $EXIT_CODE == 0 ]; then
    eval "$FLINK_DIR/bin/flink list -s"
    EXIT_CODE=$?
fi
if [ $EXIT_CODE == 0 ]; then
    eval "$FLINK_DIR/bin/flink list -r"
    EXIT_CODE=$?
fi

printf "\n==============================================================================\n"
printf "Test operation on running streaming jobs\n"
printf "==============================================================================\n"
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink run -d \
        $PERIODIC_JOB_JAR --outputPath file:///${TEST_DATA_DIR}/out/result`
    echo "job submission returns: $RETURN"
    JOB_ID=`extract_job_id_from_job_submission_return "$RETURN"`
    eval "$FLINK_DIR/bin/flink cancel ${JOB_ID}"
    EXIT_CODE=$?
fi

printf "\n==============================================================================\n"
printf "Test savepoint for a running streaming jobs\n"
printf "==============================================================================\n"
SAVEPOINT_JOB_ID=""
SAVEPOINT_PATH=""
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink run -d \
        $PERIODIC_JOB_JAR --outputPath file:///${TEST_DATA_DIR}/out/result`
    echo "job submission returns: $RETURN"
    SAVEPOINT_JOB_ID=`extract_job_id_from_job_submission_return "$RETURN"`
    EXIT_CODE=$?
fi
if [ $EXIT_CODE == 0 ]; then
    eval "$FLINK_DIR/bin/flink savepoint ${SAVEPOINT_JOB_ID} file:///${TEST_DATA_DIR}/savepoint"
    EXIT_CODE=$?
fi
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink cancel -s file:///${TEST_DATA_DIR}/savepoint ${SAVEPOINT_JOB_ID}`
    echo "job savepoint returns: $RETURN"
    SAVEPOINT_PATH=`extract_savepoint_path_from_savepoint_return "$RETURN"`
    EXIT_CODE=$?
fi
if [ $EXIT_CODE == 0 ]; then
    eval "$FLINK_DIR/bin/flink run -s ${SAVEPOINT_PATH} -d \
        ${PERIODIC_JOB_JAR} --outputPath file:///${TEST_DATA_DIR}/out/result"
    EXIT_CODE=$?
fi

printf "\n==============================================================================\n"
printf "Cleaning up... \n"
printf "==============================================================================\n"
trap cleanup_cli_test INT
trap cleanup_cli_test EXIT

if [ $EXIT_CODE == 0 ];
    then
        echo "All CLI test passed!";
    else
        echo "CLI test failed: $EXIT_CODE";
        PASS=""
        exit 1
fi
