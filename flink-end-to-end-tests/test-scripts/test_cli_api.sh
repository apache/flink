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
JOB_INFO_PACT_DATA_SOURCE_REGEX_EXTRACTOR="\"pact\": \"(Data Source)\""
JOB_INFO_PACT_DATA_SINK_REGEX_EXTRACTOR="\"pact\": \"(Data Sink)\""
JOB_LIST_REGEX_EXTRACTOR_BY_STATUS="([0-9,a-f]*) :"

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

function extract_valid_pact_from_job_info_return() {
    PACT_MATCH=0
    if [[ $1 =~ $JOB_INFO_PACT_DATA_SOURCE_REGEX_EXTRACTOR ]];
        then
            PACT_MATCH=$PACT_MATCH
        else
            PACT_MATCH=-1
        fi
    if [[ $1 =~ $JOB_INFO_PACT_DATA_SINK_REGEX_EXTRACTOR ]];
        then
            PACT_MATCH=$PACT_MATCH
        else
            PACT_MATCH=-1
        fi
    echo ${PACT_MATCH}
}

function extract_valid_job_list_by_type_from_job_list_return() {
    JOB_LIST_MATCH=0
    JOB_LIST_REGEX_EXTRACTOR="$JOB_LIST_REGEX_EXTRACTOR_BY_STATUS $2 $3"
    if [[ $1 =~ $JOB_LIST_REGEX_EXTRACTOR ]];
        then
            JOB_LIST_MATCH=$JOB_LIST_MATCH
        else
            JOB_LIST_MATCH=-1
        fi
    echo ${JOB_LIST_MATCH}
}

function extract_task_manager_slot_request_count() {
    COUNT=`grep "Receive slot request" $FLINK_DIR/log/*taskexecutor*.log | wc -l`
    echo $COUNT
}

function cleanup_cli_test() {
  $FLINK_DIR/bin/taskmanager.sh stop-all

  cleanup
}

if [ $EXIT_CODE == 0 ]; then
    printf "\n==============================================================================\n"
    printf "Test default job launch with non-detach mode\n"
    printf "==============================================================================\n"
    eval "$FLINK_DIR/bin/flink run $FLINK_DIR/examples/batch/WordCount.jar"
    EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
    printf "\n==============================================================================\n"
    printf "Test job launch with complex parameter set\n"
    printf "==============================================================================\n"
    eval "$FLINK_DIR/bin/flink run -m localhost:8081 -p 4 -q -d \
      -c org.apache.flink.examples.java.wordcount.WordCount \
      $FLINK_DIR/examples/batch/WordCount.jar \
      --input file:///$FLINK_DIR/README.txt \
      --output file:///${TEST_DATA_DIR}/out/result"
    EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
    printf "\n==============================================================================\n"
    printf "Validate job launch parallelism configuration\n"
    printf "==============================================================================\n"
    RECEIVED_TASKMGR_REQUEST=`extract_task_manager_slot_request_count`
    # expected 1 from default launch and 4 from complex parameter set.
    if [[ $RECEIVED_TASKMGR_REQUEST == 5 ]]; then
        EXIT_CODE=0
    else
        EXIT_CODE=-1
    fi
fi

printf "\n==============================================================================\n"
printf "Test information APIs\n"
printf "==============================================================================\n"
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink info $FLINK_DIR/examples/batch/WordCount.jar`
    echo "job info returns: $RETURN"
    PACT_MATCH=`extract_valid_pact_from_job_info_return "$RETURN"`
    echo "job info regex match: $PACT_MATCH"
    if [[ $PACT_MATCH == -1 ]]; then # expect at least a Data Source and a Data Sink pact match
        EXIT_CODE=-1
    else
        EXIT_CODE=0
    fi
fi

printf "\n==============================================================================\n"
printf "Test operation on running streaming jobs\n"
printf "==============================================================================\n"
JOB_ID=""
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink run -d \
        $PERIODIC_JOB_JAR --outputPath file:///${TEST_DATA_DIR}/out/result`
    echo "job submission returns: $RETURN"
    JOB_ID=`extract_job_id_from_job_submission_return "$RETURN"`
    EXIT_CODE=$? # expect matching job id extraction
fi

printf "\n==============================================================================\n"
printf "Test list API on a streaming job \n"
printf "==============================================================================\n"
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink list -a`
    echo "job list all returns: $RETURN"
    JOB_LIST_MATCH=`extract_valid_job_list_by_type_from_job_list_return "$RETURN" "Flink Streaming Job" ""`
    echo "job list all regex match: $JOB_LIST_MATCH"
    if [[ $JOB_LIST_MATCH == -1 ]]; then # expect match for all job
        EXIT_CODE=-1
    else
        EXIT_CODE=0
    fi
fi
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink list -r`
    echo "job list running returns: $RETURN"
    JOB_LIST_MATCH=`extract_valid_job_list_by_type_from_job_list_return "$RETURN" "Flink Streaming Job" "\(RUNNING\)"`
    echo "job list running regex match: $JOB_LIST_MATCH"
    if [[ $JOB_LIST_MATCH == -1 ]]; then # expect match for running job
        EXIT_CODE=-1
    else
        EXIT_CODE=0
    fi
fi
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink list -s`
    echo "job list scheduled returns: $RETURN"
    JOB_LIST_MATCH=`extract_valid_job_list_by_type_from_job_list_return "$RETURN" "Flink Streaming Job" "\(CREATED\)"`
    echo "job list scheduled regex match: $JOB_LIST_MATCH"
    if [[ $JOB_LIST_MATCH == -1 ]]; then # expect no match for scheduled job
        EXIT_CODE=0
    else
        EXIT_CODE=-1
    fi
fi

printf "\n==============================================================================\n"
printf "Test canceling a running streaming jobs\n"
printf "==============================================================================\n"
if [ $EXIT_CODE == 0 ]; then
    eval "$FLINK_DIR/bin/flink cancel ${JOB_ID}"
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
