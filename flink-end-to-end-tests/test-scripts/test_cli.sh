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

set -Eeuo pipefail

source "$(dirname "$0")"/common.sh

TEST_PROGRAM_JAR=$END_TO_END_DIR/flink-cli-test/target/PeriodicStreamingJob.jar

start_cluster
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start

# CLI regular expressions
JOB_INFO_PACT_DATA_SOURCE_REGEX_EXTRACTOR="\"pact\": \"(Data Source)\""
JOB_INFO_PACT_DATA_SINK_REGEX_EXTRACTOR="\"pact\": \"(Data Sink)\""
JOB_LIST_REGEX_EXTRACTOR_BY_STATUS="([0-9,a-f]*) :"

EXIT_CODE=0

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
    COUNT=`grep "Receive slot request" $FLINK_LOG_DIR/*taskexecutor*.log | wc -l`
    echo $COUNT
}

printf "\n==============================================================================\n"
printf "Test default job launch with non-detach mode\n"
printf "==============================================================================\n"
RESULT=`$FLINK_DIR/bin/flink run $FLINK_DIR/examples/batch/WordCount.jar`
EXIT_CODE=$?
echo "$RESULT"

if [[ $RESULT != *"(java.util.ArrayList) [170 elements]"* ]];then
    echo "[FAIL] Invalid accumulator result."
    EXIT_CODE=1
fi

if [ $EXIT_CODE == 0 ]; then
    printf "\n==============================================================================\n"
    printf "Test job launch with complex parameter set\n"
    printf "==============================================================================\n"
    eval "$FLINK_DIR/bin/flink run -m localhost:8081 -p 4 \
      -c org.apache.flink.examples.java.wordcount.WordCount \
      $FLINK_DIR/examples/batch/WordCount.jar \
      --input file:///$FLINK_DIR/README.txt \
      --output file:///${TEST_DATA_DIR}/result1"
    EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
    ROW_COUNT=`cat ${TEST_DATA_DIR}/result1/* | wc -l`
    if [ $((ROW_COUNT)) -ne 111 ]; then
        echo "[FAIL] Unexpected number of rows in output."
        echo "Found: $ROW_COUNT"
        EXIT_CODE=1
    fi
fi

if [ $EXIT_CODE == 0 ]; then
    RECEIVED_TASKMGR_REQUEST=`extract_task_manager_slot_request_count`
    # expected 1 from default launch and 4 from complex parameter set.
    if [[ $RECEIVED_TASKMGR_REQUEST != 5 ]]; then
        echo "[FAIL] Unexpected task manager slot count."
        echo "Received slots: $RECEIVED_TASKMGR_REQUEST"
        EXIT_CODE=1
    fi
fi

printf "\n==============================================================================\n"
printf "Test CLI information\n"
printf "==============================================================================\n"
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink info $FLINK_DIR/examples/batch/WordCount.jar`
    echo "$RETURN"
    PACT_MATCH=`extract_valid_pact_from_job_info_return "$RETURN"`
    if [[ $PACT_MATCH == -1 ]]; then # expect at least a Data Source and a Data Sink pact match
        echo "[FAIL] Data source and/or sink are missing."
        EXIT_CODE=1
    fi
fi

printf "\n==============================================================================\n"
printf "Test operation on running streaming jobs\n"
printf "==============================================================================\n"
JOB_ID=""
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink run -d $TEST_PROGRAM_JAR --outputPath file:///${TEST_DATA_DIR}/result2`
    echo "$RETURN"
    JOB_ID=`extract_job_id_from_job_submission_return "$RETURN"`
    EXIT_CODE=$? # expect matching job id extraction
fi

printf "\n==============================================================================\n"
printf "Test list API on a streaming job \n"
printf "==============================================================================\n"
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink list -a`
    echo "$RETURN"
    JOB_LIST_MATCH=`extract_valid_job_list_by_type_from_job_list_return "$RETURN" "Flink Streaming Job" ""`
    if [[ $JOB_LIST_MATCH == -1 ]]; then # expect match for all job
        echo "[FAIL] Unexpected 'Flink Streaming Job' list."
        EXIT_CODE=1
    fi
fi

if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink list -r`
    echo "$RETURN"
    JOB_LIST_MATCH=`extract_valid_job_list_by_type_from_job_list_return "$RETURN" "Flink Streaming Job" "\(RUNNING\)"`
    if [[ $JOB_LIST_MATCH == -1 ]]; then # expect match for running job
        echo "[FAIL] Unexpected 'Flink Streaming Job' 'RUNNING' list."
        EXIT_CODE=1
    fi
fi

if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink list -s`
    echo "$RETURN"
    JOB_LIST_MATCH=`extract_valid_job_list_by_type_from_job_list_return "$RETURN" "Flink Streaming Job" "\(CREATED\)"`
    if [[ $JOB_LIST_MATCH != -1 ]]; then # expect no match for scheduled job
        echo "[FAIL] Unexpected 'Flink Streaming Job' 'CREATED' list."
        EXIT_CODE=1
    fi
fi

printf "\n==============================================================================\n"
printf "Test canceling a running streaming jobs\n"
printf "==============================================================================\n"
if [ $EXIT_CODE == 0 ]; then
    eval "$FLINK_DIR/bin/flink cancel ${JOB_ID}"
    EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink list -a`
    echo "$RETURN"
    JOB_LIST_MATCH=`extract_valid_job_list_by_type_from_job_list_return "$RETURN" "Flink Streaming Job" "\(CANCELED\)"`
    if [[ $JOB_LIST_MATCH == -1 ]]; then # expect match for canceled job
        echo "[FAIL] Unexpected 'Flink Streaming Job' 'CANCELED' list."
        EXIT_CODE=1
    fi
fi

exit $EXIT_CODE
