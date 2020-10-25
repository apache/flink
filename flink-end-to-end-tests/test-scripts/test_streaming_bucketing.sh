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

set_hadoop_classpath

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-bucketing-sink-test/target/BucketingSinkTestProgram.jar
JOB_OUTPUT_DIR=${TEST_DATA_DIR}/out/result
LOG_DIR=${FLINK_DIR}/log

function get_total_number_of_valid_lines {
  find ${TEST_DATA_DIR}/out -type f \( -iname "part-*" \) -exec cat {} + | sort -g | wc -l
}

function wait_for_complete_result {
    local expected_number_of_values=$1
    local polling_timeout=$2
    local polling_interval=5
    local seconds_elapsed=0

    local number_of_values=0
    local previous_number_of_values=-1

    while [[ ${number_of_values} -lt ${expected_number_of_values} ]]; do
        if [[ ${seconds_elapsed} -ge ${polling_timeout} ]]; then
            echo "Did not produce expected number of values within ${polling_timeout}s"
            exit 1
        fi

        truncate_files_with_valid_data

        sleep ${polling_interval}
        ((seconds_elapsed += ${polling_interval}))

        number_of_values=$(get_total_number_of_valid_lines)
        if [[ ${previous_number_of_values} -ne ${number_of_values} ]]; then
            echo "Number of produced values ${number_of_values}/${expected_number_of_values}"
            previous_number_of_values=${number_of_values}
        fi
    done
}

function truncate_files_with_valid_data() {
  # get truncate information
  # e.g. "xxx xxx DEBUG xxx.BucketingSink  - Writing valid-length file for xxx/out/result8/part-0-0 to specify valid length 74994"
  LOG_LINES=$(grep -rnw $LOG_DIR -e 'Writing valid-length file')

  # perform truncate on every line
  echo "Truncating buckets"

  while read -r LOG_LINE; do
    PART=$(echo "$LOG_LINE" | awk '{ print $10 }' FS=" ")
    LENGTH=$(echo "$LOG_LINE" | awk '{ print $15 }' FS=" ")

    if [[ -z "${PART}" ]]; then
        continue
    fi
    re='^[0-9]+$'
    if ! [[ ${LENGTH}  =~ $re ]]; then
        continue
    fi

    dd if=$PART of="$PART.truncated" bs=$LENGTH count=1 >& /dev/null
    rm $PART
    mv "$PART.truncated" $PART
  done <<< "$LOG_LINES"
}

function bucketing_cleanup() {
  stop_cluster
  $FLINK_DIR/bin/taskmanager.sh stop-all
}

# Fix the necessary configuration parameters.

set_conf_ssl
set_config_key "heartbeat.timeout" "20000"

# enable DEBUG logging level for the BucketingSink to retrieve truncate length later
echo "" >> $FLINK_DIR/conf/log4j.properties
echo "logger.bucketingsink.name = org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink" >> $FLINK_DIR/conf/log4j.properties
echo "logger.bucketingsink.level = DEBUG" >> $FLINK_DIR/conf/log4j.properties

# Start the experiment.

start_cluster
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start

wait_for_number_of_running_tms 4

on_exit bucketing_cleanup

JOB_ID=$($FLINK_DIR/bin/flink run -d -p 4 $TEST_PROGRAM_JAR -outputPath $JOB_OUTPUT_DIR \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running ${JOB_ID}

wait_num_checkpoints "${JOB_ID}" 5

echo "Killing 1 TM"
kill_random_taskmanager
wait_for_number_of_running_tms 3

echo "Restarting 1 TM"
$FLINK_DIR/bin/taskmanager.sh start
wait_for_number_of_running_tms 4

echo "Killing 2 TMs"
kill_random_taskmanager
kill_random_taskmanager
wait_for_number_of_running_tms 2

echo "Starting 2 TMs"
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start
wait_for_number_of_running_tms 4

# This guarantees that the initializeState() is called
# before we start counting valid lines.
# In other case we risk of counting invalid lines as valid ones.
wait_job_running ${JOB_ID}

echo "Waiting until all values have been produced"
wait_for_complete_result 60000 900

cancel_job $JOB_ID
wait_job_terminal_state ${JOB_ID} "CANCELED"

echo "Job $JOB_ID was cancelled, time to verify"

# get all lines in part files
find ${TEST_DATA_DIR}/out -type f \( -iname "part-*" \) -exec cat {} + > ${TEST_DATA_DIR}/complete_result

# for debugging purposes
#echo "Checking proper result..."
#for KEY in {0..9}; do
#  for IDX in {0..5999}; do
#    FOUND_LINES=$(grep "($KEY,10,$IDX,Some payload...)" ${TEST_DATA_DIR}/complete_result | wc -l)
#    if [ ${FOUND_LINES} != 1 ] ; then
#      echo "Unexpected count $FOUND_LINES for ($KEY,10,$IDX,Some payload...)"
#      PASS=""
#      exit 1
#    fi
#  done
#done

check_result_hash "Bucketing Sink" $TEST_DATA_DIR/complete_result "01aba5ff77a0ef5e5cf6a727c248bdc3"
