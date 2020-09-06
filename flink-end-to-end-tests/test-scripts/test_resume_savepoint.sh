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

################################################################################
# This end-to-end test verifies that manually taking a savepoint of a running
# job and resuming from it works properly. It allows resuming the job with
# a different parallelism than the original execution.
#
# Using the general purpose DataStream job, the test covers savepointing and
# resuming when using different state backends (file, RocksDB), as well as the
# following types of states:
#  - Operator re-partitionable list state
#  - Broadcast state
#  - Union state
#  - Keyed state (ValueState)
#
# The general purpose DataStream job is self-verifiable, such that if any
# unexpected error occurs during savepoints or restores, exceptions will be
# thrown; if exactly-once is violated, alerts will be sent to output (and
# caught by the test script to fail the job).
################################################################################

if [ -z $1 ] || [ -z $2 ]; then
  echo "Usage: ./test_resume_savepoint.sh <original_dop> <new_dop> <state_backend_setting> <state_backend_file_async_setting>"
  exit 1
fi

source "$(dirname "$0")"/common.sh


ORIGINAL_DOP=$1
NEW_DOP=$2
STATE_BACKEND_TYPE=${3:-file}
STATE_BACKEND_FILE_ASYNC=${4:-true}
STATE_BACKEND_ROCKS_TIMER_SERVICE_TYPE=${5:-rocks}


run_resume_savepoint_test() {
  if (( $ORIGINAL_DOP >= $NEW_DOP )); then
    NUM_SLOTS=$ORIGINAL_DOP
  else
    NUM_SLOTS=$NEW_DOP
  fi

  set_config_key "taskmanager.numberOfTaskSlots" "${NUM_SLOTS}"

  if [ $STATE_BACKEND_ROCKS_TIMER_SERVICE_TYPE == 'heap' ]; then
    set_config_key "state.backend.rocksdb.timer-service.factory" "heap"
  fi
  set_config_key "metrics.fetcher.update-interval" "2000"

  setup_flink_slf4j_metric_reporter

  start_cluster

  CHECKPOINT_DIR="file://$TEST_DATA_DIR/savepoint-e2e-test-chckpt-dir"

  # run the DataStream allroundjob
  TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-datastream-allround-test/target/DataStreamAllroundTestProgram.jar
  DATASTREAM_JOB=$($FLINK_DIR/bin/flink run -d -p $ORIGINAL_DOP $TEST_PROGRAM_JAR \
    --test.semantics exactly-once \
    --environment.parallelism $ORIGINAL_DOP \
    --state_backend $STATE_BACKEND_TYPE \
    --state_backend.checkpoint_directory $CHECKPOINT_DIR \
    --state_backend.file.async $STATE_BACKEND_FILE_ASYNC \
    --sequence_generator_source.sleep_time 30 \
    --sequence_generator_source.sleep_after_elements 1 \
    | grep "Job has been submitted with JobID" | sed 's/.* //g')

  wait_job_running $DATASTREAM_JOB

  wait_oper_metric_num_in_records SemanticsCheckMapper.0 200

  # take a savepoint of the state machine job
  SAVEPOINT_PATH=$(stop_with_savepoint $DATASTREAM_JOB $TEST_DATA_DIR \
    | grep "Savepoint completed. Path:" | sed 's/.* //g')

  wait_job_terminal_state "${DATASTREAM_JOB}" "FINISHED"

  # isolate the path without the scheme ("file:") and do the necessary checks
  SAVEPOINT_DIR=${SAVEPOINT_PATH#"file:"}

  if [ -z "$SAVEPOINT_DIR" ]; then
    echo "Savepoint location was empty. This may mean that the stop-with-savepoint failed."
    exit 1
  elif [ ! -d "$SAVEPOINT_DIR" ]; then
    echo "Savepoint $SAVEPOINT_PATH does not exist."
    exit 1
  fi

  # Since it is not possible to differentiate reporter output between the first and second execution,
  # we remember the number of metrics sampled in the first execution so that they can be ignored in the following monitorings
  OLD_NUM_METRICS=$(get_num_metric_samples)

  # resume state machine job with savepoint
  DATASTREAM_JOB=$($FLINK_DIR/bin/flink run -s $SAVEPOINT_PATH -p $NEW_DOP -d $TEST_PROGRAM_JAR \
    --test.semantics exactly-once \
    --environment.parallelism $NEW_DOP \
    --state_backend $STATE_BACKEND_TYPE \
    --state_backend.checkpoint_directory $CHECKPOINT_DIR \
    --state_backend.file.async $STATE_BACKEND_FILE_ASYNC \
    --sequence_generator_source.sleep_time 15 \
    --sequence_generator_source.sleep_after_elements 1 \
    | grep "Job has been submitted with JobID" | sed 's/.* //g')

  wait_job_running $DATASTREAM_JOB

  wait_oper_metric_num_in_records SemanticsCheckMapper.0 200

  # if state is errorneous and the state machine job produces alerting state transitions,
  # output would be non-empty and the test will not pass
}

run_test_with_timeout 900 run_resume_savepoint_test
