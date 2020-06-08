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

if [ -z $1 ] || [ -z $2 ]; then
 echo "Usage: ./test_resume_externalized_checkpoints.sh <original_dop> <new_dop> <state_backend_setting> <state_backend_file_async_setting> <state_backend_rocks_incremental_setting>"
 exit 1
fi

source "$(dirname "$0")"/common.sh

function run_resume_externalized_checkpoints() {
  ORIGINAL_DOP=$1
  NEW_DOP=$2
  STATE_BACKEND_TYPE=$3
  STATE_BACKEND_FILE_ASYNC=$4
  STATE_BACKEND_ROCKS_INCREMENTAL=$5
  SIMULATE_FAILURE=$6

  if (( $ORIGINAL_DOP >= $NEW_DOP )); then
   NUM_SLOTS=$ORIGINAL_DOP
  else
   NUM_SLOTS=$NEW_DOP
  fi

  set_config_key "taskmanager.numberOfTaskSlots" "${NUM_SLOTS}"
  set_config_key "metrics.fetcher.update-interval" "2000"
  setup_flink_slf4j_metric_reporter
  start_cluster

  CHECKPOINT_DIR="$TEST_DATA_DIR/externalized-chckpt-e2e-backend-dir"
  CHECKPOINT_DIR_URI="file://$CHECKPOINT_DIR"

  # run the DataStream allroundjob

  echo "Running externalized checkpoints test, \
  with ORIGINAL_DOP=$ORIGINAL_DOP NEW_DOP=$NEW_DOP \
  and STATE_BACKEND_TYPE=$STATE_BACKEND_TYPE STATE_BACKEND_FILE_ASYNC=$STATE_BACKEND_FILE_ASYNC \
  STATE_BACKEND_ROCKSDB_INCREMENTAL=$STATE_BACKEND_ROCKS_INCREMENTAL SIMULATE_FAILURE=$SIMULATE_FAILURE ..."

  TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-datastream-allround-test/target/DataStreamAllroundTestProgram.jar

  function buildBaseJobCmd {
    local dop=$1
    local flink_args="$2"

    echo "$FLINK_DIR/bin/flink run -d ${flink_args} -p $dop $TEST_PROGRAM_JAR \
      --test.semantics exactly-once \
      --environment.parallelism $dop \
      --environment.externalize_checkpoint true \
      --environment.externalize_checkpoint.cleanup retain \
      --state_backend $STATE_BACKEND_TYPE \
      --state_backend.checkpoint_directory $CHECKPOINT_DIR_URI \
      --state_backend.file.async $STATE_BACKEND_FILE_ASYNC \
      --state_backend.rocks.incremental $STATE_BACKEND_ROCKS_INCREMENTAL \
      --sequence_generator_source.sleep_time 15 \
      --sequence_generator_source.sleep_after_elements 1"
  }

  BASE_JOB_CMD=`buildBaseJobCmd $ORIGINAL_DOP`

  JOB_CMD=""
  if [[ $SIMULATE_FAILURE == "true" ]]; then
    # the submitted job should fail after at least 1 complete checkpoint.
    # When simulating failures with the general purpose DataStream job,
    # we disable restarting because we want to manually do that after the job fails.
    JOB_CMD="$BASE_JOB_CMD \
      --test.simulate_failure true \
      --test.simulate_failure.num_records 200 \
      --test.simulate_failure.num_checkpoints 1 \
      --test.simulate_failure.max_failures 1 \
      --environment.restart_strategy no_restart"
  else
    JOB_CMD=$BASE_JOB_CMD
  fi

  DATASTREAM_JOB=$($JOB_CMD | grep "Job has been submitted with JobID" | sed 's/.* //g')

  if [[ $SIMULATE_FAILURE == "true" ]]; then
    wait_job_terminal_state $DATASTREAM_JOB FAILED
  else
    wait_job_running $DATASTREAM_JOB
    wait_num_checkpoints $DATASTREAM_JOB 1
    wait_oper_metric_num_in_records SemanticsCheckMapper.0 200

    cancel_job $DATASTREAM_JOB
  fi

  # take the latest checkpoint
  CHECKPOINT_PATH=$(find_latest_completed_checkpoint ${CHECKPOINT_DIR}/${DATASTREAM_JOB})

  if [ -z $CHECKPOINT_PATH ]; then
    echo "Expected an externalized checkpoint to be present, but none exists."
    exit 1
  fi

  echo "Restoring job with externalized checkpoint at $CHECKPOINT_PATH ..."

  BASE_JOB_CMD=`buildBaseJobCmd $NEW_DOP "-s file://${CHECKPOINT_PATH}"`
  JOB_CMD=""
  if [[ $SIMULATE_FAILURE == "true" ]]; then
    JOB_CMD="$BASE_JOB_CMD \
      --test.simulate_failure true \
      --test.simulate_failure.num_records 0 \
      --test.simulate_failure.num_checkpoints 0 \
      --test.simulate_failure.max_failures 0 \
      --environment.restart_strategy no_restart"
  else
    JOB_CMD=$BASE_JOB_CMD
  fi

  DATASTREAM_JOB=$($JOB_CMD | grep "Job has been submitted with JobID" | sed 's/.* //g')

  if [ -z $DATASTREAM_JOB ]; then
      echo "Resuming from externalized checkpoint job could not be started."
      exit 1
  fi

  wait_job_running $DATASTREAM_JOB
  wait_oper_metric_num_in_records SemanticsCheckMapper.0 200

  # if state is errorneous and the general purpose DataStream job produces alerting messages,
  # output would be non-empty and the test will not pass
}

ORIGINAL_DOP=$1
NEW_DOP=$2
STATE_BACKEND_TYPE=${3:-file}
STATE_BACKEND_FILE_ASYNC=${4:-true}
STATE_BACKEND_ROCKS_INCREMENTAL=${5:-false}
SIMULATE_FAILURE=${6:-false}

run_test_with_timeout 900 run_resume_externalized_checkpoints ${ORIGINAL_DOP} ${NEW_DOP} ${STATE_BACKEND_TYPE} ${STATE_BACKEND_FILE_ASYNC} ${STATE_BACKEND_ROCKS_INCREMENTAL} ${SIMULATE_FAILURE}
