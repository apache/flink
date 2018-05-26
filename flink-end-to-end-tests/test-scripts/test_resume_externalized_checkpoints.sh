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

STATE_BACKEND_TYPE=${1:-file}
STATE_BACKEND_FILE_ASYNC=${2:-true}
SIMULATE_FAILURE=${3:-false}

backup_config
setup_flink_slf4j_metric_reporter
start_cluster

function test_cleanup {
  # don't call ourselves again for another signal interruption
  trap "exit -1" INT
  # don't call ourselves again for normal exit
  trap "" EXIT

  rollback_flink_slf4j_metric_reporter

  # make sure to run regular cleanup as well
  cleanup
}
trap test_cleanup INT
trap test_cleanup EXIT

CHECKPOINT_DIR="$TEST_DATA_DIR/externalized-chckpt-e2e-backend-dir"
CHECKPOINT_DIR_URI="file://$CHECKPOINT_DIR"

# run the DataStream allroundjob

echo "Running externalized checkpoints test, with STATE_BACKEND_TYPE=$STATE_BACKEND_TYPE STATE_BACKEND_FILE_ASYNC=$STATE_BACKEND_FILE_ASYNC SIMULATE_FAILURE=$SIMULATE_FAILURE ..."

TEST_PROGRAM_JAR=$TEST_INFRA_DIR/../../flink-end-to-end-tests/flink-datastream-allround-test/target/DataStreamAllroundTestProgram.jar
BASE_JOB_CMD="$FLINK_DIR/bin/flink run -d $TEST_PROGRAM_JAR \
  --test.semantics exactly-once \
  --environment.externalize_checkpoint true \
  --environment.externalize_checkpoint.cleanup retain \
  --state_backend $STATE_BACKEND_TYPE \
  --state_backend.checkpoint_directory $CHECKPOINT_DIR_URI \
  --state_backend.file.async $STATE_BACKEND_FILE_ASYNC \
  --sequence_generator_source.sleep_time 15 \
  --sequence_generator_source.sleep_after_elements 1"

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

wait_job_running $DATASTREAM_JOB

if [[ $SIMULATE_FAILURE == "true" ]]; then
  wait_job_terminal_state $DATASTREAM_JOB FAILED
else
  wait_num_checkpoints $DATASTREAM_JOB 1
  wait_oper_metric_num_in_records ArtificalKeyedStateMapper.0 200

  cancel_job $DATASTREAM_JOB
fi

CHECKPOINT_PATH=$(ls -d $CHECKPOINT_DIR/$DATASTREAM_JOB/chk-[1-9]*)

if [ -z $CHECKPOINT_PATH ]; then
  echo "Expected an externalized checkpoint to be present, but none exists."
  PASS=""
  exit 1
fi

NUM_CHECKPOINTS=$(echo $CHECKPOINT_PATH | wc -l | tr -d ' ')
if (( $NUM_CHECKPOINTS > 1 )); then
  echo "Expected only exactly 1 externalized checkpoint to be present, but $NUM_CHECKPOINTS exists."
  PASS=""
  exit 1
fi

echo "Restoring job with externalized checkpoint at $CHECKPOINT_PATH ..."
DATASTREAM_JOB=$($BASE_JOB_CMD | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running $DATASTREAM_JOB
wait_oper_metric_num_in_records ArtificalKeyedStateMapper.0 200

# if state is errorneous and the general purpose DataStream job produces alerting messages,
# output would be non-empty and the test will not pass
