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
STATE_BACKEND_ROCKS_TIMER_SERVICE_TYPE=${5:-heap}

if (( $ORIGINAL_DOP >= $NEW_DOP )); then
  NUM_SLOTS=$ORIGINAL_DOP
else
  NUM_SLOTS=$NEW_DOP
fi

change_conf "taskmanager.numberOfTaskSlots" "1" "${NUM_SLOTS}"

if [ $STATE_BACKEND_ROCKS_TIMER_SERVICE_TYPE == 'rocks' ]; then
  set_conf "state.backend.rocksdb.timer-service.factory" "rocksdb"
fi

setup_flink_slf4j_metric_reporter

start_cluster

# make sure to stop Kafka and ZooKeeper at the end, as well as cleaning up the Flink cluster and our moodifications
function test_cleanup {
  # don't call ourselves again for another signal interruption
  trap "exit -1" INT
  # don't call ourselves again for normal exit
  trap "" EXIT

  # revert our modifications to the Flink distribution
  rm ${FLINK_DIR}/lib/flink-metrics-slf4j-*.jar
}
trap test_cleanup INT
trap test_cleanup EXIT

CHECKPOINT_DIR="file://$TEST_DATA_DIR/savepoint-e2e-test-chckpt-dir"

# run the DataStream allroundjob
TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-datastream-allround-test/target/DataStreamAllroundTestProgram.jar
DATASTREAM_JOB=$($FLINK_DIR/bin/flink run -d -p $ORIGINAL_DOP $TEST_PROGRAM_JAR \
  --test.semantics exactly-once \
  --environment.parallelism $ORIGINAL_DOP \
  --state_backend $STATE_BACKEND_TYPE \
  --state_backend.checkpoint_directory $CHECKPOINT_DIR \
  --state_backend.file.async $STATE_BACKEND_FILE_ASYNC \
  --sequence_generator_source.sleep_time 15 \
  --sequence_generator_source.sleep_after_elements 1 \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running $DATASTREAM_JOB

wait_oper_metric_num_in_records SemanticsCheckMapper.0 200

# take a savepoint of the state machine job
SAVEPOINT_PATH=$(take_savepoint $DATASTREAM_JOB $TEST_DATA_DIR \
  | grep "Savepoint completed. Path:" | sed 's/.* //g')

cancel_job $DATASTREAM_JOB

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
