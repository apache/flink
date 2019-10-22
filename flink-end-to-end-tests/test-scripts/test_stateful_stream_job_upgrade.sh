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

ORIGINAL_DOP="${1:-2}"
NEW_DOP="${2:-4}"

if (( $ORIGINAL_DOP >= $NEW_DOP )); then
  NUM_SLOTS=${ORIGINAL_DOP}
else
  NUM_SLOTS=${NEW_DOP}
fi

set_config_key "taskmanager.numberOfTaskSlots" "${NUM_SLOTS}"
setup_flink_slf4j_metric_reporter
set_config_key "metrics.fetcher.update-interval" "2000"

start_cluster

CHECKPOINT_DIR="file://${TEST_DATA_DIR}/savepoint-e2e-test-chckpt-dir"

TEST_PROGRAM_JAR="${END_TO_END_DIR}/flink-stream-stateful-job-upgrade-test/target/StatefulStreamJobUpgradeTestProgram.jar"

function job() {
    DOP=$1
    CMD="${FLINK_DIR}/bin/flink run -d -p ${DOP} ${TEST_PROGRAM_JAR} \
      --test.semantics exactly-once \
      --environment.parallelism ${DOP} \
      --state_backend.checkpoint_directory ${CHECKPOINT_DIR} \
      --sequence_generator_source.sleep_time 15 \
      --sequence_generator_source.sleep_after_elements 1"
    echo "${CMD}"
}

JOB=$(job ${ORIGINAL_DOP})
ORIGINAL_JOB=$(${JOB} --test.job.variant original \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running ${ORIGINAL_JOB}

wait_oper_metric_num_in_records stateMap2.1 200

# take a savepoint of the state machine job
SAVEPOINT_PATH=$(stop_with_savepoint ${ORIGINAL_JOB} ${TEST_DATA_DIR} \
  | grep "Savepoint completed. Path:" | sed 's/.* //g')

wait_job_terminal_state "${ORIGINAL_JOB}" "FINISHED"

# isolate the path without the scheme ("file:") and do the necessary checks
SAVEPOINT_DIR=${SAVEPOINT_PATH#"file:"}

if [ -z "$SAVEPOINT_DIR" ]; then
  echo "Savepoint location was empty. This may mean that the stop-with-savepoint failed."
  exit 1
elif [ ! -d "$SAVEPOINT_DIR" ]; then
  echo "Savepoint $SAVEPOINT_PATH does not exist."
  exit 1
fi

JOB=$(job ${NEW_DOP})
UPGRADED_JOB=$(${JOB} --test.job.variant upgraded \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running ${UPGRADED_JOB}

wait_oper_metric_num_in_records stateMap3.2 200

# if state is erroneous and the state machine job produces alerting state transitions,
# output would be non-empty and the test will not pass
