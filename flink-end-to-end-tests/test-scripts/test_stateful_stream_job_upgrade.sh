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

backup_config
change_conf "taskmanager.numberOfTaskSlots" "1" "${NUM_SLOTS}"
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

  # make sure to run regular cleanup as well
  cleanup
}
trap test_cleanup INT
trap test_cleanup EXIT

CHECKPOINT_DIR="file://${TEST_DATA_DIR}/savepoint-e2e-test-chckpt-dir"

TEST_PROGRAM_JAR="${TEST_INFRA_DIR}/../../flink-end-to-end-tests/flink-stream-stateful-job-upgrade-test/target/StatefulStreamJobUpgradeTestProgram.jar"

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
SAVEPOINT_PATH=$(take_savepoint ${ORIGINAL_JOB} ${TEST_DATA_DIR} \
  | grep "Savepoint completed. Path:" | sed 's/.* //g')

cancel_job ${ORIGINAL_JOB}

JOB=$(job ${NEW_DOP})
UPGRADED_JOB=$(${JOB} --test.job.variant upgraded \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running ${UPGRADED_JOB}

wait_oper_metric_num_in_records stateMap3.2 200

# if state is errorneous and the state machine job produces alerting state transitions,
# output would be non-empty and the test will not pass
