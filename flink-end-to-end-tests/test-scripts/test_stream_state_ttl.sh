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

STATE_BACKEND_TYPE="${1:-file}"
STATE_BACKEND_FILE_ASYNC="${2:-false}"
TTL="${3:-1000}"
PRECISION="${4:-5}"
PARALLELISM="${5-3}"
UPDATE_NUM="${6-1000}"

CHECKPOINT_DIR="file://$TEST_DATA_DIR/savepoint-e2e-test-chckpt-dir"

TEST=flink-stream-state-ttl-test
TEST_PROGRAM_NAME=DataStreamStateTTLTestProgram
TEST_PROGRAM_JAR=${END_TO_END_DIR}/$TEST/target/$TEST_PROGRAM_NAME.jar

setup_flink_slf4j_metric_reporter
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

start_cluster
start_taskmanagers $PARALLELISM

function job_id() {
    CMD="${FLINK_DIR}/bin/flink run -d -p ${PARALLELISM} ${TEST_PROGRAM_JAR} \
      --test.semantics exactly-once \
      --environment.parallelism ${PARALLELISM} \
      --state_backend ${STATE_BACKEND_TYPE} \
      --state_ttl_verifier.ttl_milli ${TTL} \
      --state_ttl_verifier.precision_milli ${PRECISION} \
      --state_backend.checkpoint_directory ${CHECKPOINT_DIR} \
      --state_backend.file.async ${STATE_BACKEND_FILE_ASYNC} \
      --update_generator_source.sleep_time 10 \
      --update_generator_source.sleep_after_elements 1"
    echo "${CMD}"
}

JOB_CMD=$(job_id)
echo ${JOB_CMD}
JOB=$(${JOB_CMD} | grep 'Job has been submitted with JobID' | sed 's/.* //g')
wait_job_running ${JOB}
wait_oper_metric_num_in_records TtlVerifyUpdateFunction.0 ${UPDATE_NUM} 'State TTL test job'

SAVEPOINT_PATH=$(take_savepoint ${JOB} ${TEST_DATA_DIR} \
  | grep "Savepoint completed. Path:" | sed 's/.* //g')

cancel_job ${JOB}

JOB_CMD=$(job_id)
echo ${JOB_CMD}
JOB=$(${JOB_CMD} | grep 'Job has been submitted with JobID' | sed 's/.* //g')
wait_job_running ${JOB}
wait_oper_metric_num_in_records TtlVerifyUpdateFunction.0 ${UPDATE_NUM} "State TTL test job"

# if verification fails job produces failed TTL'ed state updates,
# output would be non-empty and the test will not pass
