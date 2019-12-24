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

set -o pipefail

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/common_mesos_docker.sh

DURATION=10
OUTPUT_LOCATION="${TEST_DATA_DIR}/out/tmp_out"
TEST_PROGRAM_JAR=$END_TO_END_DIR/flink-cli-test/target/PeriodicStreamingJob.jar

mkdir -p "${TEST_DATA_DIR}"

set_config_key "taskmanager.memory.managed.size" "0"
set_config_key "state.backend" "jobmanager"

start_flink_cluster_with_mesos

JOB_ID=$(docker exec -it mesos-master bash -c "${FLINK_DIR}/bin/flink run -d -p 1 ${TEST_PROGRAM_JAR} --durationInSecond ${DURATION} --outputPath ${OUTPUT_LOCATION}" \
        | grep "Job has been submitted with JobID" | sed 's/.* //g' | tr -d '\r')

wait_num_of_occurence_in_logs_mesos "switched from DEPLOYING to RUNNING" 1

wait_job_terminal_state_mesos "${JOB_ID}" "FINISHED"
