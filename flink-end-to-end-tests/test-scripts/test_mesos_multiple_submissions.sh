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

DURATION=5
FIRST_OUTPUT_LOCATION="${TEST_DATA_DIR}/out/first_out"
SECOND_OUTPUT_LOCATION="${TEST_DATA_DIR}/out/second_out"
TEST_PROGRAM_JAR=$END_TO_END_DIR/flink-cli-test/target/PeriodicStreamingJob.jar

function submit_job {
    local output_path=$1
    docker exec --env HADOOP_CLASSPATH=$HADOOP_CLASSPATH mesos-master bash -c "${FLINK_DIR}/bin/flink run -d -p 1 ${TEST_PROGRAM_JAR} --durationInSecond ${DURATION} --outputPath ${output_path}" \
        | grep "Job has been submitted with JobID" | sed 's/.* //g' | tr -d '\r'
}


mkdir -p "${TEST_DATA_DIR}"

# There should be only one TaskManager with one slot; Thus, there are not enough resources to allocate a complete new set of slots for the second job.
# To ensure the old slots are being reused.
set_config_key "mesos.resourcemanager.tasks.cpus" "${MESOS_AGENT_CPU}"

set_hadoop_classpath

start_flink_cluster_with_mesos

JOB1_ID=$(submit_job ${FIRST_OUTPUT_LOCATION})

JOB2_ID=$(submit_job ${SECOND_OUTPUT_LOCATION})

wait_job_terminal_state_mesos "${JOB1_ID}" "FINISHED"
wait_job_terminal_state_mesos "${JOB2_ID}" "FINISHED"
