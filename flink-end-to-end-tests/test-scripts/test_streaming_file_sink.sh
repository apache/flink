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

TEST_PROGRAM_JAR="${END_TO_END_DIR}/flink-streaming-file-sink-test/target/StreamingFileSinkProgram.jar"

OUTPUT_PATH="$TEST_DATA_DIR/out"

function get_num_output_files {
    local num_files=$(find ${OUTPUT_PATH} -type f | wc -l)
    echo ${num_files}
}

function wait_for_restart {
    local base_num_restarts=$1

    local current_num_restarts=${base_num_restarts}
    local expected_num_restarts=$((current_num_restarts + 1))

    echo "Waiting for restart to happen"
    while ! [[ ${current_num_restarts} -eq ${expected_num_restarts} ]]; do
        sleep 5
        current_num_restarts=$(get_job_metric ${JOB_ID} "fullRestarts")
        if [[ -z ${current_num_restarts} ]]; then
            current_num_restarts=${base_num_restarts}
        fi
    done
}

###################################
# Wait a specific number of successful checkpoints
# to have happened
#
# Globals:
#   None
# Arguments:
#   $1: the job id
#   $2: the number of expected successful checkpoints
#   $3: timeout in seconds
# Returns:
#   None
###################################
function wait_for_number_of_checkpoints {
    local job_id=$1
    local expected_num_checkpoints=$2
    local timeout=$3
    local count=0

    echo "Starting to wait for completion of ${expected_num_checkpoints} checkpoints"
    while (($(get_completed_number_of_checkpoints ${job_id}) < ${expected_num_checkpoints})); do

        if [[ ${count} -gt ${timeout} ]]; then
            echo "A timeout occurred waiting for successful checkpoints"
            exit 1
        else
            ((count+=2))
        fi

        local current_num_checkpoints=$(get_completed_number_of_checkpoints ${job_id})
        echo "${current_num_checkpoints}/${expected_num_checkpoints} completed checkpoints"
        sleep 2
    done
}

function get_completed_number_of_checkpoints {
    local job_id=$1
    local json_res=$(curl -s http://localhost:8081/jobs/${job_id}/checkpoints)

    echo ${json_res}    | # {"counts":{"restored":0,"total":25,"in_progress":1,"completed":24,"failed":0} ...
        cut -d ":" -f 6 | # 24,"failed"
        sed 's/,.*//'     # 24
}

start_cluster

"${FLINK_DIR}/bin/taskmanager.sh" start
"${FLINK_DIR}/bin/taskmanager.sh" start
"${FLINK_DIR}/bin/taskmanager.sh" start

echo "Submitting job."
CLIENT_OUTPUT=$("$FLINK_DIR/bin/flink" run -d "${TEST_PROGRAM_JAR}" --outputPath "${OUTPUT_PATH}")
JOB_ID=$(echo "${CLIENT_OUTPUT}" | grep "Job has been submitted with JobID" | sed 's/.* //g')

if [[ -z $JOB_ID ]]; then
    echo "Job could not be submitted."
    echo "${CLIENT_OUTPUT}"
    exit 1
fi

wait_job_running ${JOB_ID}

wait_num_checkpoints "${JOB_ID}" 3

echo "Killing TM"
kill_random_taskmanager

echo "Starting TM"
"$FLINK_DIR/bin/taskmanager.sh" start

wait_for_restart 0

echo "Killing 2 TMs"
kill_random_taskmanager
kill_random_taskmanager

echo "Starting 2 TMs"
"$FLINK_DIR/bin/taskmanager.sh" start
"$FLINK_DIR/bin/taskmanager.sh" start

wait_for_restart 1

echo "Waiting until no new files are being created"
OLD_COUNT=0
NEW_COUNT=$(get_num_output_files)
while ! [[ ${OLD_COUNT} -eq ${NEW_COUNT} ]]; do
    echo "More output files were created. previous=${OLD_COUNT} now=${NEW_COUNT}"
    # so long as there is data to process new files should be created for each checkpoint
    CURRENT_NUM_CHECKPOINTS=$(get_completed_number_of_checkpoints ${JOB_ID})
    EXPECTED_NUM_CHECKPOINTS=$((CURRENT_NUM_CHECKPOINTS + 1))
    wait_for_number_of_checkpoints ${JOB_ID} ${EXPECTED_NUM_CHECKPOINTS} 60

    OLD_COUNT=${NEW_COUNT}
    NEW_COUNT=$(get_num_output_files)
done

cancel_job "${JOB_ID}"

wait_job_terminal_state "${JOB_ID}" "CANCELED"

# get all lines in part files and sort them numerically
find "${OUTPUT_PATH}" -type f \( -iname "part-*" \) -exec cat {} + | sort -g > "${TEST_DATA_DIR}/complete_result"

check_result_hash "File Streaming Sink" "$TEST_DATA_DIR/complete_result" "6727342fdd3aae2129e61fc8f433fb6f"
