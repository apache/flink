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

OUT_TYPE="${1:-local}" # other type: s3

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/common_s3.sh

s3_setup hadoop
set_conf_ssl "mutual"

OUT=temp/test_streaming_file_sink-$(uuidgen)
OUTPUT_PATH="$TEST_DATA_DIR/$OUT"
S3_OUTPUT_PATH="s3://$IT_CASE_S3_BUCKET/$OUT"

mkdir -p $OUTPUT_PATH

if [ "${OUT_TYPE}" == "local" ]; then
  echo "Use local output"
  JOB_OUTPUT_PATH=${OUTPUT_PATH}
elif [ "${OUT_TYPE}" == "s3" ]; then
  echo "Use s3 output"
  JOB_OUTPUT_PATH=${S3_OUTPUT_PATH}
else
  echo "Unknown output type: ${OUT_TYPE}"
  exit 1
fi

# make sure we delete the file at the end
function out_cleanup {
  s3_delete_by_full_path_prefix $OUT
}
if [ "${OUT_TYPE}" == "s3" ]; then
  trap out_cleanup EXIT
fi

TEST_PROGRAM_JAR="${END_TO_END_DIR}/flink-streaming-file-sink-test/target/StreamingFileSinkProgram.jar"

###################################
# Get all lines in part files and sort them numerically.
#
# Globals:
#   OUTPUT_PATH
# Arguments:
#   None
# Returns:
#   sorted content of part files
###################################
function get_complete_result {
  if [ "${OUT_TYPE}" == "s3" ]; then
    rm -rf $OUTPUT_PATH; mkdir -p $OUTPUT_PATH
    s3_get_by_full_path_and_filename_prefix ${TEST_DATA_DIR} "${OUT}" "part-"
  fi
  find "${OUTPUT_PATH}" -type f \( -iname "part-*" \) -exec cat {} + | sort -g
}

###################################
# Get total number of lines in part files.
#
# Globals:
#   OUT
# Arguments:
#   None
# Returns:
#   line number in part files
###################################
function get_total_number_of_valid_lines {
  if [ "${OUT_TYPE}" == "local" ]; then
    get_complete_result | wc -l | tr -d '[:space:]'
  elif [ "${OUT_TYPE}" == "s3" ]; then
    s3_get_number_of_lines_by_prefix "${OUT}" "part-"
  fi
}

###################################
# Waits until a number of values have been written within a timeout.
# If the timeout expires, exit with return code 1.
#
# Globals:
#   None
# Arguments:
#   $1: the number of expected values
#   $2: timeout in seconds
# Returns:
#   None
###################################
function wait_for_complete_result {
    local expected_number_of_values=$1
    local polling_timeout=$2
    local polling_interval=1
    local seconds_elapsed=0

    local number_of_values=0
    local previous_number_of_values=-1

    while [[ ${number_of_values} -lt ${expected_number_of_values} ]]; do
        if [[ ${seconds_elapsed} -ge ${polling_timeout} ]]; then
            echo "Did not produce expected number of values within ${polling_timeout}s"
            exit 1
        fi

        sleep ${polling_interval}
        ((seconds_elapsed += ${polling_interval}))

        number_of_values=$(get_total_number_of_valid_lines)
        if [[ ${previous_number_of_values} -ne ${number_of_values} ]]; then
            echo "Number of produced values ${number_of_values}/${expected_number_of_values}"
            previous_number_of_values=${number_of_values}
        fi
    done
}

start_cluster

"${FLINK_DIR}/bin/taskmanager.sh" start
"${FLINK_DIR}/bin/taskmanager.sh" start
"${FLINK_DIR}/bin/taskmanager.sh" start

echo "Submitting job."
CLIENT_OUTPUT=$("$FLINK_DIR/bin/flink" run -d "${TEST_PROGRAM_JAR}" --outputPath "${JOB_OUTPUT_PATH}")
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

wait_for_restart_to_complete 0 ${JOB_ID}

echo "Killing 2 TMs"
kill_random_taskmanager
kill_random_taskmanager

echo "Starting 2 TMs"
"$FLINK_DIR/bin/taskmanager.sh" start
"$FLINK_DIR/bin/taskmanager.sh" start

wait_for_restart_to_complete 1 ${JOB_ID}

echo "Waiting until all values have been produced"
wait_for_complete_result 60000 300

cancel_job "${JOB_ID}"

wait_job_terminal_state "${JOB_ID}" "CANCELED"

get_complete_result > "${TEST_DATA_DIR}/complete_result"

check_result_hash "File Streaming Sink" "$TEST_DATA_DIR/complete_result" "6727342fdd3aae2129e61fc8f433fb6f"
