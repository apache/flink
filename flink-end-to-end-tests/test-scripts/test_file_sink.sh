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
SINK_TO_TEST="${2:-"StreamingFileSink"}"

source "$(dirname "$0")"/common.sh

###################################
# Get all lines in part files and sort them numerically.
#
# Globals:
#   LOCAL_JOB_OUTPUT_PATH
# Arguments:
#   None
# Returns:
#   sorted content of part files
###################################
function get_complete_result {
  find "${LOCAL_JOB_OUTPUT_PATH}" -type f \( -iname "part-*" \) -exec cat {} + | sort -g
}

###################################
# Get total number of lines in part files.
#
# Globals:
#   LOCAL_JOB_OUTPUT_PATH
# Arguments:
#   None
# Returns:
#   line number in part files
###################################
function get_total_number_of_valid_lines {
  get_complete_result | wc -l | tr -d '[:space:]'
}

if [ "${OUT_TYPE}" == "local" ]; then
  echo "[INFO] Test run in local environment: No S3 environment is loaded."
elif [ "${OUT_TYPE}" == "s3" ]; then
  source "$(dirname "$0")"/common_s3_seaweedfs.sh
  s3_setup hadoop

  # overwrites implementation for local runs
  function get_complete_result {
    # copies the data from S3 to the local LOCAL_JOB_OUTPUT_PATH
    s3_get_by_full_path_and_filename_prefix "$LOCAL_JOB_OUTPUT_PATH" "$S3_DATA_PREFIX" true "part-"

    # and prints the sorted output
    find "${LOCAL_JOB_OUTPUT_PATH}" -type f \( -iname "part-*" \) -exec cat {} + | sort -g
  }

  # make sure we delete the data of the current OpenSSL linkage iteration; also registered as a
  # safety net for whichever iteration is active if the script exits unexpectedly
  function out_cleanup {
    s3_delete_by_full_path_prefix "${S3_DATA_PREFIX}"
    s3_delete_by_full_path_prefix "${S3_CHECKPOINT_PREFIX}"
  }
  on_exit out_cleanup
else
  echo "[ERROR] Unknown out type: ${OUT_TYPE}"
  exit 1
fi

set_config_key "metrics.fetcher.update-interval" "2000"
# this test relies on global failovers
set_config_key "jobmanager.execution.failover-strategy" "full"

TEST_PROGRAM_JAR="${END_TO_END_DIR}/flink-file-sink-test/target/FileSinkProgram.jar"

# set_conf_ssl moves netty libraries into FLINK_DIR; rolled back explicitly between iterations,
# and registered here as a safety net for the currently active iteration if the script exits
# unexpectedly
on_exit rollback_openssl_lib

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

function run_file_sink_test {
  start_cluster

  "${FLINK_DIR}/bin/taskmanager.sh" start
  "${FLINK_DIR}/bin/taskmanager.sh" start
  "${FLINK_DIR}/bin/taskmanager.sh" start

  echo "Submitting job."
  CLIENT_OUTPUT=$("$FLINK_DIR/bin/flink" run -d "${TEST_PROGRAM_JAR}" --outputPath "${JOB_OUTPUT_PATH}" \
    --sinkToTest "${SINK_TO_TEST}")
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
  wait_for_complete_result 60000 900

  cancel_job "${JOB_ID}"

  wait_job_terminal_state "${JOB_ID}" "CANCELED"

  get_complete_result > "${TEST_DATA_DIR}/complete_result"

  check_result_hash "File Streaming Sink" "$TEST_DATA_DIR/complete_result" "6727342fdd3aae2129e61fc8f433fb6f"
}

###################################
# Runs the file sink test once, with the given OpenSSL linkage.
#
# Globals:
#   TEST_DATA_DIR, OUT_TYPE, IT_CASE_S3_BUCKET
# Arguments:
#   $1: OpenSSL linkage, 'dynamic' or 'static'
# Returns:
#   None
###################################
function run_file_sink_test_with_linkage {
  local openssl_linkage=$1

  echo "Executing test with ${openssl_linkage} openSSL linkage"

  # LOCAL_JOB_OUTPUT_PATH is a local folder that can be used as a download folder for remote data
  # the helper functions will access this folder
  local random_prefix="temp/test_file_sink-${openssl_linkage}-$(uuidgen)"
  LOCAL_JOB_OUTPUT_PATH="$TEST_DATA_DIR/${random_prefix}"
  mkdir -p "${LOCAL_JOB_OUTPUT_PATH}"

  # JOB_OUTPUT_PATH is the location where the job writes its data to
  JOB_OUTPUT_PATH="${LOCAL_JOB_OUTPUT_PATH}"

  if [ "${OUT_TYPE}" == "s3" ]; then
    # overwrites JOB_OUTPUT_PATH to point to S3
    S3_DATA_PREFIX="${random_prefix}"
    S3_CHECKPOINT_PREFIX="${random_prefix}-chk"
    JOB_OUTPUT_PATH="s3://$IT_CASE_S3_BUCKET/${S3_DATA_PREFIX}"
    set_config_key "execution.checkpointing.dir" "s3://$IT_CASE_S3_BUCKET/${S3_CHECKPOINT_PREFIX}"
  fi

  set_conf_ssl "mutual" "OPENSSL" "${openssl_linkage}"

  run_test_with_timeout 900 run_file_sink_test

  stop_cluster
  # set_conf_ssl moves netty libraries into FLINK_DIR which we want to rollback before the next
  # linkage is set up
  rollback_openssl_lib

  if [ "${OUT_TYPE}" == "s3" ]; then
    out_cleanup
  fi
}

# usual runtime is ~6 minutes per linkage
for openssl_linkage in dynamic static; do
  run_file_sink_test_with_linkage "${openssl_linkage}"
done
