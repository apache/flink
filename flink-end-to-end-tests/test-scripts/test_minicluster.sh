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

# Tests bin/minicluster.sh from the distribution: to verify that it runs an application,
# that the arguments & JVM options are passed through, and that exit codes pass through
# the script to/from the JVM

source "$(dirname "$0")"/common.sh

MINICLUSTER_REST_PORT=8091
JVM_OPTS_MARKER="-Dminicluster.e2e.marker=true"
WORDCOUNT_RESULT_HASH="5a9945c9ab08890b2a0f6b31a4437d57"
# a file, as the cleanup runs outside of the subshell that run_test_with_timeout runs the test in
MINICLUSTER_PID_FILE="${TEST_DATA_DIR}/minicluster.pid"

function minicluster_cleanup() {
  if [[ -f "${MINICLUSTER_PID_FILE}" ]]; then
    kill "$(cat "${MINICLUSTER_PID_FILE}")" 2> /dev/null || true
  fi
  kill_all 'MiniClusterJobEntryPoint'
}

on_exit minicluster_cleanup

function assert_exit_code() {
  local description=$1
  local expected=$2
  local actual=$3

  if [[ "${actual}" != "${expected}" ]]; then
    echo "FAILURE: ${description}: expected exit code ${expected}, but was ${actual}."
    exit 1
  fi
  echo "${description}: exit code ${actual}, as expected."
}

function assert_minicluster_log_contains() {
  local description=$1
  local text=$2
  local log_dir=${3:-${FLINK_LOG_DIR}}

  if ! grep -q -F -e "${text}" "${log_dir}"/flink-*-minicluster-*.log*; then
    echo "FAILURE: ${description}: '${text}' not found in the MiniCluster log in ${log_dir}."
    exit 1
  fi
  echo "${description}: found '${text}' in the MiniCluster log."
}

function run_bounded_application() {
  echo "Running a bounded application, which the process exits after."
  local output_dir="${TEST_DATA_DIR}/out/wordcount"

  "${FLINK_DIR}"/bin/minicluster.sh \
      --jars "local://${FLINK_DIR}/examples/streaming/WordCount.jar" \
      -- \
      --input "${END_TO_END_DIR}/test-scripts/test-data/words" \
      --output "${output_dir}" \
      --execution-mode BATCH &
  local pid=$!
  echo ${pid} > "${MINICLUSTER_PID_FILE}"
  wait ${pid}
  assert_exit_code "Bounded application" 0 $?

  local output_files
  output_files=$(find "${output_dir}" -type f)
  check_result_hash "WordCount" "${output_files}" "${WORDCOUNT_RESULT_HASH}"
}

function run_unbounded_application_and_send_sigterm() {
  echo "Running an unbounded application, and stopping it with SIGTERM."

  set_config_key "env.java.opts.jobmanager" "${JVM_OPTS_MARKER}"

  "${FLINK_DIR}"/bin/minicluster.sh \
      --jars "local://${FLINK_DIR}/examples/streaming/TopSpeedWindowing.jar" \
      -Drest.port=${MINICLUSTER_REST_PORT} &
  local pid=$!
  echo ${pid} > "${MINICLUSTER_PID_FILE}"

  wait_rest_endpoint_up "http://localhost:${MINICLUSTER_REST_PORT}/jobs/overview" \
      "MiniCluster" '"state":"RUNNING"'

  # the configuration is logged too, so check the options the JVM was started with
  local jvm_environment
  jvm_environment=$(curl ${CURL_SSL_ARGS} -s "http://localhost:${MINICLUSTER_REST_PORT}/jobmanager/environment")
  if [[ "${jvm_environment}" != *"\"${JVM_OPTS_MARKER}\""* ]]; then
    echo "FAILURE: the JVM was not started with '${JVM_OPTS_MARKER}': ${jvm_environment%%,\"classpath\"*}"
    exit 1
  fi
  echo "JVM options: started with '${JVM_OPTS_MARKER}'."

  # the script must exec the JVM, so that a supervisor's signals reach it
  local command
  command=$(ps -p ${pid} -o comm=)
  if [[ "${command}" != *java ]]; then
    echo "FAILURE: process ${pid} started by minicluster.sh is '${command}', not the JVM."
    exit 1
  fi

  kill -TERM ${pid}
  wait ${pid}
  assert_exit_code "SIGTERM" 143 $?

  assert_minicluster_log_contains "SIGTERM" "RECEIVED SIGNAL 15: SIGTERM"
  if jps | grep -q 'MiniClusterJobEntryPoint'; then
    echo "FAILURE: the MiniCluster process is still running after SIGTERM."
    exit 1
  fi
}

function run_application_that_fails_to_start() {
  echo "Running an application whose job class does not exist."
  # the expected exception is logged elsewhere, so that the checks of the test logs don't fail
  local log_dir="${TEST_DATA_DIR}/startup-failure-log"

  FLINK_LOG_DIR="${log_dir}" "${FLINK_DIR}"/bin/minicluster.sh \
      --job-classname org.apache.flink.e2e.DoesNotExist &
  local pid=$!
  echo ${pid} > "${MINICLUSTER_PID_FILE}"
  wait ${pid}
  assert_exit_code "Startup failure" 1 $?

  assert_minicluster_log_contains "Startup failure" \
      "Could not run the application on a MiniCluster." "${log_dir}"
}

function run_minicluster_test() {
  run_bounded_application
  run_unbounded_application_and_send_sigterm
  run_application_that_fails_to_start
}

run_test_with_timeout 600 run_minicluster_test
