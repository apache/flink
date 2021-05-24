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

source "${END_TO_END_DIR}"/test-scripts/common.sh

export FLINK_VERSION=$(MVN_RUN_VERBOSE=false run_mvn --file ${END_TO_END_DIR}/pom.xml org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)

#######################################
# Prints the given description, runs the given test and prints how long the execution took.
# Arguments:
#   $1: description of the test
#   $2: command to execute
#   $3: check logs for erors & exceptions
#######################################
function run_test {
    local description="$1"
    local command="$2"
    local skip_check_exceptions=${3:-}

    printf "\n==============================================================================\n"
    printf "Running '${description}'\n"
    printf "==============================================================================\n"

    # used to randomize created directories
    export TEST_DATA_DIR=$TEST_INFRA_DIR/temp-test-directory-$(date +%S%N)
    echo "TEST_DATA_DIR: $TEST_DATA_DIR"

    backup_flink_dir
    start_timer

    function test_error() {
      echo "[FAIL] Test script contains errors."
      post_test_validation 1 "$description" "$skip_check_exceptions"
    }
    # set a trap to catch a test execution error
    trap 'test_error' ERR

    # Always enable unaligned checkpoint
    set_config_key "execution.checkpointing.unaligned" "true"

    ${command}
    exit_code="$?"
    # remove trap for test execution
    trap - ERR
    post_test_validation ${exit_code} "$description" "$skip_check_exceptions"
}

# Validates the test result and exit code after its execution.
function post_test_validation {
    local exit_code="$1"
    local description="$2"
    local skip_check_exceptions="$3"

    local time_elapsed=$(end_timer)

    if [[ "${skip_check_exceptions}" != "skip_check_exceptions" ]]; then
        check_logs_for_errors
        check_logs_for_exceptions
        check_logs_for_non_empty_out_files
    else
        echo "Checking of logs skipped."
    fi

    # Investigate exit_code for failures of test executable as well as EXIT_CODE for failures of the test.
    # Do not clean up if either fails.
    if [[ ${exit_code} == 0 ]]; then
        if [[ ${EXIT_CODE} != 0 ]]; then
            printf "\n[FAIL] '${description}' failed after ${time_elapsed}! Test exited with exit code 0 but the logs contained errors, exceptions or non-empty .out files\n\n"
            exit_code=1
        else
            printf "\n[PASS] '${description}' passed after ${time_elapsed}! Test exited with exit code 0.\n\n"
        fi
    else
        if [[ ${EXIT_CODE} != 0 ]]; then
            printf "\n[FAIL] '${description}' failed after ${time_elapsed}! Test exited with exit code ${exit_code} and the logs contained errors, exceptions or non-empty .out files\n\n"
        else
            printf "\n[FAIL] '${description}' failed after ${time_elapsed}! Test exited with exit code ${exit_code}\n\n"
        fi
    fi

    if [[ ${exit_code} == 0 ]]; then
        cleanup
        log_environment_info
    else
        log_environment_info
        exit "${exit_code}"
    fi
}

function log_environment_info {
    echo "##[group]Environment Information"
    echo "Jps"
    jps

    echo "Disk information"
    df -hH

    echo "Allocated ports"
    sudo netstat -tulpn

    echo "Running docker containers"
    docker ps -a
    echo "##[endgroup]"
}

# Shuts down cluster and reverts changes to cluster configs
function cleanup_proc {
    shutdown_all
    revert_flink_dir
}

# Cleans up all temporary folders and files
function cleanup_tmp_files {
    rm -f $FLINK_LOG_DIR/*
    echo "Deleted all files under $FLINK_LOG_DIR/"

    rm -rf ${TEST_DATA_DIR} 2> /dev/null
    echo "Deleted ${TEST_DATA_DIR}"
}

# Shuts down the cluster and cleans up all temporary folders and files.
function cleanup {
    cleanup_proc
    cleanup_tmp_files
}

trap cleanup SIGINT
on_exit cleanup_proc
