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

    local num_ports_before=$(get_num_ports)

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
    post_test_validation ${exit_code} "$description" "$skip_check_exceptions" "$num_ports_before"
}

# Validates the test result and exit code after its execution.
function post_test_validation {
    local exit_code="$1"
    local description="$2"
    local skip_check_exceptions="$3"
    local num_ports_before="$4"

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
        ensure_clean_environment ${num_ports_before} || exit $?
    else
        log_environment_info
        # make logs available if ARTIFACTS_DIR is set
        if [[ ${ARTIFACTS_DIR} != "" ]]; then
            mkdir ${ARTIFACTS_DIR}/e2e-flink-logs 
            cp $FLINK_DIR/log/* ${ARTIFACTS_DIR}/e2e-flink-logs/
            echo "Published e2e logs into debug logs artifact:"
            ls ${ARTIFACTS_DIR}/e2e-flink-logs/
        fi
        exit "${exit_code}"
    fi
}

# returns the number of allocated ports
function get_num_ports {
    # "ps --ppid 2 -p 2 --deselect" shows all non-kernel processes
    # "ps --ppid $$" shows all children of this bash process
    # "ps -o pid= -o comm=" removes the header line
    echo $(sudo netstat -tulpn | wc -l)
}

# Ensure that the number of running processes has not increased (no leftover daemons,
# potentially affecting subsequent tests due to allocated ports etc.)
function ensure_clean_environment {
    local num_ports_before=$1
    local num_ports_after=$(get_num_ports)
    if [ "$num_ports_before" -ne "$num_ports_after" ]; then
        printf "\n==============================================================================\n"
        echo "FATAL: This test has left ports allocated."
        echo "Allocated ports before the test: $num_ports_before and after: $num_ports_after. Stopping test execution."
        printf "\n==============================================================================\n"
        echo "Printing pstree for debugging:"
        sudo pstree -p
        exit 1
    fi

    if [ $(jps | wc -l) -ne 1 ]; then
        printf "\n==============================================================================\n"
        echo "FATAL: This test has left JVMs running. Stopping test execution."
        printf "\n==============================================================================\n"
        exit 1
    fi
}


function log_environment_info {
    echo "##[group]Environment Information"
    echo "Running JVMs"
    jps -v

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
    rm -f ${FLINK_DIR}/log/*
    echo "Deleted all files under ${FLINK_DIR}/log/"

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
