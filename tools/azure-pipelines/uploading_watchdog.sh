#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file has the following tasks
# a) It reads the e2e timeout from the configuration file
# b) It prints a warning if the test has reached 80% of it's execution time
# c) N minutes before the end of the execution time, it will start uploading the current output as azure artifacts

COMMAND=$@

HERE="`dirname \"$0\"`"             # relative
HERE="`( cd \"$HERE\" && pwd )`"    # absolutized and normalized
if [ -z "$HERE" ] ; then
  exit 1
fi

if [ -n "${TF_BUILD+x}" ]; then
  echo "[INFO] Azure Pipelines environment detected: $0 will rely on Azure-specific environment variables."
  job_name="$AGENT_JOBNAME"
  temporary_folder="$AGENT_TEMPDIRECTORY"
  job_timeout="$SYSTEM_JOBTIMEOUT"
  pipeline_start_time="${SYSTEM_PIPELINESTARTTIME}"
elif [ -n "${GITHUB_ACTIONS+x}" ]; then
  echo "[INFO] GitHub Actions environment detected: $0 will rely on GHA-specific environment variables."
  job_name="${GITHUB_JOB}"
  temporary_folder="${RUNNER_TEMP}"

  if [ -n "${GHA_JOB_TIMEOUT+x}" ]; then
    job_timeout="${GHA_JOB_TIMEOUT}"
  else
    echo "[ERROR] GHA_JOB_TIMEOUT is not set: GHA_JOB_TIMEOUT needs to be set by the workflow because there is no pre-defined environment variable available for this parameter for now."
    exit 1
  fi

  if [ -n "${GHA_PIPELINE_START_TIME+x}" ]; then
    pipeline_start_time="${GHA_PIPELINE_START_TIME}"
  else
    echo "[ERROR] GHA_PIPELINE_START_TIME is not set: GHA_PIPELINE_START_TIME needs to be set by the workflow because there is no pre-defined environment variable available for this parameter for now."
    exit 1
  fi
else
  echo "[ERROR] No CI environment detected that could be used for injecting the parameters which are needed by $0."
  exit 1
fi

source "${HERE}/../ci/controller_utils.sh"

source ./tools/azure-pipelines/debug_files_utils.sh
prepare_debug_files "${temporary_folder}" "${job_name}"
export FLINK_LOG_DIR="$DEBUG_FILES_OUTPUT_DIR/flink-logs"
mkdir $FLINK_LOG_DIR || { echo "FAILURE: cannot create log directory '${FLINK_LOG_DIR}'." ; exit 1; }
sudo apt-get install -y moreutils

REAL_START_SECONDS=$(date +"%s")
REAL_END_SECONDS=$(date -d "${pipeline_start_time} + ${job_timeout} minutes" +"%s")
REAL_TIMEOUT_SECONDS=$(($REAL_END_SECONDS - $REAL_START_SECONDS))
KILL_SECONDS_BEFORE_TIMEOUT=$((2 * 60))

echo "Running command '$COMMAND' with a timeout of $(($REAL_TIMEOUT_SECONDS / 60)) minutes."

MAIN_PID_FILE="/tmp/uploading_watchdog_main.pid"

function timeout_watchdog() {
  # 95%
  sleep $(($REAL_TIMEOUT_SECONDS * 95 / 100))
  echo "=========================================================================================="
  echo "=== WARNING: This task took already 95% of the available time budget of $((REAL_TIMEOUT_SECONDS / 60)) minutes ==="
  echo "=========================================================================================="
  print_stacktraces | tee "$DEBUG_FILES_OUTPUT_DIR/jps-traces.0"

  # final stack trace and kill processes 1 min before timeout
  local secondsToKill=$(($REAL_END_SECONDS - $(date +"%s") - $KILL_SECONDS_BEFORE_TIMEOUT))
  if [[ $secondsToKill -lt 0 ]]; then
    secondsToKill=0
  fi
  sleep ${secondsToKill}
  print_stacktraces | tee "$DEBUG_FILES_OUTPUT_DIR/jps-traces.1"

  echo "============================="
  echo "=== WARNING: Killing task ==="
  echo "============================="
  pkill -P $(<$MAIN_PID_FILE) # kill descendants
  kill $(<$MAIN_PID_FILE)     # kill process itself

  exit 42
}

timeout_watchdog &
WATCHDOG_PID=$!

# ts from moreutils prepends the time to each line
( $COMMAND & PID=$! ; echo $PID >$MAIN_PID_FILE ; wait $PID ) | ts | tee $DEBUG_FILES_OUTPUT_DIR/watchdog
TEST_EXIT_CODE=${PIPESTATUS[0]}

if [[ "$TEST_EXIT_CODE" == 0 ]]; then
  echo "[INFO] Test execution finished with exit code 0. Artifacts related to the watchdog process will be cleaned up."
  kill $WATCHDOG_PID
  rm $DEBUG_FILES_OUTPUT_DIR/watchdog
  rm -f $DEBUG_FILES_OUTPUT_DIR/jps-traces.*

  unset_debug_artifacts_if_empty
fi

# properly forward exit code
exit $TEST_EXIT_CODE
