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

COMMAND=$1

HERE="`dirname \"$0\"`"             # relative
HERE="`( cd \"$HERE\" && pwd )`"    # absolutized and normalized
if [ -z "$HERE" ] ; then
	exit 1
fi

OUTPUT_FILE=/tmp/_e2e_watchdog.output
# Start uploading 11 minutes before the timeout imposed by Azure (11 minutes because the upload happens 
# every 5 minutes, so we should ideally get 2 uploads and then the operation gets killed)
START_LOG_UPLOAD_SECONDS_FROM_END=$((11*60))

DEFINED_TIMEOUT_MINUTES=`cat $HERE/jobs-template.yml | grep "timeoutInMinutes" | tail -n 1 | cut -d ":" -f 2 | tr -d '[:space:]'`
DEFINED_TIMEOUT_SECONDS=$(($DEFINED_TIMEOUT_MINUTES*60))

echo "Running command '$COMMAND' with a timeout of $DEFINED_TIMEOUT_MINUTES minutes."

function warning_watchdog {
	SLEEP_TIME=$(echo "scale=0; $DEFINED_TIMEOUT_SECONDS*0.8/1" | bc)
	sleep $SLEEP_TIME
	echo "=========================================================================================="
	echo "=== WARNING: This E2E Run took already 80% of the allocated time budget of $DEFINED_TIMEOUT_MINUTES minutes ==="
	echo "=========================================================================================="
}

function log_upload_watchdog {
	SLEEP_TIME=$(($DEFINED_TIMEOUT_SECONDS-$START_LOG_UPLOAD_SECONDS_FROM_END))
	sleep $SLEEP_TIME
	echo "======================================================================================================"
	echo "=== WARNING: This E2E Run will time out in the next few minutes. Starting to upload the log output ==="
	echo "======================================================================================================"

	INDEX=0
	while true; do
		cp $OUTPUT_FILE "$OUTPUT_FILE.$INDEX"
		echo "##vso[artifact.upload containerfolder=e2e-timeout-logs;artifactname=log_upload_watchdog.output;]$OUTPUT_FILE.$INDEX"
		INDEX=$(($INDEX+1))
		sleep 300
	done
}


function stop_watchdog {
  # pkill (the child processes) and the watchdog shell itself. This is necessary to prevent the
  # sleep inside the watchdog to become a daemon process, which inherits the file descriptors and
  # potentially prevents parent processes from noticing that this script is done.
  # Kill silently. If a watchdog has "triggered", it won't exist anymore.
  ( pkill -P $1 2>&1 ) > /dev/null
  ( kill $1 2>&1 ) > /dev/null
}

warning_watchdog &
warning_wd_pid=$!
log_upload_watchdog &
log_upload_wd_pid=$!

# ts from moreutils prepends the time to each line
eval $COMMAND 2>&1 | ts | tee $OUTPUT_FILE
TEST_EXIT_CODE=${PIPESTATUS[0]}

stop_watchdog $warning_wd_pid
stop_watchdog $log_upload_wd_pid

# properly forward exit code
exit $TEST_EXIT_CODE
