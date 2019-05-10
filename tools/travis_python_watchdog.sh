#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

HERE="`dirname \"$0\"`"				# relative
HERE="`( cd \"$HERE\" && pwd )`" 	# absolutized and normalized
if [ -z "$HERE" ] ; then
	# error; for some reason, the path is not accessible
	# to the script (e.g. permissions re-evaled after suid)
	exit 1  # fail
fi

source "${HERE}/travis/stage.sh"

ARTIFACTS_DIR="${HERE}/artifacts"

mkdir -p $ARTIFACTS_DIR || { echo "FAILURE: cannot create log directory '${ARTIFACTS_DIR}'." ; exit 1; }

echo "Build for commit ${TRAVIS_COMMIT} of ${TRAVIS_REPO_SLUG} [build ID: ${TRAVIS_BUILD_ID}, job number: $TRAVIS_JOB_NUMBER]." | tee "${ARTIFACTS_DIR}/build_info"

# =============================================================================
# CONFIG
# =============================================================================

# Number of seconds w/o output before printing a stack trace and killing python test process
MAX_NO_OUTPUT=${1:-300}

# Number of seconds to sleep before checking the output again
SLEEP_TIME=20

PYTHON_TEST="./flink-python/dev/lint-python.sh"

PYTHON_PID="${ARTIFACTS_DIR}/watchdog.python.pid"
PYTHON_EXIT="${ARTIFACTS_DIR}/watchdog.python.exit"
PYTHON_OUT="${ARTIFACTS_DIR}/python.out"

TRACE_OUT="${ARTIFACTS_DIR}/jps-traces.out"

# E.g. travis-artifacts/apache/flink/1595/1595.1
UPLOAD_TARGET_PATH="travis-artifacts/${TRAVIS_REPO_SLUG}/${TRAVIS_BUILD_NUMBER}/"
# These variables are stored as secure variables in '.travis.yml', which are generated per repo via
# the travis command line tool.
UPLOAD_BUCKET=$ARTIFACTS_AWS_BUCKET
UPLOAD_ACCESS_KEY=$ARTIFACTS_AWS_ACCESS_KEY
UPLOAD_SECRET_KEY=$ARTIFACTS_AWS_SECRET_KEY

ARTIFACTS_FILE=${TRAVIS_JOB_NUMBER}.tar.gz

# =============================================================================
# FUNCTIONS
# =============================================================================

upload_artifacts_s3() {
	echo "PRODUCED build artifacts."

	ls $ARTIFACTS_DIR

	echo "COMPRESSING build artifacts."

	cd $ARTIFACTS_DIR
	dmesg > container.log
	tar -zcvf $ARTIFACTS_FILE *

	# Upload to secured S3
	if [ -n "$UPLOAD_BUCKET" ] && [ -n "$UPLOAD_ACCESS_KEY" ] && [ -n "$UPLOAD_SECRET_KEY" ]; then

		# Install artifacts tool
		curl -sL https://raw.githubusercontent.com/travis-ci/artifacts/master/install | bash

		PATH=$HOME/bin/artifacts:$HOME/bin:$PATH

		echo "UPLOADING build artifacts."

		# Upload everything in $ARTIFACTS_DIR. Use relative path, otherwise the upload tool
		# re-creates the whole directory structure from root.
		artifacts upload --bucket $UPLOAD_BUCKET --key $UPLOAD_ACCESS_KEY --secret $UPLOAD_SECRET_KEY --target-paths $UPLOAD_TARGET_PATH $ARTIFACTS_FILE
	fi

	# upload to https://transfer.sh
	echo "Uploading to transfer.sh"
	curl --upload-file $ARTIFACTS_FILE --max-time 60 https://transfer.sh
}

# For PythonGatewayServer
print_stacktraces () {
	echo "=============================================================================="
	echo "The following Java processes are running (JPS)"
	echo "=============================================================================="

	jps

	local pids=( $(jps | awk '{print $1}') )

	for pid in "${pids[@]}"; do
		echo "=============================================================================="
		echo "Printing stack trace of Java process ${pid}"
		echo "=============================================================================="

		jstack $pid
	done
}

mod_time () {
	if [[ `uname` == 'Darwin' ]]; then
		eval $(stat -s $PYTHON_OUT)
		echo $st_mtime
	else
		echo `stat -c "%Y" $PYTHON_OUT`
	fi
}

the_time() {
	echo `date +%s`
}

watchdog () {
	touch PYTHON_OUT

	while true; do
		sleep $SLEEP_TIME

		time_diff=$((`the_time` - `mod_time`))

		if [ $time_diff -ge $MAX_NO_OUTPUT ]; then
			echo "=============================================================================="
			echo "Maven produced no output for ${MAX_NO_OUTPUT} seconds."
			echo "=============================================================================="

			print_stacktraces | tee $TRACE_OUT

			kill $(<$PYTHON_PID)

			exit 1
		fi
	done
}

# =============================================================================
# WATCHDOG
# =============================================================================

# Start watching $PYTHON_OUT
watchdog &

WD_PID=$!

echo "STARTED watchdog (${WD_PID})."

# Make sure to be in project root
cd $HERE/../

# Compile modules

echo "RUNNING '${PYTHON_TEST}'."

# Run $PYTHON_TEST and pipe output to $PYTHON_OUT for the watchdog. The PID is written to $PYTHON_PID to
# allow the watchdog to kill python test process if it is not producing any output anymore. $PYTHON_EXIT contains
# the exit code. This is important for Travis' build life-cycle (success/failure).
( $PYTHON_TEST & PID=$! ; echo $PID >&3 ; wait $PID ; echo $? >&4 ) 3>$PYTHON_PID 4>$PYTHON_EXIT | tee $PYTHON_OUT

EXIT_CODE=$(<$PYTHON_EXIT)

echo "Pytest exited with EXIT CODE: ${EXIT_CODE}."

# Make sure to kill the watchdog in any case after PYTHON_TEST has completed
echo "Trying to KILL watchdog (${WD_PID})."
( kill $WD_PID 2>&1 ) > /dev/null

rm $PYTHON_PID
rm $PYTHON_EXIT

# Post

upload_artifacts_s3

# since we are in flink/tools/artifacts
# we are going back to
cd ../../

# Exit code for Travis build success/failure
exit $EXIT_CODE
