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

# Number of seconds w/o output before printing a stack trace and killing $MVN
MAX_NO_OUTPUT=${1:-300}

# Number of seconds to sleep before checking the output again
SLEEP_TIME=20

LOG4J_PROPERTIES=${HERE}/log4j-travis.properties

MVN_COMPILE_MODULES=$(get_compile_modules_for_stage ${TEST})
MVN_TEST_MODULES=$(get_test_modules_for_stage ${TEST})

# Maven command to run. We set the forkCount manually, because otherwise Maven sees too many cores
# on the Travis VMs. Set forkCountTestPackage to 1 for container-based environment (4 GiB memory)
# and 2 for sudo-enabled environment (7.5 GiB memory).
#
# -nsu option forbids downloading snapshot artifacts. The only snapshot artifacts we depend are from
# Flink, which however should all be built locally. see FLINK-7230
#
# We use -Punsafe-mapr-repo since the https version fails on Travis for some reason.
MVN_LOGGING_OPTIONS="-Dlog.dir=${ARTIFACTS_DIR} -Dlog4j.configuration=file://$LOG4J_PROPERTIES -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"
MVN_COMMON_OPTIONS="-nsu -Dflink.forkCount=2 -Dflink.forkCountTestPackage=2 -Dfast -B -Punsafe-mapr-repo $MVN_LOGGING_OPTIONS"
MVN_COMPILE_OPTIONS="-DskipTests"
MVN_TEST_OPTIONS="$MVN_LOGGING_OPTIONS"

MVN_COMPILE="mvn $MVN_COMMON_OPTIONS $MVN_COMPILE_OPTIONS $PROFILE $MVN_COMPILE_MODULES install"
MVN_TEST="mvn $MVN_COMMON_OPTIONS $MVN_TEST_OPTIONS $PROFILE $MVN_TEST_MODULES verify"

MVN_PID="${ARTIFACTS_DIR}/watchdog.mvn.pid"
MVN_EXIT="${ARTIFACTS_DIR}/watchdog.mvn.exit"
MVN_OUT="${ARTIFACTS_DIR}/mvn.out"

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

print_system_info() {
	FOLD_ESCAPE="\x0d\x1b"
	COLOR_ON="\x5b\x30\x4b\x1b\x5b\x33\x33\x3b\x31\x6d"
	COLOR_OFF="\x1b\x5b\x30\x6d"

	echo -e "travis_fold:start:cpu_info${FOLD_ESCAPE}${COLOR_ON}CPU information${COLOR_OFF}"
	lscpu
	echo -en "travis_fold:end:cpu_info${FOLD_ESCAPE}"

	echo -e "travis_fold:start:mem_info${FOLD_ESCAPE}${COLOR_ON}Memory information${COLOR_OFF}"
	cat /proc/meminfo
	echo -en "travis_fold:end:mem_info${FOLD_ESCAPE}"

	echo -e "travis_fold:start:disk_info${FOLD_ESCAPE}${COLOR_ON}Disk information${COLOR_OFF}"
	df -hH
	echo -en "travis_fold:end:disk_info${FOLD_ESCAPE}"

	echo -e "travis_fold:start:cache_info${FOLD_ESCAPE}${COLOR_ON}Cache information${COLOR_OFF}"
	du -s --si $HOME/.m2
	echo -en "travis_fold:end:cache_info${FOLD_ESCAPE}"
}

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
	curl --upload-file $ARTIFACTS_FILE https://transfer.sh
}

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

# locate YARN logs and put them into artifacts directory
put_yarn_logs_to_artifacts() {
	# Make sure to be in project root
	cd $HERE/../
	for file in `find ./flink-yarn-tests/target/flink-yarn-tests* -type f -name '*.log'`; do
		TARGET_FILE=`echo "$file" | grep -Eo "container_[0-9_]+/(.*).log"`
		TARGET_DIR=`dirname	 "$TARGET_FILE"`
		mkdir -p "$ARTIFACTS_DIR/yarn-tests/$TARGET_DIR"
		cp $file "$ARTIFACTS_DIR/yarn-tests/$TARGET_FILE"
	done
}

mod_time () {
	if [[ `uname` == 'Darwin' ]]; then
		eval $(stat -s $MVN_OUT)
		echo $st_mtime
	else
		echo `stat -c "%Y" $MVN_OUT`
	fi
}

the_time() {
	echo `date +%s`
}

watchdog () {
	touch $MVN_OUT

	while true; do
		sleep $SLEEP_TIME

		time_diff=$((`the_time` - `mod_time`))

		if [ $time_diff -ge $MAX_NO_OUTPUT ]; then
			echo "=============================================================================="
			echo "Maven produced no output for ${MAX_NO_OUTPUT} seconds."
			echo "=============================================================================="

			print_stacktraces | tee $TRACE_OUT

			kill $(<$MVN_PID)

			exit 1
		fi
	done
}

# =============================================================================
# WATCHDOG
# =============================================================================

# Start watching $MVN_OUT
watchdog &

WD_PID=$!

echo "STARTED watchdog (${WD_PID})."

print_system_info

# Make sure to be in project root
cd $HERE/../

# Compile modules

echo "RUNNING '${MVN_COMPILE}'."

# Run $MVN_COMPILE and pipe output to $MVN_OUT for the watchdog. The PID is written to $MVN_PID to
# allow the watchdog to kill $MVN if it is not producing any output anymore. $MVN_EXIT contains
# the exit code. This is important for Travis' build life-cycle (success/failure).
( $MVN_COMPILE & PID=$! ; echo $PID >&3 ; wait $PID ; echo $? >&4 ) 3>$MVN_PID 4>$MVN_EXIT | tee $MVN_OUT

EXIT_CODE=$(<$MVN_EXIT)

echo "MVN exited with EXIT CODE: ${EXIT_CODE}."

# Make sure to kill the watchdog in any case after $MVN_COMPILE has completed
echo "Trying to KILL watchdog (${WD_PID})."
( kill $WD_PID 2>&1 ) > /dev/null

rm $MVN_PID
rm $MVN_EXIT

# Run tests if compilation was successful
if [ $EXIT_CODE == 0 ]; then

	# Start watching $MVN_OUT
	watchdog &
	echo "STARTED watchdog (${WD_PID})."

	WD_PID=$!

	echo "RUNNING '${MVN_TEST}'."

	# Run $MVN_TEST and pipe output to $MVN_OUT for the watchdog. The PID is written to $MVN_PID to
	# allow the watchdog to kill $MVN if it is not producing any output anymore. $MVN_EXIT contains
	# the exit code. This is important for Travis' build life-cycle (success/failure).
	( $MVN_TEST & PID=$! ; echo $PID >&3 ; wait $PID ; echo $? >&4 ) 3>$MVN_PID 4>$MVN_EXIT | tee $MVN_OUT

	EXIT_CODE=$(<$MVN_EXIT)

	echo "MVN exited with EXIT CODE: ${EXIT_CODE}."

	# Make sure to kill the watchdog in any case after $MVN_TEST has completed
	echo "Trying to KILL watchdog (${WD_PID})."
	( kill $WD_PID 2>&1 ) > /dev/null

	rm $MVN_PID
	rm $MVN_EXIT
else
	echo "=============================================================================="
	echo "Compilation failure detected, skipping test execution."
	echo "=============================================================================="
fi

# Post

# only misc builds flink-dist and flink-yarn-tests
case $TEST in
	(misc)
		put_yarn_logs_to_artifacts
	;;
esac

upload_artifacts_s3

# since we are in flink/tools/artifacts
# we are going back to
cd ../../

# only run end-to-end tests in misc because we only have flink-dist here
case $TEST in
	(misc)
		if [ $EXIT_CODE == 0 ]; then
			printf "\n\n==============================================================================\n"
			printf "Running end-to-end tests\n"
			printf "==============================================================================\n"

			FLINK_DIR=build-target flink-end-to-end-tests/run-pre-commit-tests.sh

			EXIT_CODE=$?
		else
			printf "\n==============================================================================\n"
			printf "Previous build failure detected, skipping end-to-end tests.\n"
			printf "==============================================================================\n"
		fi
	;;
esac

# Exit code for Travis build success/failure
exit $EXIT_CODE
