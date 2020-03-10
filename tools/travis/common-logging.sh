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

# Maximum times to retry uploading artifacts file to transfer.sh
TRANSFER_UPLOAD_MAX_RETRIES=2

# The delay between two retries to upload artifacts file to transfer.sh. The default exponential
# backoff algorithm should be too long for the last several retries.
TRANSFER_UPLOAD_RETRY_DELAY=5

# E.g. travis-artifacts/apache/flink/1595/1595.1
UPLOAD_TARGET_PATH="travis-artifacts/${TRAVIS_REPO_SLUG}/${TRAVIS_BUILD_NUMBER}/"
# These variables are stored as secure variables in '.travis.yml', which are generated per repo via
# the travis command line tool.
UPLOAD_BUCKET=$ARTIFACTS_AWS_BUCKET
UPLOAD_ACCESS_KEY=$ARTIFACTS_AWS_ACCESS_KEY
UPLOAD_SECRET_KEY=$ARTIFACTS_AWS_SECRET_KEY


SCRIPT_DIR="`dirname \"$0\"`"
SCRIPT_DIR="`( cd \"${SCRIPT_DIR}\" && pwd -P)`"
export FLINK_ROOT="`( cd \"${SCRIPT_DIR}/..\" && pwd -P)`"
if [ -z "${FLINK_ROOT}" ] ; then
	# error; for some reason, the path is not accessible
	# to the script (e.g. permissions re-evaled after suid)
	exit 1  # fail
fi

prepare_artifacts() {
	export ARTIFACTS_DIR="${SCRIPT_DIR}/artifacts"

	mkdir -p $ARTIFACTS_DIR || { echo "FAILURE: cannot create log directory '${ARTIFACTS_DIR}'." ; exit 1; }
}

upload_artifacts() {
	ARTIFACTS_FILE=${TRAVIS_JOB_NUMBER}.tar.gz
	if [ ! -z "$TF_BUILD" ] ; then
		# set proper artifacts file name on Azure Pipelines
		ARTIFACTS_FILE=${BUILD_BUILDNUMBER}.tar.gz
		if [ ! -z "$MODULE" ] ; then
			ARTIFACTS_FILE=${BUILD_BUILDNUMBER}-$(echo $MODULE | tr -dc '[:alnum:]\n\r').tar.gz
		fi
	fi

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

	# On Azure, publish ARTIFACTS_FILE as a build artifact
	if [ ! -z "$TF_BUILD" ] ; then
		ARTIFACT_DIR="$FLINK_ROOT/artifact-dir"
		mkdir $ARTIFACT_DIR
		cp $ARTIFACTS_FILE $ARTIFACT_DIR/
		
		echo "##vso[task.setvariable variable=ARTIFACT_DIR]$ARTIFACT_DIR"
		echo "##vso[task.setvariable variable=ARTIFACT_NAME]$(echo $MODULE | tr -dc '[:alnum:]\n\r')"
	fi

	# upload to https://transfer.sh
	echo "Uploading to transfer.sh"
	curl --retry ${TRANSFER_UPLOAD_MAX_RETRIES} --retry-delay ${TRANSFER_UPLOAD_RETRY_DELAY} --upload-file $ARTIFACTS_FILE --max-time 60 https://transfer.sh
}


################ Utilities for handling logs ##################

collect_coredumps() {
	echo "Searching for .dump, .dumpstream and related files in $FLINK_ROOT"
	for file in `find $FLINK_ROOT -type f -regextype posix-extended -iregex '.*\.dump|.*\.dumpstream|.*hs.*\.log|.*/core(.[0-9]+)?$'`; do
		echo "Copying $file to artifacts"
		cp $file $ARTIFACTS_DIR/
	done
}

# locate YARN logs and put them into artifacts directory
put_yarn_logs_to_artifacts() {
	# Make sure to be in project root
	echo "Searching for YARN container logs in '$FLINK_ROOT/flink-yarn-tests/target'"
	for file in `find $FLINK_ROOT/flink-yarn-tests/target -type f -name '*.log'`; do
		TARGET_FILE=`echo "$file" | grep -Eo "container_[0-9_]+/(.*).log"`
		TARGET_DIR=`dirname	 "$TARGET_FILE"`
		mkdir -p "$ARTIFACTS_DIR/yarn-tests/$TARGET_DIR"
		cp $file "$ARTIFACTS_DIR/yarn-tests/$TARGET_FILE"
	done
}

