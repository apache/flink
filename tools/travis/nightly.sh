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
HERE="`( cd \"${HERE}\" && pwd -P)`" 	# absolutized and normalized
if [ -z "${HERE}" ] ; then
	# error; for some reason, the path is not accessible
	# to the script (e.g. permissions re-evaled after suid)
	exit 1  # fail
fi

SCRIPT=$1
CMD=${@:2}

source ${HERE}/setup_docker.sh

ARTIFACTS_DIR="${HERE}/artifacts"

mkdir -p $ARTIFACTS_DIR || { echo "FAILURE: cannot create log directory '${ARTIFACTS_DIR}'." ; exit 1; }

LOG4J_PROPERTIES=${HERE}/../log4j-travis.properties

MVN_LOGGING_OPTIONS="-Dlog.dir=${ARTIFACTS_DIR} -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"
MVN_COMMON_OPTIONS="-nsu -B -Dflink.forkCount=2 -Dflink.forkCountTestPackage=2 -Dmaven.wagon.http.pool=false -Dfast -Pskip-webui-build"
MVN_COMPILE_OPTIONS="-DskipTests"

cp tools/travis/splits/* flink-end-to-end-tests

COMMIT_HASH=$(git rev-parse HEAD)
echo "Testing branch ${BRANCH} from remote ${REMOTE}. Commit hash: ${COMMIT_HASH}"

e2e_modules=$(find flink-end-to-end-tests -mindepth 2 -maxdepth 5 -name 'pom.xml' -printf '%h\n' | sort -u | tr '\n' ',')
e2e_modules="${e2e_modules},$(find flink-walkthroughs -mindepth 2 -maxdepth 2 -name 'pom.xml' -printf '%h\n' | sort -u | tr '\n' ',')"
MVN_COMPILE="mvn ${MVN_COMMON_OPTIONS} ${MVN_COMPILE_OPTIONS} ${MVN_LOGGING_OPTIONS} ${PROFILE} clean install -pl ${e2e_modules},flink-dist -am"

eval "${MVN_COMPILE}"
EXIT_CODE=$?

if [ $EXIT_CODE == 0 ]; then
	printf "\n\n==============================================================================\n"
	printf "Running Java end-to-end tests\n"
	printf "==============================================================================\n"

	MVN_TEST="mvn ${MVN_COMMON_OPTIONS} ${MVN_LOGGING_OPTIONS} ${PROFILE} verify -pl ${e2e_modules} -DdistDir=$(readlink -e build-target)"

	eval "${MVN_TEST}"
	EXIT_CODE=$?
else
	printf "\n\n==============================================================================\n"
	printf "Compile failure detected, skipping Java end-to-end tests\n"
	printf "==============================================================================\n"
fi

if [ $EXIT_CODE == 0 ]; then
	printf "\n\n==============================================================================\n"
	printf "Running end-to-end tests\n"
	printf "==============================================================================\n"

	FLINK_DIR=build-target flink-end-to-end-tests/${SCRIPT} ${CMD}

	EXIT_CODE=$?
else
	printf "\n\n==============================================================================\n"
	printf "Compile failure detected, skipping end-to-end tests\n"
	printf "==============================================================================\n"
fi

# Exit code for Travis build success/failure
exit ${EXIT_CODE}
