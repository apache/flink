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

# Utility for invoking Maven in CI
function run_mvn {
	if [[ "$MVN_RUN_VERBOSE" != "false" ]]; then
		$MAVEN_WRAPPER --version
		echo "Invoking mvn with '$MVN_GLOBAL_OPTIONS ${@}'"
	fi
	$MAVEN_WRAPPER $MVN_GLOBAL_OPTIONS "${@}"
}
export -f run_mvn

function set_mirror_config {
	if [[ "$MAVEN_MIRROR_CONFIG_FILE" != "" ]]; then
		echo "[WARN] Maven mirror already configured to $MAVEN_MIRROR_CONFIG_FILE"
		exit 0;
	fi

	echo "Checking for availability of CI Maven mirror"
	# test if alibaba mirror is available
	curl --silent --max-time 10 http://mavenmirror.alicloud.dak8s.net:8888/repository/maven-central/ | grep "Nexus Repository Manager"

	if [[ "$?" == "0" ]]; then
		echo "Using Alibaba mirror"
		MAVEN_MIRROR_CONFIG_FILE="$CI_DIR/alibaba-mirror-settings.xml"
		NPM_PROXY_PROFILE_ACTIVATION="-Duse-alibaba-mirror"
	else
		echo "Using Google mirror"
		MAVEN_MIRROR_CONFIG_FILE="$CI_DIR/google-mirror-settings.xml"
	fi
}

function collect_coredumps {
	local SEARCHDIR=$1
	local TARGET_DIR=$2
	echo "Searching for .dump, .dumpstream and related files in '$SEARCHDIR'"
	for file in `find $SEARCHDIR -type f -regextype posix-extended -iregex '.*\.hprof|.*\.dump|.*\.dumpstream|.*hs.*\.log(\.[0-9]+)?|.*/core(\.[0-9]+)?$'`; do
		echo "Moving '$file' to target directory ('$TARGET_DIR')"
		mv $file $TARGET_DIR/$(echo $file | tr "/" "-")
	done
}


CI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if [[ ! "${CI_DIR}" =~ .*/tools/ci[/]{0,1}$ ]]; then
  echo "Error: ${BASH_SOURCE[0]} is expected to be located in the './tools/ci/' subfolder to make the Maven wrapper path resolution work."
  exit 1
fi
MAVEN_WRAPPER="${CI_DIR}/../../mvnw"

export MAVEN_WRAPPER

MAVEN_MIRROR_CONFIG_FILE=""
NPM_PROXY_PROFILE_ACTIVATION=""
set_mirror_config

export MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR="$MAVEN_ARGS "
# see https://developercommunity.visualstudio.com/content/problem/851041/microsoft-hosted-agents-run-into-maven-central-tim.html
MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR+="-Dmaven.wagon.http.pool=false "
# logging 
MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR+="-Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn "
# suppress snapshot updates
MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR+="--no-snapshot-updates "
# enable non-interactive batch mode
MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR+="-B "
# globally control the build profile details
MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR+="$PROFILE "

export MVN_GLOBAL_OPTIONS="${MVN_GLOBAL_OPTIONS_WITHOUT_MIRROR} "
# use google mirror everywhere
MVN_GLOBAL_OPTIONS+="--settings $MAVEN_MIRROR_CONFIG_FILE ${NPM_PROXY_PROFILE_ACTIVATION} "
