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
	MVN_CMD="mvn"
	if [[ "$M2_HOME" != "" ]]; then
		MVN_CMD="${M2_HOME}/bin/mvn"
	fi

	if [[ "$MVN_RUN_VERBOSE" != "false" ]]; then
		echo "Invoking mvn with '$MVN_GLOBAL_OPTIONS ${@}'"
	fi
	$MVN_CMD $MVN_GLOBAL_OPTIONS "${@}"
}
export -f run_mvn

function setup_maven {
	set -e # fail if there was an error setting up maven
	if [ ! -d "${MAVEN_VERSIONED_DIR}" ]; then
	  wget -nv https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/${MAVEN_VERSION}/apache-maven-${MAVEN_VERSION}-bin.zip
	  unzip -d "${MAVEN_CACHE_DIR}" -qq "apache-maven-${MAVEN_VERSION}-bin.zip"
	  rm "apache-maven-${MAVEN_VERSION}-bin.zip"
	fi

	export M2_HOME="${MAVEN_VERSIONED_DIR}"
	echo "##vso[task.setvariable variable=M2_HOME]$M2_HOME"

	# just in case: clean up the .m2 home and remove invalid jar files
	if [ -d "${HOME}/.m2/repository/" ]; then
	  find ${HOME}/.m2/repository/ -name "*.jar" -exec sh -c 'if ! zip -T {} >/dev/null ; then echo "deleting invalid file: {}"; rm -f {} ; fi' \;
	fi

	echo "Installed Maven ${MAVEN_VERSION} to ${M2_HOME}"
	set +e
}

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

MAVEN_VERSION="3.8.6"
MAVEN_CACHE_DIR=${HOME}/maven_cache
MAVEN_VERSIONED_DIR=${MAVEN_CACHE_DIR}/apache-maven-${MAVEN_VERSION}


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
