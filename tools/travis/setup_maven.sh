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

MAVEN_VERSION="3.2.5"
MAVEN_CACHE_DIR=${HOME}/maven_cache
MAVEN_VERSIONED_DIR=${MAVEN_CACHE_DIR}/apache-maven-${MAVEN_VERSION}

if [ ! -d "${MAVEN_VERSIONED_DIR}" ]; then
  wget https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.zip
  unzip -d "${MAVEN_CACHE_DIR}" -qq "apache-maven-${MAVEN_VERSION}-bin.zip"
  rm "apache-maven-${MAVEN_VERSION}-bin.zip"
fi

export M2_HOME="${MAVEN_VERSIONED_DIR}"
export PATH=${M2_HOME}/bin:${PATH}
export MAVEN_OPTS="${MAVEN_OPTS} -Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS"

# just in case: clean up the .m2 home and remove invalid jar files
if [ -d "${HOME}/.m2/repository/" ]; then
  find ${HOME}/.m2/repository/ -name "*.jar" -exec sh -c 'if ! zip -T {} >/dev/null ; then echo "deleting invalid file: {}"; rm -f {} ; fi' \;
fi

