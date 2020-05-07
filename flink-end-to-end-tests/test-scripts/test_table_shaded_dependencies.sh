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

source "$(dirname "$0")"/common.sh

FLINK_SCALA_VERSION=`ls "${END_TO_END_DIR}/../flink-table/flink-table-runtime-blink/target" | sed -n "s/.*flink-table-runtime-blink_\(.*\)-tests\.jar/\1/p" | uniq`
FLINK_VERSION=`ls "${END_TO_END_DIR}/../flink-table/flink-table-api-java/target" | sed -n "s/.*flink-table-api-java-\(.*\)-tests\.jar/\1/p" | uniq`

# This checks the bytecode for dependencies on external classes. Some classes below
# are not available in the uber jar. We check for classes that we expect in the uber jar with
# checkAllowedPackages method.
function checkCodeDependencies {
  local JAR=$1
  local CONTENTS_FILE=$TEST_DATA_DIR/contentsInJar.txt

  jdeps $JAR |\
      grep "^\s*\->" |\
      `# jdk dependencies` \
      grep -v "^\s*\-> java." |\
      grep -v "^\s*\-> sun.misc." |\
      grep -v "^\s*\-> javax." |\
      `# scala dependencies` \
      grep -v "^\s*\-> scala" |\
      `# flink dependencies` \
      grep -v "^\s*\-> org.apache.flink" |\
      `# flink-core dependencies` \
      grep -v "^\s*\-> com.esotericsoftware.kryo" |\
      `# janino dependencies` \
      grep -v "^\s*\-> org.codehaus.janino" |\
      grep -v "^\s*\-> org.codehaus.commons" |\
      grep -v "^\s*\-> org.apache.tools.ant" |\
      `# calcite dependencies` \
      grep -v "^\s*\-> org.apache.calcite" |\
      grep -v "^\s*\-> org.pentaho.aggdes" |\
      grep -v "^\s*\-> org.apache.commons.lang3" |\
      grep -v "^\s*\-> org.apache.commons.math3" |\
      grep -v "^\s*\-> org.apache.commons.dbcp2" |\
      grep -v "^\s*\-> org.apache.http" |\
      grep -v "^\s*\-> org.w3c.dom" |\
      grep -v "^\s*\-> org.xml.sax" |\
      grep -v "^\s*\-> org.ietf.jgss" |\
      grep -v "^\s*\-> com.esri.core." |\
      grep -v "^\s*\-> com.yahoo.sketches.hll." |\
      grep -v "^\s*\-> org.slf4j" |\
      grep -v "^\s*\-> org.json" |\
      grep -v "^\s*\-> org.apache.tapestry5.json." |\
      grep -v "^\s*\-> org.codehaus.jettison" |\
      grep -v "^\s*\-> org.apiguardian.api" |\
      grep -v "^\s*\-> org.apache.commons.io.input" |\
      grep -v "^\s*\-> com.ibm.icu" |\
      grep -v "^\s*\-> net.minidev.json" > $CONTENTS_FILE
  if [[ `cat $CONTENTS_FILE | wc -l` -eq '0' ]]; then
      echo "Success: There are no unwanted dependencies in the ${JAR} jar."
  else
      echo "Failure: There are unwanted dependencies in the ${JAR} jar: `cat $CONTENTS_FILE`"
      exit 1
  fi
}

# Checks that the uber jars contain only flink, relocated packages, or packages that we
# consciously decided to include as not relocated.
function checkAllowedPackages {
  local JAR=$1
  local CONTENTS_FILE=$TEST_DATA_DIR/contentsInJar.txt

  jar tf $JAR |\
      grep ".*class" |\
      grep -v "org/codehaus/janino" |\
      grep -v "org/codehaus/commons" |\
      grep -v "org/apache/calcite" |\
      grep -v "com/ibm/icu" |\
      grep -v "org/apache/flink" > $CONTENTS_FILE
  if [[ `cat $CONTENTS_FILE | wc -l` -eq '0' ]]; then
      echo "Success: There are no unwanted classes in the ${JAR} jar."
  else
      echo "Failure: There are unwanted classes in the ${JAR} jar: `cat $CONTENTS_FILE`"
      exit 1
  fi
}

checkCodeDependencies "${END_TO_END_DIR}/../flink-table/flink-table-api-java/target/flink-table-api-java-${FLINK_VERSION}.jar"
checkCodeDependencies "${END_TO_END_DIR}/../flink-table/flink-table-api-scala/target/flink-table-api-scala_${FLINK_SCALA_VERSION}.jar"
checkCodeDependencies "${END_TO_END_DIR}/../flink-table/flink-table-api-java-bridge/target/flink-table-api-java-bridge_${FLINK_SCALA_VERSION}.jar"
checkCodeDependencies "${END_TO_END_DIR}/../flink-table/flink-table-api-scala-bridge/target/flink-table-api-scala-bridge_${FLINK_SCALA_VERSION}.jar"

checkCodeDependencies "${END_TO_END_DIR}/../flink-table/flink-table-planner/target/flink-table-planner_${FLINK_SCALA_VERSION}.jar"
checkCodeDependencies "${END_TO_END_DIR}/../flink-table/flink-table-planner-blink/target/flink-table-planner-blink_${FLINK_SCALA_VERSION}.jar"
checkCodeDependencies "${END_TO_END_DIR}/../flink-table/flink-table-runtime-blink/target/flink-table-runtime-blink_${FLINK_SCALA_VERSION}.jar"
checkCodeDependencies "${FLINK_DIR}/lib/flink-table-blink_${FLINK_SCALA_VERSION}.jar"
checkCodeDependencies "${FLINK_DIR}/lib/flink-table_${FLINK_SCALA_VERSION}.jar"

checkAllowedPackages "${END_TO_END_DIR}/../flink-table/flink-table-api-java/target/flink-table-api-java-${FLINK_VERSION}.jar"
checkAllowedPackages "${END_TO_END_DIR}/../flink-table/flink-table-api-scala/target/flink-table-api-scala_${FLINK_SCALA_VERSION}.jar"
checkAllowedPackages "${END_TO_END_DIR}/../flink-table/flink-table-api-java-bridge/target/flink-table-api-java-bridge_${FLINK_SCALA_VERSION}.jar"
checkAllowedPackages "${END_TO_END_DIR}/../flink-table/flink-table-api-scala-bridge/target/flink-table-api-scala-bridge_${FLINK_SCALA_VERSION}.jar"

checkAllowedPackages "${END_TO_END_DIR}/../flink-table/flink-table-planner/target/flink-table-planner_${FLINK_SCALA_VERSION}.jar"
checkAllowedPackages "${END_TO_END_DIR}/../flink-table/flink-table-planner-blink/target/flink-table-planner-blink_${FLINK_SCALA_VERSION}.jar"
checkAllowedPackages "${END_TO_END_DIR}/../flink-table/flink-table-runtime-blink/target/flink-table-runtime-blink_${FLINK_SCALA_VERSION}.jar"
checkAllowedPackages "${FLINK_DIR}/lib/flink-table-blink_${FLINK_SCALA_VERSION}.jar"
checkAllowedPackages "${FLINK_DIR}/lib/flink-table_${FLINK_SCALA_VERSION}.jar"
