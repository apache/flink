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

function checkDependencies {
  JAR=$1
  CONTENTS_FILE=$TEST_DATA_DIR/contentsInJar.txt
  jdeps $JAR > $CONTENTS_FILE
  if [[ `grep -c "(?<!org.apache.flink.calcite.shaded.)com.google" $CONTENTS_FILE` -eq '0' && \
      `grep -c "(?<!org.apache.flink.calcite.shaded.)com.jayway" $CONTENTS_FILE` -eq '0' && \
      `grep -c "(?<!org.apache.flink.calcite.shaded.)org.apache.commons.codec" $CONTENTS_FILE` -eq '0' && \
      `grep -c "(?<!org.apache.flink.table.shaded.)org.joda.time" $CONTENTS_FILE` -eq '0' && \
      `grep -c "(?<!org.apache.flink.table.shaded.)net.jpountz" $CONTENTS_FILE` -eq '0' && \
      `grep -c "(?<!org.apache.flink.calcite.shaded.)com.fasterxml" $CONTENTS_FILE` -eq '0' ]]; then

      echo "Success: There are no unwanted dependencies in the ${JAR} jar."
  else
      echo "Failure: There are unwanted dependencies in the ${JAR} jar."
      exit 1
  fi
}

checkDependencies "${END_TO_END_DIR}/../flink-table/flink-table-planner/target/flink-table-planner_*.jar"
checkDependencies "${END_TO_END_DIR}/../flink-table/flink-table-planner-blink/target/flink-table-planner-blink_*.jar"
checkDependencies "${END_TO_END_DIR}/../flink-table/flink-table-runtime-blink/target/flink-table-runtime-blink_*.jar"
checkDependencies "${FLINK_DIR}/lib/flink-table-blink.jar"
checkDependencies "${FLINK_DIR}/lib/flink-table.jar"
