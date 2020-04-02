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

# Wrapper script to compare the TM heap size calculation of config.sh with Java
USAGE="Usage: runBashJavaUtilsCmd.sh <command>"

COMMAND=$1

if [[ -z "${COMMAND}" ]]; then
  echo "$USAGE"
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

FLINK_CONF_DIR=${bin}/../../main/resources
FLINK_TARGET_DIR=${bin}/../../../target
FLINK_DIST_JAR=`find $FLINK_TARGET_DIR -name 'flink-dist*.jar'`

. ${bin}/../../main/flink-bin/bin/config.sh > /dev/null

output=$(runBashJavaUtilsCmd GET_TM_RESOURCE_PARAMS ${FLINK_CONF_DIR} "$FLINK_TARGET_DIR/bash-java-utils.jar:$FLINK_DIST_JAR}" | tail -n 2)
extractExecutionParams "$(echo "$output" | head -n 1)"
extractExecutionParams "$(echo "$output" | tail -n 1)"
