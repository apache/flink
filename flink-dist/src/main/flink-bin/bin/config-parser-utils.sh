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

USAGE="Usage: config-parser-utils.sh FLINK_CONF_DIR FLINK_BIN_DIR FLINK_LIB_DIR [dynamic args...]"

if [ "$#" -lt 3 ]; then
    echo "$USAGE"
    exit 1
fi

source "$2"/bash-java-utils.sh
setJavaRun "$1"

ARGS=("${@:1}")
result=$(updateAndGetFlinkConfiguration "${ARGS[@]}")

if [[ $? -ne 0 ]]; then
  echo "[ERROR] Could not get configurations properly, the result is :"
  echo "$result"
  exit 1
fi

CONF_FILE="$1/flink-conf.yaml"
if [ ! -e "$1/flink-conf.yaml" ]; then
  CONF_FILE="$1/config.yaml"
fi;

# Output the result
echo "${result}" > "$CONF_FILE";
