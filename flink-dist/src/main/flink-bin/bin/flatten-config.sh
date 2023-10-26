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

# Check the number of input parameters
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 FLINK_CONF_DIR FLINK_BIN_DIR FLINK_LIB_DIR [args]"
    exit 1
fi

# Add the path to the bash-java-utils script
source "$2"/bash-java-utils.sh

ARGS=("${@:1}")
result=$(parseConfigurationAndExportLogs "${ARGS[@]}")

CONF_FILE="$1/flink-conf.yaml"
if [ ! -e "$1/flink-conf.yaml" ]; then
  CONF_FILE="$1/config.yaml"
fi;

# Output the result
echo "${result}" > "$CONF_FILE";
