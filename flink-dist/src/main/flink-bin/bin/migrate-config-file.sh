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

echo "Starting configuration migration..."
  
FLINK_BIN_DIR=`dirname "$0"`
FLINK_BIN_DIR=`cd "$FLINK_BIN_DIR"; pwd`
  
. "$FLINK_BIN_DIR"/bash-java-utils.sh
  
FLINK_CONF_DIR="$FLINK_BIN_DIR"/../conf
echo "Using Flink configuration directory: $FLINK_CONF_DIR"
setJavaRun "$FLINK_CONF_DIR"

FLINK_LIB_DIR="$FLINK_BIN_DIR"/../lib
echo "Using Flink library directory: $FLINK_LIB_DIR"

echo "Running migration..."
result=$(migrateLegacyFlinkConfigToStandardYaml "$FLINK_CONF_DIR" "$FLINK_BIN_DIR" "$FLINK_LIB_DIR")
  
if [[ $? -ne 0 ]]; then
  echo "[ERROR] Could not migrate configurations properly, the result is:"
  echo "$result"
  exit 1
fi
  
CONF_FILE="$FLINK_CONF_DIR/config.yaml"
echo "Migration completed successfully. Writing configuration to $CONF_FILE"
  
# Output the result
echo "${result}" > "$CONF_FILE";

echo "Configuration migration finished. New configuration file is located at: $CONF_FILE"
