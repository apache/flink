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

# Start a MiniCluster
USAGE="Usage: minicluster.sh [--jars <jars>] [--job-classname <job class name>] [--job-id <job id>] [--fromSavepoint <path> [--allowNonRestoredState]] [-Dkey=value ...] [-- <program args>]"

if [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
    echo $USAGE
    exit 0
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_JM}"

ARGS=("--configDir" "${FLINK_CONF_DIR}" "$@")

exec "${FLINK_BIN_DIR}"/flink-console.sh minicluster "${ARGS[@]}"
