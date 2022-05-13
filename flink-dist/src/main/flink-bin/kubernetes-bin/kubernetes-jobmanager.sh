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

# Start a Flink JobManager for native Kubernetes.
# NOTE: This script is not meant to be started manually. It will be used by native Kubernetes integration.

USAGE="Usage: kubernetes-jobmanager.sh kubernetes-session|kubernetes-application [args]"

ENTRY_POINT_NAME=$1

ARGS=("${@:2}")

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# Add JobManager specific JVM options
export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_JM}"
parseJmArgsAndExportLogs "${ARGS[@]}"

if [ ! -z "${DYNAMIC_PARAMETERS}" ]; then
    ARGS=(${DYNAMIC_PARAMETERS[@]} "${ARGS[@]}")
fi

exec "${FLINK_BIN_DIR}"/flink-console.sh ${ENTRY_POINT_NAME} "${ARGS[@]}"
