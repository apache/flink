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

# Start a Flink TaskManager for native Kubernetes.
# NOTE: This script is not meant to be started manually. It will be used by native Kubernetes integration.

USAGE="Usage: kubernetes-taskmanager.sh [args]"

ENTRYPOINT=kubernetes-taskmanager

ARGS=("${@:1}")

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# if no other JVM options are set, set the GC to G1
if [ -z "${FLINK_ENV_JAVA_OPTS}" ] && [ -z "${FLINK_ENV_JAVA_OPTS_TM}" ]; then
    export JVM_ARGS="$JVM_ARGS -XX:+UseG1GC"
fi

# Add TaskManager specific JVM options
export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_TM}"
export JVM_ARGS="$JVM_ARGS $FLINK_TM_JVM_MEM_OPTS"

ARGS=("--configDir" "${FLINK_CONF_DIR}" "${ARGS[@]}")

exec "${FLINK_BIN_DIR}"/flink-console.sh $ENTRYPOINT "${ARGS[@]}"
