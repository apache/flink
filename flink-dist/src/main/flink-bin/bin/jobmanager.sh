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

# Start/stop a Flink JobManager.
USAGE="Usage: jobmanager.sh ((start|start-foreground) [host] [webui-port])|stop|stop-all"

STARTSTOP=$1
HOST=$2 # optional when starting multiple instances
WEBUIPORT=$3 # optional when starting multiple instances

if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]] && [[ $STARTSTOP != "stop-all" ]]; then
  echo $USAGE
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

ENTRYPOINT=standalonesession

if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then
    # Add JobManager-specific JVM options
    export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_JM}"

    # Startup parameters

    java_utils_output=$(runBashJavaUtilsCmd GET_JM_RESOURCE_PARAMS ${FLINK_CONF_DIR} $FLINK_BIN_DIR/bash-java-utils.jar:$(findFlinkDistJar))
    logging_output=$(extractLoggingOutputs "${java_utils_output}")
    jvm_params=$(extractExecutionResults "${java_utils_output}" 1)
    export JVM_ARGS="${JVM_ARGS} ${jvm_params}"

    args=("--configDir" "${FLINK_CONF_DIR}" "--executionMode" "cluster")
    if [ ! -z $HOST ]; then
        args+=("--host")
        args+=("${HOST}")
    fi

    if [ ! -z $WEBUIPORT ]; then
        args+=("--webui-port")
        args+=("${WEBUIPORT}")
    fi

    export FLINK_INHERITED_LOGS="
$FLINK_INHERITED_LOGS

JM_RESOURCE_PARAMS extraction logs:
jvm_params: $jvm_params
logs: $logging_output
"

fi

if [[ $STARTSTOP == "start-foreground" ]]; then
    exec "${FLINK_BIN_DIR}"/flink-console.sh $ENTRYPOINT "${args[@]}"
else
    "${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP $ENTRYPOINT "${args[@]}"
fi
