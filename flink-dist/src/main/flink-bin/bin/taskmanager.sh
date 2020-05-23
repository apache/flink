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

# Start/stop a Flink TaskManager.
USAGE="Usage: taskmanager.sh (start|start-foreground|stop|stop-all)"

STARTSTOP=$1

ARGS=("${@:2}")

if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]] && [[ $STARTSTOP != "stop-all" ]]; then
  echo $USAGE
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

ENTRYPOINT=taskexecutor

if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then

    # if no other JVM options are set, set the GC to G1
    if [ -z "${FLINK_ENV_JAVA_OPTS}" ] && [ -z "${FLINK_ENV_JAVA_OPTS_TM}" ]; then
        export JVM_ARGS="$JVM_ARGS -XX:+UseG1GC"
    fi

    # Add TaskManager-specific JVM options
    export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_TM}"

    # Startup parameters

    java_utils_output=$(runBashJavaUtilsCmd GET_TM_RESOURCE_PARAMS "${FLINK_CONF_DIR}" "$FLINK_BIN_DIR/bash-java-utils.jar:$(findFlinkDistJar)" "${ARGS[@]}")

    logging_output=$(extractLoggingOutputs "${java_utils_output}")
    params_output=$(extractExecutionResults "${java_utils_output}" 2)

    if [[ $? -ne 0 ]]; then
        echo "[ERROR] Could not get JVM parameters and dynamic configurations properly."
        exit 1
    fi

    jvm_params=$(echo "${params_output}" | head -n 1)
    export JVM_ARGS="${JVM_ARGS} ${jvm_params}"

    IFS=$" " dynamic_configs=$(echo "${params_output}" | tail -n 1)
    ARGS+=("--configDir" "${FLINK_CONF_DIR}" ${dynamic_configs[@]})

    export FLINK_INHERITED_LOGS="
$FLINK_INHERITED_LOGS

TM_RESOURCE_PARAMS extraction logs:
jvm_params: $jvm_params
dynamic_configs: $dynamic_configs
logs: $logging_output
"
fi

if [[ $STARTSTOP == "start-foreground" ]]; then
    exec "${FLINK_BIN_DIR}"/flink-console.sh $ENTRYPOINT "${ARGS[@]}"
else
    if [[ $FLINK_TM_COMPUTE_NUMA == "false" ]]; then
        # Start a single TaskManager
        "${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP $ENTRYPOINT "${ARGS[@]}"
    else
        # Example output from `numactl --show` on an AWS c4.8xlarge:
        # policy: default
        # preferred node: current
        # physcpubind: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35
        # cpubind: 0 1
        # nodebind: 0 1
        # membind: 0 1
        read -ra NODE_LIST <<< $(numactl --show | grep "^nodebind: ")
        for NODE_ID in "${NODE_LIST[@]:1}"; do
            # Start a TaskManager for each NUMA node
            numactl --membind=$NODE_ID --cpunodebind=$NODE_ID -- "${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP $ENTRYPOINT "${ARGS[@]}"
        done
    fi
fi
