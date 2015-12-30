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
USAGE="Usage: jobmanager.sh (start (local|cluster) [host] [webui-port]|stop|stop-all)"

STARTSTOP=$1
EXECUTIONMODE=$2
STREAMINGMODE=$3
HOST=$4 # optional when starting multiple instances
WEBUIPORT=$5 # optinal when starting multiple instances

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

if [[ $STARTSTOP == "start" ]]; then
    if [ -z $EXECUTIONMODE ]; then
        echo "Missing execution mode (local|cluster) argument. $USAGE."
        exit 1
    fi

    if [[ ! ${FLINK_JM_HEAP} =~ $IS_NUMBER ]] || [[ "${FLINK_JM_HEAP}" -lt "0" ]]; then
        echo "[ERROR] Configured JobManager memory size is not a valid value. Please set '${KEY_JOBM_MEM_SIZE}' in ${FLINK_CONF_FILE}."
        exit 1
    fi

    if [ "$EXECUTIONMODE" = "local" ]; then
        if [[ ! ${FLINK_TM_HEAP} =~ $IS_NUMBER ]] || [[ "${FLINK_TM_HEAP}" -lt "0" ]]; then
            echo "[ERROR] Configured TaskManager memory size is not a valid value. Please set ${KEY_TASKM_MEM_SIZE} in ${FLINK_CONF_FILE}."
            exit 1
        fi

        FLINK_JM_HEAP=`expr $FLINK_JM_HEAP + $FLINK_TM_HEAP`
    fi

    if [ "${FLINK_JM_HEAP}" -gt "0" ]; then
        export JVM_ARGS="$JVM_ARGS -Xms"$FLINK_JM_HEAP"m -Xmx"$FLINK_JM_HEAP"m"
    fi

    # Startup parameters
    args=("--configDir" "${FLINK_CONF_DIR}" "--executionMode" "${EXECUTIONMODE}")
    if [ ! -z $HOST ]; then
        args+=("--host")
        args+=("${HOST}")
    fi

    if [ ! -z $WEBUIPORT ]; then
        args+=("--webui-port")
        args+=("${WEBUIPORT}")
    fi
fi

"${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP jobmanager "${args[@]}"
