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
USAGE="Usage: taskmanager.sh (start [batch|streaming])|stop|stop-all)"

STARTSTOP=$1
STREAMINGMODE=$2

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

if [[ $STARTSTOP == "start" ]]; then
    # Use batch mode as default
    if [ -z $STREAMINGMODE ]; then
        echo "Missing streaming mode (batch|streaming). Using 'batch'."
        STREAMINGMODE="batch"
    fi

    if [[ ! ${FLINK_TM_HEAP} =~ ${IS_NUMBER} ]]; then
        echo "[ERROR] Configured TaskManager JVM heap size is not a number. Please set '$KEY_TASKM_HEAP_MB' in $FLINK_CONF_FILE."
        exit 1
    fi

    if [ "$FLINK_TM_HEAP" -gt 0 ]; then
        export JVM_ARGS="$JVM_ARGS -Xms"$FLINK_TM_HEAP"m -Xmx"$FLINK_TM_HEAP"m"
    fi

    # Startup parameters
    args="--configDir ${FLINK_CONF_DIR} --streamingMode ${STREAMINGMODE}"
fi

${bin}/flink-daemon.sh $STARTSTOP taskmanager "${args}"
