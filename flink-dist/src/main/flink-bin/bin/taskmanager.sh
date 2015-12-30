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
USAGE="Usage: taskmanager.sh (start|stop|stop-all)"

STARTSTOP=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

if [[ $STARTSTOP == "start" ]]; then

    # if memory allocation mode is lazy and no other JVM options are set,
    # set the 'Concurrent Mark Sweep GC'
    if [[ $FLINK_TM_MEM_PRE_ALLOCATE == "false" ]] && [ -z $FLINK_ENV_JAVA_OPTS ]; then

        JAVA_VERSION=$($JAVA_RUN -version 2>&1 | sed 's/.*version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')

        # set the GC to G1 in Java 8 and to CMS in Java 7
        if [[ ${JAVA_VERSION} =~ ${IS_NUMBER} ]]; then
            if [ "$JAVA_VERSION" -lt 18 ]; then
                export JVM_ARGS="$JVM_ARGS -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled"
            else
                export JVM_ARGS="$JVM_ARGS -XX:+UseG1GC"
            fi
        fi
    fi

    if [[ ! ${FLINK_TM_HEAP} =~ ${IS_NUMBER} ]] || [[ "${FLINK_TM_HEAP}" -lt "0" ]]; then
        echo "[ERROR] Configured TaskManager JVM heap size is not a number. Please set '${KEY_TASKM_MEM_SIZE}' in ${FLINK_CONF_FILE}."
        exit 1
    fi

    if [ "${FLINK_TM_HEAP}" -gt "0" ]; then

        TM_HEAP_SIZE=${FLINK_TM_HEAP}
        # Long.MAX_VALUE in TB: This is an upper bound, much less direct memory will be used
        #
        TM_MAX_OFFHEAP_SIZE="8388607T"

        if [[ "${FLINK_TM_MEM_PRE_ALLOCATE}" == "true" ]] && useOffHeapMemory; then
            if [[ "${FLINK_TM_MEM_MANAGED_SIZE}" -gt "0" ]]; then
                # We split up the total memory in heap and off-heap memory
                if [[ "${FLINK_TM_HEAP}" -le "${FLINK_TM_MEM_MANAGED_SIZE}" ]]; then
                    echo "[ERROR] Configured TaskManager memory size ('${KEY_TASKM_MEM_SIZE}') must be larger than the managed memory size ('${KEY_TASKM_MEM_MANAGED_SIZE}')."
                    exit 1
                fi
                TM_HEAP_SIZE=$((FLINK_TM_HEAP - FLINK_TM_MEM_MANAGED_SIZE))
            else
                # Bash only performs integer arithmetic so floating point computation is performed using bc
                command -v bc >/dev/null 2>&1
                if [[ $? -ne 0 ]]; then
                    echo "[ERROR] Program 'bc' not found. Please install bc or define '${KEY_TASKM_MEM_MANAGED_SIZE}' instead of '${KEY_TASKM_MEM_MANAGED_FRACTION}' in ${FLINK_CONF_FILE}"
                    exit 1
                fi
                # We calculate the memory using a fraction of the total memory
                if [[ `bc -l <<< "${FLINK_TM_MEM_MANAGED_FRACTION} >= 1.0"` != "0" ]] || [[ `bc -l <<< "${FLINK_TM_MEM_MANAGED_FRACTION} <= 0.0"` != "0" ]]; then
                    echo "[ERROR] Configured TaskManager managed memory fraction is not a valid value. Please set '${KEY_TASKM_MEM_MANAGED_FRACTION}' in ${FLINK_CONF_FILE}"
                    exit 1
                fi
                # recalculate the JVM heap memory by taking the off-heap ratio into account
                OFFHEAP_MANAGED_MEMORY_SIZE=`printf '%.0f\n' $(bc -l <<< "${FLINK_TM_HEAP} * ${FLINK_TM_MEM_MANAGED_FRACTION}")`
                TM_HEAP_SIZE=$((FLINK_TM_HEAP - OFFHEAP_MANAGED_MEMORY_SIZE))
            fi
        fi

        export JVM_ARGS="${JVM_ARGS} -Xms${TM_HEAP_SIZE}M -Xmx${TM_HEAP_SIZE}M -XX:MaxDirectMemorySize=${TM_MAX_OFFHEAP_SIZE}"

    fi

    # Startup parameters
    args=("--configDir" "${FLINK_CONF_DIR}")
fi

"${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP taskmanager "${args[@]}"
