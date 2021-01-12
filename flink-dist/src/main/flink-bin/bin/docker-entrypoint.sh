#!/usr/bin/env bash

###############################################################################
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
###############################################################################

COMMAND_STANDALONE="standalone-job"
# Deprecated, should be remove in Flink release 1.13
COMMAND_NATIVE_KUBERNETES="native-k8s"
COMMAND_HISTORY_SERVER="history-server"
COMMAND_DISABLE_JEMALLOC="disable-jemalloc"

args=("$@")
echo "${args[@]}"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# FLINK_HOME is set by the docker image
export _FLINK_HOME_DETERMINED=true
. ${FLINK_HOME}/bin/config.sh

# If unspecified, the hostname of the container is taken as the JobManager address
JOB_MANAGER_RPC_ADDRESS=${JOB_MANAGER_RPC_ADDRESS:-$(hostname -f)}
CONF_FILE="${FLINK_CONF_DIR}/flink-conf.yaml"

copy_plugins_if_required() {
    if [ -z "$ENABLE_BUILT_IN_PLUGINS" ]; then
        return 0
    fi

    echo "Enabling required built-in plugins"
    for target_plugin in $(echo "$ENABLE_BUILT_IN_PLUGINS" | tr ';' ' '); do
        echo "Linking ${target_plugin} to plugin directory"
        plugin_name=${target_plugin%.jar}

        mkdir -p "${FLINK_PLUGINS_DIR}/${plugin_name}"
        if [ ! -e "${FLINK_OPT_DIR}/${target_plugin}" ]; then
            echo "Plugin ${target_plugin} does not exist. Exiting."
            exit 1
        else
            ln -fs "${FLINK_OPT_DIR}/${target_plugin}" "${FLINK_HOME}/plugins/${plugin_name}"
            echo "Successfully enabled ${target_plugin}"
        fi
    done
}

set_config_option() {
    local option=$1
    local value=$2

    # escape periods for usage in regular expressions
    local escaped_option=$(echo ${option} | sed -e "s/\./\\\./g")

    # either override an existing entry, or append a new one
    if grep -E "^${escaped_option}:.*" "${CONF_FILE}" > /dev/null; then
        sed -i -e "s/${escaped_option}:.*/$option: $value/g" "${CONF_FILE}"
    else
        echo "${option}: ${value}" >> "${CONF_FILE}"
    fi
}

set_common_options() {
    set_config_option jobmanager.rpc.address ${JOB_MANAGER_RPC_ADDRESS}
    set_config_option blob.server.port 6124
    set_config_option query.server.port 6125
}

prepare_job_manager_start() {
    echo "Starting Job Manager"
    copy_plugins_if_required

    set_common_options

    if [ -n "${FLINK_PROPERTIES}" ]; then
        echo "${FLINK_PROPERTIES}" >> "${CONF_FILE}"
    fi
    envsubst < "${CONF_FILE}" > "${CONF_FILE}.tmp" && mv "${CONF_FILE}.tmp" "${CONF_FILE}"
}

echo "${args[@]}"

if [ "$1" = "help" ]; then
    printf "Usage: $(basename "$0") (jobmanager|${COMMAND_STANDALONE}|taskmanager|${COMMAND_HISTORY_SERVER}) [${COMMAND_DISABLE_JEMALLOC}]\n"
    printf "    Or $(basename "$0") help\n\n"
    printf "By default, Flink image adopts jemalloc as default memory allocator and will disable jemalloc if option '${COMMAND_DISABLE_JEMALLOC}' given.\n"
    exit 0
elif [ "$1" = "jobmanager" ]; then
    args=("${args[@]:1}")

    prepare_job_manager_start

    exec "${FLINK_BIN_DIR}/jobmanager.sh" start-foreground "${args[@]}"
elif [ "$1" = ${COMMAND_STANDALONE} ]; then
    args=("${args[@]:1}")

    prepare_job_manager_start

    exec "${FLINK_BIN_DIR}/standalone-job.sh" start-foreground "${args[@]}"
elif [ "$1" = ${COMMAND_HISTORY_SERVER} ]; then
    args=("${args[@]:1}")

    echo "Starting History Server"
    copy_plugins_if_required

    if [ -n "${FLINK_PROPERTIES}" ]; then
        echo "${FLINK_PROPERTIES}" >> "${CONF_FILE}"
    fi
    envsubst < "${CONF_FILE}" > "${CONF_FILE}.tmp" && mv "${CONF_FILE}.tmp" "${CONF_FILE}"

    exec "${FLINK_BIN_DIR}/historyserver.sh" start-foreground "${args[@]}"
elif [ "$1" = "taskmanager" ]; then
    args=("${args[@]:1}")

    echo "Starting Task Manager"
    copy_plugins_if_required

    TASK_MANAGER_NUMBER_OF_TASK_SLOTS=${TASK_MANAGER_NUMBER_OF_TASK_SLOTS:-$(grep -c ^processor /proc/cpuinfo)}

    set_common_options
    set_config_option taskmanager.numberOfTaskSlots ${TASK_MANAGER_NUMBER_OF_TASK_SLOTS}

    if [ -n "${FLINK_PROPERTIES}" ]; then
        echo "${FLINK_PROPERTIES}" >> "${CONF_FILE}"
    fi
    envsubst < "${CONF_FILE}" > "${CONF_FILE}.tmp" && mv "${CONF_FILE}.tmp" "${CONF_FILE}"

    exec "${FLINK_BIN_DIR}/taskmanager.sh" start-foreground "${args[@]}"
elif [ "$1" = "$COMMAND_NATIVE_KUBERNETES" ]; then
    args=("${args[@]:1}")

    copy_plugins_if_required

    # Override classpath since it was assembled manually
    export FLINK_CLASSPATH="`constructFlinkClassPath`:${INTERNAL_HADOOP_CLASSPATHS}"
    # Start commands for jobmanager and taskmanager are generated by Flink internally.
    echo "Start command: ${args[@]}"
    exec bash -c "${args[@]}"
fi

copy_plugins_if_required

# Override classpath since it was assembled manually
export FLINK_CLASSPATH="`constructFlinkClassPath`:${INTERNAL_HADOOP_CLASSPATHS}"

# Running command in pass-through mode
exec "${args[@]}"
