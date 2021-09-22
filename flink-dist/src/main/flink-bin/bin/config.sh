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

constructFlinkClassPath() {
    local FLINK_DIST
    local FLINK_CLASSPATH

    while read -d '' -r jarfile ; do
        if [[ "$jarfile" =~ .*/flink-dist[^/]*.jar$ ]]; then
            FLINK_DIST="$FLINK_DIST":"$jarfile"
        elif [[ "$FLINK_CLASSPATH" == "" ]]; then
            FLINK_CLASSPATH="$jarfile";
        else
            FLINK_CLASSPATH="$FLINK_CLASSPATH":"$jarfile"
        fi
    done < <(find "$FLINK_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)

    if [[ "$FLINK_DIST" == "" ]]; then
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] Flink distribution jar not found in $FLINK_LIB_DIR.")

        # exit function with empty classpath to force process failure
        exit 1
    fi

    echo "$FLINK_CLASSPATH""$FLINK_DIST"
}

findFlinkDistJar() {
    local FLINK_DIST="`find "$FLINK_LIB_DIR" -name 'flink-dist*.jar'`"

    if [[ "$FLINK_DIST" == "" ]]; then
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] Flink distribution jar not found in $FLINK_LIB_DIR.")

        # exit function with empty classpath to force process failure
        exit 1
    fi

    echo "$FLINK_DIST"
}

# These are used to mangle paths that are passed to java when using
# cygwin. Cygwin paths are like linux paths, i.e. /path/to/somewhere
# but the windows java version expects them in Windows Format, i.e. C:\bla\blub.
# "cygpath" can do the conversion.
manglePath() {
    UNAME=$(uname -s)
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -w "$1"`
    else
        echo $1
    fi
}

manglePathList() {
    UNAME=$(uname -s)
    # a path list, for example a java classpath
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -wp "$1"`
    else
        echo $1
    fi
}

# Looks up a config value by key from a simple YAML-style key-value map.
# $1: key to look up
# $2: default value to return if key does not exist
# $3: config file to read from
readFromConfig() {
    local key=$1
    local defaultValue=$2
    local configFile=$3

    # first extract the value with the given key (1st sed), then trim the result (2nd sed)
    # if a key exists multiple times, take the "last" one (tail)
    local value=`sed -n "s/^[ ]*${key}[ ]*: \([^#]*\).*$/\1/p" "${configFile}" | sed "s/^ *//;s/ *$//" | tail -n 1`

    [ -z "$value" ] && echo "$defaultValue" || echo "$value"
}

########################################################################################################################
# DEFAULT CONFIG VALUES: These values will be used when nothing has been specified in conf/flink-conf.yaml
# -or- the respective environment variables are not set.
########################################################################################################################


# WARNING !!! , these values are only used if there is nothing else is specified in
# conf/flink-conf.yaml

DEFAULT_ENV_PID_DIR="/tmp"                          # Directory to store *.pid files to
DEFAULT_ENV_LOG_MAX=10                              # Maximum number of old log files to keep
DEFAULT_ENV_JAVA_OPTS=""                            # Optional JVM args
DEFAULT_ENV_JAVA_OPTS_JM=""                         # Optional JVM args (JobManager)
DEFAULT_ENV_JAVA_OPTS_TM=""                         # Optional JVM args (TaskManager)
DEFAULT_ENV_JAVA_OPTS_HS=""                         # Optional JVM args (HistoryServer)
DEFAULT_ENV_JAVA_OPTS_CLI=""                        # Optional JVM args (Client)
DEFAULT_ENV_SSH_OPTS=""                             # Optional SSH parameters running in cluster mode
DEFAULT_YARN_CONF_DIR=""                            # YARN Configuration Directory, if necessary
DEFAULT_HADOOP_CONF_DIR=""                          # Hadoop Configuration Directory, if necessary
DEFAULT_HBASE_CONF_DIR=""                           # HBase Configuration Directory, if necessary

########################################################################################################################
# CONFIG KEYS: The default values can be overwritten by the following keys in conf/flink-conf.yaml
########################################################################################################################

KEY_TASKM_COMPUTE_NUMA="taskmanager.compute.numa"

KEY_ENV_PID_DIR="env.pid.dir"
KEY_ENV_LOG_DIR="env.log.dir"
KEY_ENV_LOG_MAX="env.log.max"
KEY_ENV_YARN_CONF_DIR="env.yarn.conf.dir"
KEY_ENV_HADOOP_CONF_DIR="env.hadoop.conf.dir"
KEY_ENV_HBASE_CONF_DIR="env.hbase.conf.dir"
KEY_ENV_JAVA_HOME="env.java.home"
KEY_ENV_JAVA_OPTS="env.java.opts"
KEY_ENV_JAVA_OPTS_JM="env.java.opts.jobmanager"
KEY_ENV_JAVA_OPTS_TM="env.java.opts.taskmanager"
KEY_ENV_JAVA_OPTS_HS="env.java.opts.historyserver"
KEY_ENV_JAVA_OPTS_CLI="env.java.opts.client"
KEY_ENV_SSH_OPTS="env.ssh.opts"
KEY_HIGH_AVAILABILITY="high-availability"
KEY_ZK_HEAP_MB="zookeeper.heap.mb"

########################################################################################################################
# PATHS AND CONFIG
########################################################################################################################

target="$0"
# For the case, the executable has been directly symlinked, figure out
# the correct bin path by following its symlink up to an upper bound.
# Note: we can't use the readlink utility here if we want to be POSIX
# compatible.
iteration=0
while [ -L "$target" ]; do
    if [ "$iteration" -gt 100 ]; then
        echo "Cannot resolve path: You have a cyclic symlink in $target."
        break
    fi
    ls=`ls -ld -- "$target"`
    target=`expr "$ls" : '.* -> \(.*\)$'`
    iteration=$((iteration + 1))
done

# Convert relative path to absolute path and resolve directory symlinks
bin=`dirname "$target"`
SYMLINK_RESOLVED_BIN=`cd "$bin"; pwd -P`

# Define the main directory of the flink installation
# If config.sh is called by pyflink-shell.sh in python bin directory(pip installed), then do not need to set the FLINK_HOME here.
if [ -z "$_FLINK_HOME_DETERMINED" ]; then
    FLINK_HOME=`dirname "$SYMLINK_RESOLVED_BIN"`
fi
if [ -z "$FLINK_LIB_DIR" ]; then FLINK_LIB_DIR=$FLINK_HOME/lib; fi
if [ -z "$FLINK_PLUGINS_DIR" ]; then FLINK_PLUGINS_DIR=$FLINK_HOME/plugins; fi
if [ -z "$FLINK_OPT_DIR" ]; then FLINK_OPT_DIR=$FLINK_HOME/opt; fi


# These need to be mangled because they are directly passed to java.
# The above lib path is used by the shell script to retrieve jars in a
# directory, so it needs to be unmangled.
FLINK_HOME_DIR_MANGLED=`manglePath "$FLINK_HOME"`
if [ -z "$FLINK_CONF_DIR" ]; then FLINK_CONF_DIR=$FLINK_HOME_DIR_MANGLED/conf; fi
FLINK_BIN_DIR=$FLINK_HOME_DIR_MANGLED/bin
DEFAULT_FLINK_LOG_DIR=$FLINK_HOME_DIR_MANGLED/log
FLINK_CONF_FILE="flink-conf.yaml"
YAML_CONF=${FLINK_CONF_DIR}/${FLINK_CONF_FILE}

### Exported environment variables ###
export FLINK_CONF_DIR
export FLINK_BIN_DIR
export FLINK_PLUGINS_DIR
# export /lib dir to access it during deployment of the Yarn staging files
export FLINK_LIB_DIR
# export /opt dir to access it for the SQL client
export FLINK_OPT_DIR

########################################################################################################################
# ENVIRONMENT VARIABLES
########################################################################################################################

# read JAVA_HOME from config with no default value
MY_JAVA_HOME=$(readFromConfig ${KEY_ENV_JAVA_HOME} "" "${YAML_CONF}")
# check if config specified JAVA_HOME
if [ -z "${MY_JAVA_HOME}" ]; then
    # config did not specify JAVA_HOME. Use system JAVA_HOME
    MY_JAVA_HOME="${JAVA_HOME}"
fi
# check if we have a valid JAVA_HOME and if java is not available
if [ -z "${MY_JAVA_HOME}" ] && ! type java > /dev/null 2> /dev/null; then
    echo "Please specify JAVA_HOME. Either in Flink config ./conf/flink-conf.yaml or as system-wide JAVA_HOME."
    exit 1
else
    JAVA_HOME="${MY_JAVA_HOME}"
fi

UNAME=$(uname -s)
if [ "${UNAME:0:6}" == "CYGWIN" ]; then
    JAVA_RUN=java
else
    if [[ -d "$JAVA_HOME" ]]; then
        JAVA_RUN="$JAVA_HOME"/bin/java
    else
        JAVA_RUN=java
    fi
fi

# Define HOSTNAME if it is not already set
if [ -z "${HOSTNAME}" ]; then
    HOSTNAME=`hostname`
fi

IS_NUMBER="^[0-9]+$"

# Verify that NUMA tooling is available
command -v numactl >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    FLINK_TM_COMPUTE_NUMA="false"
else
    # Define FLINK_TM_COMPUTE_NUMA if it is not already set
    if [ -z "${FLINK_TM_COMPUTE_NUMA}" ]; then
        FLINK_TM_COMPUTE_NUMA=$(readFromConfig ${KEY_TASKM_COMPUTE_NUMA} "false" "${YAML_CONF}")
    fi
fi

if [ -z "${MAX_LOG_FILE_NUMBER}" ]; then
    MAX_LOG_FILE_NUMBER=$(readFromConfig ${KEY_ENV_LOG_MAX} ${DEFAULT_ENV_LOG_MAX} "${YAML_CONF}")
    export MAX_LOG_FILE_NUMBER
fi

if [ -z "${FLINK_LOG_DIR}" ]; then
    FLINK_LOG_DIR=$(readFromConfig ${KEY_ENV_LOG_DIR} "${DEFAULT_FLINK_LOG_DIR}" "${YAML_CONF}")
fi

if [ -z "${YARN_CONF_DIR}" ]; then
    YARN_CONF_DIR=$(readFromConfig ${KEY_ENV_YARN_CONF_DIR} "${DEFAULT_YARN_CONF_DIR}" "${YAML_CONF}")
fi

if [ -z "${HADOOP_CONF_DIR}" ]; then
    HADOOP_CONF_DIR=$(readFromConfig ${KEY_ENV_HADOOP_CONF_DIR} "${DEFAULT_HADOOP_CONF_DIR}" "${YAML_CONF}")
fi

if [ -z "${HBASE_CONF_DIR}" ]; then
    HBASE_CONF_DIR=$(readFromConfig ${KEY_ENV_HBASE_CONF_DIR} "${DEFAULT_HBASE_CONF_DIR}" "${YAML_CONF}")
fi

if [ -z "${FLINK_PID_DIR}" ]; then
    FLINK_PID_DIR=$(readFromConfig ${KEY_ENV_PID_DIR} "${DEFAULT_ENV_PID_DIR}" "${YAML_CONF}")
fi

if [ -z "${FLINK_ENV_JAVA_OPTS}" ]; then
    FLINK_ENV_JAVA_OPTS=$(readFromConfig ${KEY_ENV_JAVA_OPTS} "${DEFAULT_ENV_JAVA_OPTS}" "${YAML_CONF}")

    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS="$( echo "${FLINK_ENV_JAVA_OPTS}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_JM}" ]; then
    FLINK_ENV_JAVA_OPTS_JM=$(readFromConfig ${KEY_ENV_JAVA_OPTS_JM} "${DEFAULT_ENV_JAVA_OPTS_JM}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_JM="$( echo "${FLINK_ENV_JAVA_OPTS_JM}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_TM}" ]; then
    FLINK_ENV_JAVA_OPTS_TM=$(readFromConfig ${KEY_ENV_JAVA_OPTS_TM} "${DEFAULT_ENV_JAVA_OPTS_TM}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_TM="$( echo "${FLINK_ENV_JAVA_OPTS_TM}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_HS}" ]; then
    FLINK_ENV_JAVA_OPTS_HS=$(readFromConfig ${KEY_ENV_JAVA_OPTS_HS} "${DEFAULT_ENV_JAVA_OPTS_HS}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_HS="$( echo "${FLINK_ENV_JAVA_OPTS_HS}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_ENV_JAVA_OPTS_CLI}" ]; then
    FLINK_ENV_JAVA_OPTS_CLI=$(readFromConfig ${KEY_ENV_JAVA_OPTS_CLI} "${DEFAULT_ENV_JAVA_OPTS_CLI}" "${YAML_CONF}")
    # Remove leading and ending double quotes (if present) of value
    FLINK_ENV_JAVA_OPTS_CLI="$( echo "${FLINK_ENV_JAVA_OPTS_CLI}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${FLINK_SSH_OPTS}" ]; then
    FLINK_SSH_OPTS=$(readFromConfig ${KEY_ENV_SSH_OPTS} "${DEFAULT_ENV_SSH_OPTS}" "${YAML_CONF}")
fi

# Define ZK_HEAP if it is not already set
if [ -z "${ZK_HEAP}" ]; then
    ZK_HEAP=$(readFromConfig ${KEY_ZK_HEAP_MB} 0 "${YAML_CONF}")
fi

# High availability
if [ -z "${HIGH_AVAILABILITY}" ]; then
     HIGH_AVAILABILITY=$(readFromConfig ${KEY_HIGH_AVAILABILITY} "" "${YAML_CONF}")
     if [ -z "${HIGH_AVAILABILITY}" ]; then
        # Try deprecated value
        DEPRECATED_HA=$(readFromConfig "recovery.mode" "" "${YAML_CONF}")
        if [ -z "${DEPRECATED_HA}" ]; then
            HIGH_AVAILABILITY="none"
        elif [ ${DEPRECATED_HA} == "standalone" ]; then
            # Standalone is now 'none'
            HIGH_AVAILABILITY="none"
        else
            HIGH_AVAILABILITY=${DEPRECATED_HA}
        fi
     fi
fi

# Arguments for the JVM. Used for job and task manager JVMs.
# DO NOT USE FOR MEMORY SETTINGS! Use conf/flink-conf.yaml with keys
# JobManagerOptions#TOTAL_PROCESS_MEMORY and TaskManagerOptions#TOTAL_PROCESS_MEMORY for that!
if [ -z "${JVM_ARGS}" ]; then
    JVM_ARGS=""
fi

# Check if deprecated HADOOP_HOME is set, and specify config path to HADOOP_CONF_DIR if it's empty.
if [ -z "$HADOOP_CONF_DIR" ]; then
    if [ -n "$HADOOP_HOME" ]; then
        # HADOOP_HOME is set. Check if its a Hadoop 1.x or 2.x HADOOP_HOME path
        if [ -d "$HADOOP_HOME/conf" ]; then
            # It's Hadoop 1.x
            HADOOP_CONF_DIR="$HADOOP_HOME/conf"
        fi
        if [ -d "$HADOOP_HOME/etc/hadoop" ]; then
            # It's Hadoop 2.2+
            HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
        fi
    fi
fi

# if neither HADOOP_CONF_DIR nor HADOOP_CLASSPATH are set, use some common default (if available)
if [ -z "$HADOOP_CONF_DIR" ] && [ -z "$HADOOP_CLASSPATH" ]; then
    if [ -d "/etc/hadoop/conf" ]; then
        echo "Setting HADOOP_CONF_DIR=/etc/hadoop/conf because no HADOOP_CONF_DIR or HADOOP_CLASSPATH was set."
        HADOOP_CONF_DIR="/etc/hadoop/conf"
    fi
fi

# Check if deprecated HBASE_HOME is set, and specify config path to HBASE_CONF_DIR if it's empty.
if [ -z "$HBASE_CONF_DIR" ]; then
    if [ -n "$HBASE_HOME" ]; then
        # HBASE_HOME is set.
        if [ -d "$HBASE_HOME/conf" ]; then
            HBASE_CONF_DIR="$HBASE_HOME/conf"
        fi
    fi
fi

# try and set HBASE_CONF_DIR to some common default if it's not set
if [ -z "$HBASE_CONF_DIR" ]; then
    if [ -d "/etc/hbase/conf" ]; then
        echo "Setting HBASE_CONF_DIR=/etc/hbase/conf because no HBASE_CONF_DIR was set."
        HBASE_CONF_DIR="/etc/hbase/conf"
    fi
fi

INTERNAL_HADOOP_CLASSPATHS="${HADOOP_CLASSPATH}:${HADOOP_CONF_DIR}:${YARN_CONF_DIR}"

if [ -n "${HBASE_CONF_DIR}" ]; then
    INTERNAL_HADOOP_CLASSPATHS="${INTERNAL_HADOOP_CLASSPATHS}:${HBASE_CONF_DIR}"
fi

# Auxilliary function which extracts the name of host from a line which
# also potentially includes topology information and the taskManager type
extractHostName() {
    # handle comments: extract first part of string (before first # character)
    WORKER=`echo $1 | cut -d'#' -f 1`

    # Extract the hostname from the network hierarchy
    if [[ "$WORKER" =~ ^.*/([0-9a-zA-Z.-]+)$ ]]; then
            WORKER=${BASH_REMATCH[1]}
    fi

    echo $WORKER
}

readMasters() {
    MASTERS_FILE="${FLINK_CONF_DIR}/masters"

    if [[ ! -f "${MASTERS_FILE}" ]]; then
        echo "No masters file. Please specify masters in 'conf/masters'."
        exit 1
    fi

    MASTERS=()
    WEBUIPORTS=()

    MASTERS_ALL_LOCALHOST=true
    GOON=true
    while $GOON; do
        read line || GOON=false
        HOSTWEBUIPORT=$( extractHostName $line)

        if [ -n "$HOSTWEBUIPORT" ]; then
            HOST=$(echo $HOSTWEBUIPORT | cut -f1 -d:)
            WEBUIPORT=$(echo $HOSTWEBUIPORT | cut -s -f2 -d:)
            MASTERS+=(${HOST})

            if [ -z "$WEBUIPORT" ]; then
                WEBUIPORTS+=(0)
            else
                WEBUIPORTS+=(${WEBUIPORT})
            fi

            if [ "${HOST}" != "localhost" ] && [ "${HOST}" != "127.0.0.1" ] ; then
                MASTERS_ALL_LOCALHOST=false
            fi
        fi
    done < "$MASTERS_FILE"
}

readWorkers() {
    WORKERS_FILE="${FLINK_CONF_DIR}/workers"

    if [[ ! -f "$WORKERS_FILE" ]]; then
        echo "No workers file. Please specify workers in 'conf/workers'."
        exit 1
    fi

    WORKERS=()

    WORKERS_ALL_LOCALHOST=true
    GOON=true
    while $GOON; do
        read line || GOON=false
        HOST=$( extractHostName $line)
        if [ -n "$HOST" ] ; then
            WORKERS+=(${HOST})
            if [ "${HOST}" != "localhost" ] && [ "${HOST}" != "127.0.0.1" ] ; then
                WORKERS_ALL_LOCALHOST=false
            fi
        fi
    done < "$WORKERS_FILE"
}

# starts or stops TMs on all workers
# TMWorkers start|stop
TMWorkers() {
    CMD=$1

    readWorkers

    if [ ${WORKERS_ALL_LOCALHOST} = true ] ; then
        # all-local setup
        for worker in ${WORKERS[@]}; do
            "${FLINK_BIN_DIR}"/taskmanager.sh "${CMD}"
        done
    else
        # non-local setup
        # start/stop TaskManager instance(s) using pdsh (Parallel Distributed Shell) when available
        command -v pdsh >/dev/null 2>&1
        if [[ $? -ne 0 ]]; then
            for worker in ${WORKERS[@]}; do
                ssh -n $FLINK_SSH_OPTS $worker -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/taskmanager.sh\" \"${CMD}\" &"
            done
        else
            PDSH_SSH_ARGS="" PDSH_SSH_ARGS_APPEND=$FLINK_SSH_OPTS pdsh -w $(IFS=, ; echo "${WORKERS[*]}") \
                "nohup /bin/bash -l \"${FLINK_BIN_DIR}/taskmanager.sh\" \"${CMD}\""
        fi
    fi
}

runBashJavaUtilsCmd() {
    local cmd=$1
    local conf_dir=$2
    local class_path=$3
    local dynamic_args=${@:4}
    class_path=`manglePathList "${class_path}"`

    local output=`"${JAVA_RUN}" -classpath "${class_path}" org.apache.flink.runtime.util.bash.BashJavaUtils ${cmd} --configDir "${conf_dir}" $dynamic_args 2>&1 | tail -n 1000`
    if [[ $? -ne 0 ]]; then
        echo "[ERROR] Cannot run BashJavaUtils to execute command ${cmd}." 1>&2
        # Print the output in case the user redirect the log to console.
        echo "$output" 1>&2
        exit 1
    fi

    echo "$output"
}

extractExecutionResults() {
    local output="$1"
    local expected_lines="$2"
    local EXECUTION_PREFIX="BASH_JAVA_UTILS_EXEC_RESULT:"
    local execution_results
    local num_lines

    execution_results=$(echo "${output}" | grep ${EXECUTION_PREFIX})
    num_lines=$(echo "${execution_results}" | wc -l)
    # explicit check for empty result, becuase if execution_results is empty, then wc returns 1
    if [[ -z ${execution_results} ]]; then
        echo "[ERROR] The execution result is empty." 1>&2
        exit 1
    fi
    if [[ ${num_lines} -ne ${expected_lines} ]]; then
        echo "[ERROR] The execution results has unexpected number of lines, expected: ${expected_lines}, actual: ${num_lines}." 1>&2
        echo "[ERROR] An execution result line is expected following the prefix '${EXECUTION_PREFIX}'" 1>&2
        echo "$output" 1>&2
        exit 1
    fi

    echo "${execution_results//${EXECUTION_PREFIX}/}"
}

extractLoggingOutputs() {
    local output="$1"
    local EXECUTION_PREFIX="BASH_JAVA_UTILS_EXEC_RESULT:"

    echo "${output}" | grep -v ${EXECUTION_PREFIX}
}

parseResourceParamsAndExportLogs() {
  local cmd=$1
  java_utils_output=$(runBashJavaUtilsCmd ${cmd} "${FLINK_CONF_DIR}" "${FLINK_BIN_DIR}/bash-java-utils.jar:$(findFlinkDistJar)" "${@:2}")
  logging_output=$(extractLoggingOutputs "${java_utils_output}")
  params_output=$(extractExecutionResults "${java_utils_output}" 2)

  if [[ $? -ne 0 ]]; then
    echo "[ERROR] Could not get JVM parameters and dynamic configurations properly."
    echo "[ERROR] Raw output from BashJavaUtils:"
    echo "$java_utils_output"
    exit 1
  fi

  jvm_params=$(echo "${params_output}" | head -n1)
  export JVM_ARGS="${JVM_ARGS} ${jvm_params}"
  export DYNAMIC_PARAMETERS=$(IFS=" " echo "${params_output}" | tail -n1)

  export FLINK_INHERITED_LOGS="
$FLINK_INHERITED_LOGS

RESOURCE_PARAMS extraction logs:
jvm_params: $jvm_params
dynamic_configs: $DYNAMIC_PARAMETERS
logs: $logging_output
"
}

parseJmArgsAndExportLogs() {
  parseResourceParamsAndExportLogs GET_JM_RESOURCE_PARAMS "$@"
}

parseTmArgsAndExportLogs() {
  parseResourceParamsAndExportLogs GET_TM_RESOURCE_PARAMS "$@"
}
