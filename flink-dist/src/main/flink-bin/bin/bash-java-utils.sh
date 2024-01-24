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

readFromConfigFile() {
    local key=$1
    local defaultValue=$2
    local configFile=$3

    # first extract the value with the given key (1st sed), then trim the result (2nd sed)
    # if a key exists multiple times, take the "last" one (tail)
    local value=`sed -n "s/^[ ]*${key}[ ]*: \([^#]*\).*$/\1/p" "${configFile}" | sed "s/^ *//;s/ *$//" | tail -n 1`

    [ -z "$value" ] && echo "$defaultValue" || echo "$value"
}


setJavaHome() {
    # read JAVA_HOME from config with no default value
    # NOTE: we need to obtain JAVA_HOME before using BashJavaUtils, so the value for env.java.home must
    # be in a flattened format, rather than nested, allowing us to retrieve the corresponding value via
    # shell script.
    CONF_FILE="$1/flink-conf.yaml"
    if [ ! -e "$1/flink-conf.yaml" ]; then
        CONF_FILE="$1/config.yaml"
    fi;
    
    KEY_ENV_JAVA_HOME="env.java.home"
    MY_JAVA_HOME=$(readFromConfigFile ${KEY_ENV_JAVA_HOME} "" "${CONF_FILE}")
    # check if config specified JAVA_HOME
    if [ -z "${MY_JAVA_HOME}" ]; then
        # config did not specify JAVA_HOME. Use system JAVA_HOME
        MY_JAVA_HOME="${JAVA_HOME}"
    fi
    # check if we have a valid JAVA_HOME and if java is not available
    if [ -z "${MY_JAVA_HOME}" ] && ! type java > /dev/null 2> /dev/null; then
        echo "Please specify JAVA_HOME. Either in Flink config ./conf/config.yaml or as system-wide JAVA_HOME."
        exit 1
    else
        export JAVA_HOME="${MY_JAVA_HOME}"
    fi
}
  
setJavaRun() {
    setJavaHome "$1"

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
    export JAVA_RUN
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

findFlinkDistJar() {
    local FLINK_DIST
    local LIB_DIR
    if [[ -n "$1" ]]; then
       LIB_DIR="$1"
    else
       LIB_DIR="$FLINK_LIB_DIR"
    fi
    FLINK_DIST="$(find "$LIB_DIR" -name 'flink-dist*.jar')"
    local FLINK_DIST_COUNT
    FLINK_DIST_COUNT="$(echo "$FLINK_DIST" | wc -l)"

    # If flink-dist*.jar cannot be resolved write error messages to stderr since stdout is stored
    # as the classpath and exit function with empty classpath to force process failure
    if [[ "$FLINK_DIST" == "" ]]; then
        (>&2 echo "[ERROR] Flink distribution jar not found in $FLINK_LIB_DIR.")
        exit 1
    elif [[ "$FLINK_DIST_COUNT" -gt 1 ]]; then
        (>&2 echo "[ERROR] Multiple flink-dist*.jar found in $FLINK_LIB_DIR. Please resolve.")
        exit 1
    fi

    echo "$FLINK_DIST"
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

updateAndGetFlinkConfiguration() {
    local FLINK_CONF_DIR="$1"
    local FLINK_BIN_DIR="$2"
    local FLINK_LIB_DIR="$3"
    local command_result
    command_result=$(parseConfigurationAndExportLogs "$FLINK_CONF_DIR" "$FLINK_BIN_DIR" "$FLINK_LIB_DIR" "UPDATE_AND_GET_FLINK_CONFIGURATION" "${@:4}")
    echo "$command_result"
}

migrateLegacyFlinkConfigToStandardYaml() {
    local FLINK_CONF_DIR="$1"
    local FLINK_BIN_DIR="$2"
    local FLINK_LIB_DIR="$3"
    local command_result
    command_result=$(parseConfigurationAndExportLogs "$FLINK_CONF_DIR" "$FLINK_BIN_DIR" "$FLINK_LIB_DIR" "MIGRATE_LEGACY_FLINK_CONFIGURATION_TO_STANDARD_YAML")
    echo "$command_result"
}

parseConfigurationAndExportLogs() {
    local FLINK_CONF_DIR="$1"
    local FLINK_BIN_DIR="$2"
    local FLINK_LIB_DIR="$3"
    local COMMAND="$4"
    local EXECUTION_PREFIX="BASH_JAVA_UTILS_EXEC_RESULT:"

    java_utils_output=$(runBashJavaUtilsCmd "${COMMAND}" "${FLINK_CONF_DIR}" "${FLINK_BIN_DIR}/bash-java-utils.jar:$(findFlinkDistJar ${FLINK_LIB_DIR})" "${@:5}")
    logging_output=$(extractLoggingOutputs "${java_utils_output}")
    execution_results=$(echo "${java_utils_output}" | grep ${EXECUTION_PREFIX})

    if [[ $? -ne 0 ]]; then
      echo "[ERROR] Could not parse configurations properly."
      echo "[ERROR] Raw output from BashJavaUtils:"
      echo "$java_utils_output"
      exit 1
    fi

    echo "${execution_results//${EXECUTION_PREFIX}/}"
}

extractLoggingOutputs() {
    local output="$1"
    local EXECUTION_PREFIX="BASH_JAVA_UTILS_EXEC_RESULT:"

    echo "${output}" | grep -v ${EXECUTION_PREFIX}
}

extractExecutionResults() {
    local output="$1"
    local expected_lines="$2"
    local EXECUTION_PREFIX="BASH_JAVA_UTILS_EXEC_RESULT:"
    local execution_results
    local num_lines

    execution_results=$(echo "${output}" | grep ${EXECUTION_PREFIX})
    num_lines=$(echo "${execution_results}" | wc -l)
    # explicit check for empty result, because if execution_results is empty, then wc returns 1
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
