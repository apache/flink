#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Enable this line when developing a new end-to-end test
#set -Eexuo pipefail
set -o pipefail

if [[ -z $FLINK_DIR ]]; then
    echo "FLINK_DIR needs to point to a Flink distribution directory"
    exit 1
fi

case "$(uname -s)" in
    Linux*)     OS_TYPE=linux;;
    Darwin*)    OS_TYPE=mac;;
    CYGWIN*)    OS_TYPE=cygwin;;
    MINGW*)     OS_TYPE=mingw;;
    *)          OS_TYPE="UNKNOWN:${unameOut}"
esac

export EXIT_CODE=0
export TASK_SLOTS_PER_TM_HA=4

echo "Flink dist directory: $FLINK_DIR"

TEST_ROOT=`pwd -P`
TEST_INFRA_DIR="$END_TO_END_DIR/test-scripts/"
cd $TEST_INFRA_DIR
TEST_INFRA_DIR=`pwd -P`
cd $TEST_ROOT

source "${TEST_INFRA_DIR}/common_utils.sh"

NODENAME=${NODENAME:-`hostname -f`}

# REST_PROTOCOL and CURL_SSL_ARGS can be modified in common_ssl.sh if SSL is activated
# they should be used in curl command to query Flink REST API
REST_PROTOCOL="http"
CURL_SSL_ARGS=""
source "${TEST_INFRA_DIR}/common_ssl.sh"

function set_hadoop_classpath {
  YARN_CLASSPATH_LOCATION="${TEST_INFRA_DIR}/hadoop/yarn.classpath";
  if [ ! -f $YARN_CLASSPATH_LOCATION ]; then
    echo "File '$YARN_CLASSPATH_LOCATION' does not exist."
    exit 1
  fi
  export HADOOP_CLASSPATH=`cat $YARN_CLASSPATH_LOCATION`
}

function print_mem_use_osx {
    declare -a mem_types=("active" "inactive" "wired down")
    used=""
    for mem_type in "${mem_types[@]}"
    do
       used_type=$(vm_stat | grep "Pages ${mem_type}:" | awk '{print $NF}' | rev | cut -c 2- | rev)
       let used_type="(${used_type}*4096)/1024/1024"
       used="$used $mem_type=${used_type}MB"
    done
    let mem=$(sysctl -n hw.memsize)/1024/1024
    echo "Memory Usage: ${used} total=${mem}MB"
}

function print_mem_use {
    if [[ "$OS_TYPE" == "mac" ]]; then
        print_mem_use_osx
    else
        free -m | awk 'NR==2{printf "Memory Usage: used=%sMB total=%sMB %.2f%%\n", $3,$2,$3*100/$2 }'
    fi
}

BACKUP_FLINK_DIRS="conf lib plugins"

function backup_flink_dir() {
    mkdir -p "${TEST_DATA_DIR}/tmp/backup"
    # Note: not copying all directory tree, as it may take some time on some file systems.
    for dirname in ${BACKUP_FLINK_DIRS}; do
        cp -r "${FLINK_DIR}/${dirname}" "${TEST_DATA_DIR}/tmp/backup/"
    done
}

function revert_flink_dir() {

    for dirname in ${BACKUP_FLINK_DIRS}; do
        if [ -d "${TEST_DATA_DIR}/tmp/backup/${dirname}" ]; then
            rm -rf "${FLINK_DIR}/${dirname}"
            mv "${TEST_DATA_DIR}/tmp/backup/${dirname}" "${FLINK_DIR}/"
        fi
    done

    rm -r "${TEST_DATA_DIR}/tmp/backup"

    REST_PROTOCOL="http"
    CURL_SSL_ARGS=""
}

function setup_flink_shaded_zookeeper() {
  local version=$1
  # if it is already in lib we don't have to do anything
  if ! [ -e "${FLINK_DIR}"/lib/flink-shaded-zookeeper-${version}* ]; then
    if ! [ -e "${FLINK_DIR}"/opt/flink-shaded-zookeeper-${version}* ]; then
      echo "Could not find ZK ${version} in opt or lib."
      exit 1
    else
      # contents of 'opt' must not be changed since it is not backed up in common.sh#backup_flink_dir
      # it is fine to delete jars from 'lib' since it is backed up and will be restored after the test
      rm "${FLINK_DIR}"/lib/flink-shaded-zookeeper-*
      cp "${FLINK_DIR}"/opt/flink-shaded-zookeeper-${version}* "${FLINK_DIR}/lib"
    fi
  fi
}

function add_optional_lib() {
    local lib_name=$1
    cp "$FLINK_DIR/opt/flink-${lib_name}"*".jar" "$FLINK_DIR/lib"
}

function add_optional_plugin() {
    # This is similar to add_optional_lib, but the jar would be copied to
    # Flink's plugins dir (the nested folder name does not matter).
    # Note: this may not work with some jars, as not all of them implement plugin api.
    # Please check the corresponding code of the jar.
    local plugin="$1"
    local plugin_dir="$FLINK_DIR/plugins/$plugin"

    mkdir -p "$plugin_dir"
    cp "$FLINK_DIR/opt/flink-$plugin"*".jar" "$plugin_dir"
}

function delete_config_key() {
    local config_key=$1
    sed -i -e "/^${config_key}: /d" ${FLINK_DIR}/conf/flink-conf.yaml
}

function set_config_key() {
    local config_key=$1
    local value=$2
    delete_config_key ${config_key}
    echo "$config_key: $value" >> $FLINK_DIR/conf/flink-conf.yaml
}

function create_ha_config() {

    # clean up the dir that will be used for zookeeper storage
    # (see high-availability.zookeeper.storageDir below)
    if [ -e $TEST_DATA_DIR/recovery ]; then
       echo "File ${TEST_DATA_DIR}/recovery exists. Deleting it..."
       rm -rf $TEST_DATA_DIR/recovery
    fi

    # create the masters file (only one currently).
    # This must have all the masters to be used in HA.
    echo "localhost:8081" > ${FLINK_DIR}/conf/masters

    # then move on to create the flink-conf.yaml
    sed 's/^    //g' > ${FLINK_DIR}/conf/flink-conf.yaml << EOL
    #==============================================================================
    # Common
    #==============================================================================

    jobmanager.rpc.address: localhost
    jobmanager.rpc.port: 6123
    jobmanager.memory.process.size: 1024m
    taskmanager.memory.process.size: 1024m
    taskmanager.numberOfTaskSlots: ${TASK_SLOTS_PER_TM_HA}

    #==============================================================================
    # High Availability
    #==============================================================================

    high-availability: zookeeper
    high-availability.zookeeper.storageDir: file://${TEST_DATA_DIR}/recovery/
    high-availability.zookeeper.quorum: localhost:2181
    high-availability.zookeeper.path.root: /flink
    high-availability.cluster-id: /test_cluster_one

    #==============================================================================
    # Web Frontend
    #==============================================================================

    rest.port: 8081

    queryable-state.server.ports: 9000-9009
    queryable-state.proxy.ports: 9010-9019
EOL
}

function get_node_ip {
    local ip_addr

    if [[ ${OS_TYPE} == "linux" ]]; then
        ip_addr=$(hostname -I)
    elif [[ ${OS_TYPE} == "mac" ]]; then
        ip_addr=$(
            ifconfig |
            grep -E "([0-9]{1,3}\.){3}[0-9]{1,3}" | # grep IPv4 addresses only
            grep -v 127.0.0.1 |                     # do not use 127.0.0.1 (to be consistent with hostname -I)
            awk '{ print $2 }' |                    # extract ip from row
            paste -sd " " -                         # combine everything to one line
        )
    else
        echo "Warning: Unsupported OS_TYPE '${OS_TYPE}' for 'get_node_ip'. Falling back to 'hostname -I' (linux)"
        ip_addr=$(hostname -I)
    fi

    echo ${ip_addr}
}

function start_ha_cluster {
    create_ha_config
    start_local_zk
    start_cluster
}

function start_local_zk {
    # Parses the zoo.cfg and starts locally zk.

    # This is almost the same code as the
    # /bin/start-zookeeper-quorum.sh without the SSH part and only running for localhost.

    while read server ; do
        server=$(echo -e "${server}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//') # trim

        # match server.id=address[:port[:port]]
        if [[ $server =~ ^server\.([0-9]+)[[:space:]]*\=[[:space:]]*([^: \#]+) ]]; then
            id=${BASH_REMATCH[1]}
            address=${BASH_REMATCH[2]}

            if [ "${address}" != "localhost" ]; then
                echo "[ERROR] Parse error. Only available for localhost. Expected address 'localhost' but got '${address}'"
                exit 1
            fi
            ${FLINK_DIR}/bin/zookeeper.sh start $id
        else
            echo "[WARN] Parse error. Skipping config entry '$server'."
        fi
    done < <(grep "^server\." "${FLINK_DIR}/conf/zoo.cfg")
}

function wait_rest_endpoint_up {
  local query_url=$1
  local endpoint_name=$2
  local successful_response_regex=$3
  # wait at most 10 seconds until the endpoint is up
  local TIMEOUT=20
  for i in $(seq 1 ${TIMEOUT}); do
    # without the || true this would exit our script if the endpoint is not yet up
    QUERY_RESULT=$(curl ${CURL_SSL_ARGS} "$query_url" 2> /dev/null || true)

    # ensure the response adapts with the suceessful regex
    if [[ ${QUERY_RESULT} =~ ${successful_response_regex} ]]; then
      echo "${endpoint_name} REST endpoint is up."
      return
    fi

    echo "Waiting for ${endpoint_name} REST endpoint to come up..."
    sleep 1
  done
  echo "${endpoint_name} REST endpoint has not started within a timeout of ${TIMEOUT} sec"
  exit 1
}

function wait_dispatcher_running {
  local query_url="${REST_PROTOCOL}://${NODENAME}:8081/taskmanagers"
  wait_rest_endpoint_up "${query_url}" "Dispatcher" "\{\"taskmanagers\":\[.+\]\}"
}

function start_cluster {
  "$FLINK_DIR"/bin/start-cluster.sh
  wait_dispatcher_running
}

function start_taskmanagers {
    local tmnum=$1
    local c

    echo "Start ${tmnum} more task managers"
    for (( c=0; c<tmnum; c++ ))
    do
        $FLINK_DIR/bin/taskmanager.sh start
    done
}

function start_and_wait_for_tm {
  tm_query_result=`query_running_tms`
  # we assume that the cluster is running
  if ! [[ ${tm_query_result} =~ \{\"taskmanagers\":\[.*\]\} ]]; then
    echo "Your cluster seems to be unresponsive at the moment: ${tm_query_result}" 1>&2
    exit 1
  fi

  running_tms=`query_number_of_running_tms`
  ${FLINK_DIR}/bin/taskmanager.sh start
  wait_for_number_of_running_tms $((running_tms+1))
}

function query_running_tms {
  local url="${REST_PROTOCOL}://${NODENAME}:8081/taskmanagers"
  curl ${CURL_SSL_ARGS} -s "${url}"
}

function query_number_of_running_tms {
  query_running_tms | grep -o "id" | wc -l
}

function wait_for_number_of_running_tms {
  local TM_NUM_TO_WAIT=${1}
  local TIMEOUT_COUNTER=10
  local TIMEOUT_INC=4
  local TIMEOUT=$(( $TIMEOUT_COUNTER * $TIMEOUT_INC ))
  local TM_NUM_TEXT="Number of running task managers"
  for i in $(seq 1 ${TIMEOUT_COUNTER}); do
    local TM_NUM=`query_number_of_running_tms`
    if [ $((TM_NUM - TM_NUM_TO_WAIT)) -eq 0 ]; then
      echo "${TM_NUM_TEXT} has reached ${TM_NUM_TO_WAIT}."
      return
    else
      echo "${TM_NUM_TEXT} ${TM_NUM} is not yet ${TM_NUM_TO_WAIT}."
    fi
    sleep ${TIMEOUT_INC}
  done
  echo "${TM_NUM_TEXT} has not reached ${TM_NUM_TO_WAIT} within a timeout of ${TIMEOUT} sec"
  exit 1
}

function check_logs_for_errors {
  echo "Checking for errors..."
  error_count=$(grep -rv "GroupCoordinatorNotAvailableException" $FLINK_DIR/log \
      | grep -v "RetriableCommitFailedException" \
      | grep -v "NoAvailableBrokersException" \
      | grep -v "Async Kafka commit failed" \
      | grep -v "DisconnectException" \
      | grep -v "Cannot connect to ResourceManager right now" \
      | grep -v "AskTimeoutException" \
      | grep -v "Error while loading kafka-version.properties" \
      | grep -v "WARN  akka.remote.transport.netty.NettyTransport" \
      | grep -v "WARN  org.apache.flink.shaded.akka.org.jboss.netty.channel.DefaultChannelPipeline" \
      | grep -v "jvm-exit-on-fatal-error" \
      | grep -v 'INFO.*AWSErrorCode' \
      | grep -v "RejectedExecutionException" \
      | grep -v "An exception was thrown by an exception handler" \
      | grep -v "java.lang.NoClassDefFoundError: org/apache/hadoop/yarn/exceptions/YarnException" \
      | grep -v "java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration" \
      | grep -v "org.apache.commons.beanutils.FluentPropertyBeanIntrospector.*Error when creating PropertyDescriptor.*org.apache.commons.configuration2.AbstractConfiguration.setProperty(java.lang.String,java.lang.Object)! Ignoring this property." \
      | grep -v "Error while loading kafka-version.properties :null" \
      | grep -v "Failed Elasticsearch item request" \
      | grep -v "[Terror] modules" \
      | grep -v "HeapDumpOnOutOfMemoryError" \
      | grep -v "error_prone_annotations" \
      | grep -ic "error" || true)
  if [[ ${error_count} -gt 0 ]]; then
    echo "Found error in log files:"
    cat $FLINK_DIR/log/*
    EXIT_CODE=1
  else
    echo "No errors in log files."
  fi
}

function check_logs_for_exceptions {
  echo "Checking for exceptions..."
  exception_count=$(grep -rv "GroupCoordinatorNotAvailableException" $FLINK_DIR/log \
   | grep -v "RetriableCommitFailedException" \
   | grep -v "NoAvailableBrokersException" \
   | grep -v "Async Kafka commit failed" \
   | grep -v "DisconnectException" \
   | grep -v "Cannot connect to ResourceManager right now" \
   | grep -v "AskTimeoutException" \
   | grep -v "WARN  akka.remote.transport.netty.NettyTransport" \
   | grep -v  "WARN  org.apache.flink.shaded.akka.org.jboss.netty.channel.DefaultChannelPipeline" \
   | grep -v 'INFO.*AWSErrorCode' \
   | grep -v "RejectedExecutionException" \
   | grep -v "An exception was thrown by an exception handler" \
   | grep -v "Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.yarn.exceptions.YarnException" \
   | grep -v "Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.conf.Configuration" \
   | grep -v "java.lang.NoClassDefFoundError: org/apache/hadoop/yarn/exceptions/YarnException" \
   | grep -v "java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration" \
   | grep -v "java.lang.Exception: Execution was suspended" \
   | grep -v "java.io.InvalidClassException: org.apache.flink.formats.avro.typeutils.AvroSerializer" \
   | grep -v "Caused by: java.lang.Exception: JobManager is shutting down" \
   | grep -v "java.lang.Exception: Artificial failure" \
   | grep -v "org.apache.flink.runtime.checkpoint.CheckpointException" \
   | grep -v "org.elasticsearch.ElasticsearchException" \
   | grep -v "Elasticsearch exception" \
   | grep -v "org.apache.flink.runtime.JobException: Recovery is suppressed" \
   | grep -ic "exception" || true)
  if [[ ${exception_count} -gt 0 ]]; then
    echo "Found exception in log files:"
    cat $FLINK_DIR/log/*
    EXIT_CODE=1
  else
    echo "No exceptions in log files."
  fi
}

function check_logs_for_non_empty_out_files {
  echo "Checking for non-empty .out files..."
  # exclude reflective access warnings as these are expected (and currently unavoidable) on Java 9
  # exclude message about JAVA_TOOL_OPTIONS being set (https://bugs.openjdk.java.net/browse/JDK-8039152)
  if grep -ri -v \
    -e "WARNING: An illegal reflective access" \
    -e "WARNING: Illegal reflective access"\
    -e "WARNING: Please consider reporting"\
    -e "WARNING: Use --illegal-access"\
    -e "WARNING: All illegal access"\
    -e "Picked up JAVA_TOOL_OPTIONS"\
    $FLINK_DIR/log/*.out\
   | grep "." \
   > /dev/null; then
    echo "Found non-empty .out files:"
    cat $FLINK_DIR/log/*.out
    EXIT_CODE=1
  else
    echo "No non-empty .out files."
  fi
}

function shutdown_all {
  stop_cluster
  tm_kill_all
  jm_kill_all
}

function stop_cluster {
  "$FLINK_DIR"/bin/stop-cluster.sh

  # stop zookeeper only if there are processes running
  zookeeper_process_count=$(jps | grep -c 'FlinkZooKeeperQuorumPeer' || true)
  if [[ ${zookeeper_process_count} -gt 0 ]]; then
    echo "Stopping zookeeper..."
    "$FLINK_DIR"/bin/zookeeper.sh stop
  fi
}

function wait_for_job_state_transition {
  local job=$1
  local initial_state=$2
  local next_state=$3
    
  echo "Waiting for job ($job) to switch from state ${initial_state} to state ${next_state} ..."

  while : ; do
    N=$(grep -o "($job) switched from state ${initial_state} to ${next_state}" $FLINK_DIR/log/*standalonesession*.log | tail -1)

    if [[ -z $N ]]; then
      sleep 1
    else
      break
    fi
  done
}

function wait_job_running {
  local TIMEOUT=10
  for i in $(seq 1 ${TIMEOUT}); do
    JOB_LIST_RESULT=$("$FLINK_DIR"/bin/flink list -r | grep "$1")

    if [[ "$JOB_LIST_RESULT" == "" ]]; then
      echo "Job ($1) is not yet running."
    else
      echo "Job ($1) is running."
      return
    fi
    sleep 1
  done
  echo "Job ($1) has not started within a timeout of ${TIMEOUT} sec"
  exit 1
}

function wait_job_terminal_state {
  local job=$1
  local expected_terminal_state=$2
  local log_file_name=${3:-standalonesession}

  echo "Waiting for job ($job) to reach terminal state $expected_terminal_state ..."

  while : ; do
    local N=$(grep -o "Job $job reached globally terminal state .*" $FLINK_DIR/log/*$log_file_name*.log | tail -1 || true)
    if [[ -z $N ]]; then
      sleep 1
    else
      local actual_terminal_state=$(echo $N | sed -n 's/.*state \([A-Z]*\).*/\1/p')
      if [[ -z $expected_terminal_state ]] || [[ "$expected_terminal_state" == "$actual_terminal_state" ]]; then
        echo "Job ($job) reached terminal state $actual_terminal_state"
        break
      else
        echo "Job ($job) is in state $actual_terminal_state but expected $expected_terminal_state"
        exit 1
      fi
    fi
  done
}

function stop_with_savepoint {
  "$FLINK_DIR"/bin/flink stop -p $2 $1
}

function take_savepoint {
  "$FLINK_DIR"/bin/flink savepoint $1 $2
}

function cancel_job {
  "$FLINK_DIR"/bin/flink cancel $1
}

function check_result_hash {
  local error_code=0
  check_result_hash_no_exit "$@" || error_code=$?

  if [ "$error_code" != "0" ]
  then
    exit $error_code
  fi
}

function check_result_hash_no_exit {
  local name=$1
  local outfile_prefix=$2
  local expected=$3

  local actual
  if [ "`command -v md5`" != "" ]; then
    actual=$(LC_ALL=C sort $outfile_prefix* | md5 -q)
  elif [ "`command -v md5sum`" != "" ]; then
    actual=$(LC_ALL=C sort $outfile_prefix* | md5sum | awk '{print $1}')
  else
    echo "Neither 'md5' nor 'md5sum' binary available."
    return 2
  fi
  if [[ "$actual" != "$expected" ]]
  then
    echo "FAIL $name: Output hash mismatch.  Got $actual, expected $expected."
    echo "head hexdump of actual:"
    head $outfile_prefix* | hexdump -c
    return 1
  else
    echo "pass $name"
    # Output files are left behind in /tmp
  fi
  return 0
}

# This function starts the given number of task managers and monitors their processes.
# If a task manager process goes away a replacement is started.
function tm_watchdog {
  local expectedTm=$1
  while true;
  do
    runningTm=`jps | grep -Eo 'TaskManagerRunner|TaskManager' | wc -l`;
    count=$((expectedTm-runningTm))
    if (( count != 0 )); then
        start_taskmanagers ${count} > /dev/null
    fi
    sleep 5;
  done
}

# Kills all job manager.
function jm_kill_all {
  kill_all 'ClusterEntrypoint'
}

# Kills all task manager.
function tm_kill_all {
  kill_all 'TaskManagerRunner|TaskManager'
}

# Kills all processes that match the given name.
function kill_all {
  local pid=`jps | grep -E "${1}" | cut -d " " -f 1 || true`
  kill ${pid} 2> /dev/null || true
  wait ${pid} 2> /dev/null || true
}

function kill_random_taskmanager {
  local pid=`jps | grep -E "TaskManagerRunner|TaskManager" | sort -R | head -n 1 | cut -d " " -f 1 || true`
  kill -9 "$pid"
  echo "TaskManager $pid killed."
}

function setup_flink_slf4j_metric_reporter() {
  INTERVAL="${1:-1 SECONDS}"
  add_optional_plugin "metrics-slf4j"
  set_config_key "metrics.reporter.slf4j.factory.class" "org.apache.flink.metrics.slf4j.Slf4jReporterFactory"
  set_config_key "metrics.reporter.slf4j.interval" "${INTERVAL}"
}

function get_job_metric {
  local job_id=$1
  local metric_name=$2

  local json=$(curl ${CURL_SSL_ARGS} -s ${REST_PROTOCOL}://${NODENAME}:8081/jobs/${job_id}/metrics?get=${metric_name})
  local metric_value=$(echo ${json} | sed -n 's/.*"value":"\(.*\)".*/\1/p')

  echo ${metric_value}
}

function get_metric_processed_records {
  OPERATOR=$1
  JOB_NAME="${2:-General purpose test job}"
  N=$(grep ".${JOB_NAME}.$OPERATOR.numRecordsIn:" $FLINK_DIR/log/*taskexecutor*.log | sed 's/.* //g' | tail -1)
  if [ -z $N ]; then
    N=0
  fi
  echo $N
}

function get_num_metric_samples {
  OPERATOR=$1
  JOB_NAME="${2:-General purpose test job}"
  N=$(grep ".${JOB_NAME}.$OPERATOR.numRecordsIn:" $FLINK_DIR/log/*taskexecutor*.log | wc -l)
  if [ -z $N ]; then
    N=0
  fi
  echo $N
}

function wait_oper_metric_num_in_records {
    OPERATOR=$1
    MAX_NUM_METRICS="${2:-200}"
    JOB_NAME="${3:-General purpose test job}"
    NUM_METRICS=$(get_num_metric_samples ${OPERATOR} '${JOB_NAME}')
    OLD_NUM_METRICS=${4:-${NUM_METRICS}}
    # monitor the numRecordsIn metric of the state machine operator in the second execution
    # we let the test finish once the second restore execution has processed 200 records
    while : ; do
      NUM_METRICS=$(get_num_metric_samples ${OPERATOR} "${JOB_NAME}")
      NUM_RECORDS=$(get_metric_processed_records ${OPERATOR} "${JOB_NAME}")

      # only account for metrics that appeared in the second execution
      if (( $OLD_NUM_METRICS >= $NUM_METRICS )) ; then
        NUM_RECORDS=0
      fi

      if (( $NUM_RECORDS < $MAX_NUM_METRICS )); then
        echo "Waiting for job to process up to ${MAX_NUM_METRICS} records, current progress: ${NUM_RECORDS} records ..."
        sleep 1
      else
        break
      fi
    done
}

function wait_num_of_occurence_in_logs {
    local text=$1
    local number=$2
    local logs=${3:-standalonesession}

    echo "Waiting for text ${text} to appear ${number} of times in logs..."

    while : ; do
      N=$(grep -o "${text}" $FLINK_DIR/log/*${logs}*.log | wc -l)

      if [ -z $N ]; then
        N=0
      fi

      if (( N < number )); then
        sleep 1
      else
        break
      fi
    done
}

function wait_num_checkpoints {
    JOB=$1
    NUM_CHECKPOINTS=$2

    echo "Waiting for job ($JOB) to have at least $NUM_CHECKPOINTS completed checkpoints ..."

    while : ; do
      N=$(grep -o "Completed checkpoint [1-9]* for job $JOB" $FLINK_DIR/log/*standalonesession*.log | awk '{print $3}' | tail -1)

      if [ -z $N ]; then
        N=0
      fi

      if (( N < NUM_CHECKPOINTS )); then
        sleep 1
      else
        break
      fi
    done
}

# Starts the timer. Note that nested timers are not supported.
function start_timer {
    SECONDS=0
}

# prints the number of minutes and seconds that have elapsed since the last call to start_timer
function end_timer {
    duration=$SECONDS
    echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds"
}

function clean_stdout_files {
    rm ${FLINK_DIR}/log/*.out
    echo "Deleted all stdout files under ${FLINK_DIR}/log/"
}

# Expect a string to appear in the log files of the task manager before a given timeout
# $1: expected string
# $2: timeout in seconds
function expect_in_taskmanager_logs {
    local expected="$1"
    local timeout=$2
    local i=0
    local logfile="${FLINK_DIR}/log/flink*taskexecutor*log"


    while ! grep "${expected}" ${logfile} > /dev/null; do
        sleep 1s
        ((i++))
        if ((i > timeout)); then
            echo "A timeout occurred waiting for '${expected}' to appear in the taskmanager logs"
            exit 1
        fi
    done
}

function wait_for_restart_to_complete {
    local base_num_restarts=$1
    local jobid=$2

    local current_num_restarts=${base_num_restarts}
    local expected_num_restarts=$((current_num_restarts + 1))

    echo "Waiting for restart to happen"
    while [[ ${current_num_restarts} -lt ${expected_num_restarts} ]]; do
        echo "Still waiting for restarts. Expected: $expected_num_restarts Current: $current_num_restarts"
        sleep 5
        current_num_restarts=$(get_job_metric ${jobid} "fullRestarts")
        if [[ -z ${current_num_restarts} ]]; then
            current_num_restarts=${base_num_restarts}
        fi
    done
}

function find_latest_completed_checkpoint {
    local checkpoint_root_directory=$1
    # a completed checkpoint must contain the _metadata file
    local checkpoint_meta_file=$(ls -d ${checkpoint_root_directory}/chk-[1-9]*/_metadata | sort -Vr | head -n1)
    echo "$(dirname "${checkpoint_meta_file}")"
}

function retry_times() {
    retry_times_with_backoff_and_cleanup $1 $2 "$3" "true"
}

function retry_times_with_backoff_and_cleanup() {
    local retriesNumber=$1
    local backoff=$2
    local command="$3"
    local cleanup_command="$4"

    for i in $(seq 1 ${retriesNumber})
    do
        if ${command}; then
            return 0
        else
            ${cleanup_command}
        fi

        echo "Command: ${command} failed. Retrying..."
        sleep ${backoff}
    done

    echo "Command: ${command} failed ${retriesNumber} times."
    ${cleanup_command}
    return 1
}

JOB_ID_REGEX_EXTRACTOR=".*JobID ([0-9,a-f]*)"

function extract_job_id_from_job_submission_return() {
    if [[ $1 =~ $JOB_ID_REGEX_EXTRACTOR ]];
        then
            JOB_ID="${BASH_REMATCH[1]}";
        else
            JOB_ID=""
        fi
    echo "$JOB_ID"
}

