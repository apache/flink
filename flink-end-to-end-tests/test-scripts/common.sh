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

echo "Flink dist directory: $FLINK_DIR"

FLINK_VERSION=$(cat ${END_TO_END_DIR}/pom.xml | sed -n 's/.*<version>\(.*\)<\/version>/\1/p')

USE_SSL=OFF # set via set_conf_ssl(), reset via revert_default_config()
TEST_ROOT=`pwd -P`
TEST_INFRA_DIR="$END_TO_END_DIR/test-scripts/"
cd $TEST_INFRA_DIR
TEST_INFRA_DIR=`pwd -P`
cd $TEST_ROOT

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

function backup_config() {
    # back up the masters and flink-conf.yaml
    cp $FLINK_DIR/conf/masters $FLINK_DIR/conf/masters.bak
    cp $FLINK_DIR/conf/flink-conf.yaml $FLINK_DIR/conf/flink-conf.yaml.bak
}

function revert_default_config() {

    # revert our modifications to the masters file
    if [ -f $FLINK_DIR/conf/masters.bak ]; then
        mv -f $FLINK_DIR/conf/masters.bak $FLINK_DIR/conf/masters
    fi

    # revert our modifications to the Flink conf yaml
    if [ -f $FLINK_DIR/conf/flink-conf.yaml.bak ]; then
        mv -f $FLINK_DIR/conf/flink-conf.yaml.bak $FLINK_DIR/conf/flink-conf.yaml
    fi

    USE_SSL=OFF
}

function set_conf() {
    CONF_NAME=$1
    VAL=$2
    echo "$CONF_NAME: $VAL" >> $FLINK_DIR/conf/flink-conf.yaml
}

function change_conf() {
    CONF_NAME=$1
    OLD_VAL=$2
    NEW_VAL=$3
    sed -i -e "s/${CONF_NAME}: ${OLD_VAL}/${CONF_NAME}: ${NEW_VAL}/" ${FLINK_DIR}/conf/flink-conf.yaml
}

function create_ha_config() {

    backup_config

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
    jobmanager.heap.mb: 1024
    taskmanager.heap.mb: 1024
    taskmanager.numberOfTaskSlots: 4

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

    query.server.ports: 9000-9009
    query.proxy.ports: 9010-9019
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

function set_conf_ssl {

    # clean up the dir that will be used for SSL certificates and trust stores
    if [ -e "${TEST_DATA_DIR}/ssl" ]; then
       echo "File ${TEST_DATA_DIR}/ssl exists. Deleting it..."
       rm -rf "${TEST_DATA_DIR}/ssl"
    fi
    mkdir -p "${TEST_DATA_DIR}/ssl"

    NODENAME=`hostname -f`
    SANSTRING="dns:${NODENAME}"
    for NODEIP in $(get_node_ip) ; do
        SANSTRING="${SANSTRING},ip:${NODEIP}"
    done

    echo "Using SAN ${SANSTRING}"

    # create certificates
    keytool -genkeypair -alias ca -keystore "${TEST_DATA_DIR}/ssl/ca.keystore" -dname "CN=Sample CA" -storepass password -keypass password -keyalg RSA -ext bc=ca:true
    keytool -keystore "${TEST_DATA_DIR}/ssl/ca.keystore" -storepass password -alias ca -exportcert > "${TEST_DATA_DIR}/ssl/ca.cer"
    keytool -importcert -keystore "${TEST_DATA_DIR}/ssl/ca.truststore" -alias ca -storepass password -noprompt -file "${TEST_DATA_DIR}/ssl/ca.cer"

    keytool -genkeypair -alias node -keystore "${TEST_DATA_DIR}/ssl/node.keystore" -dname "CN=${NODENAME}" -ext SAN=${SANSTRING} -storepass password -keypass password -keyalg RSA
    keytool -certreq -keystore "${TEST_DATA_DIR}/ssl/node.keystore" -storepass password -alias node -file "${TEST_DATA_DIR}/ssl/node.csr"
    keytool -gencert -keystore "${TEST_DATA_DIR}/ssl/ca.keystore" -storepass password -alias ca -ext SAN=${SANSTRING} -infile "${TEST_DATA_DIR}/ssl/node.csr" -outfile "${TEST_DATA_DIR}/ssl/node.cer"
    keytool -importcert -keystore "${TEST_DATA_DIR}/ssl/node.keystore" -storepass password -file "${TEST_DATA_DIR}/ssl/ca.cer" -alias ca -noprompt
    keytool -importcert -keystore "${TEST_DATA_DIR}/ssl/node.keystore" -storepass password -file "${TEST_DATA_DIR}/ssl/node.cer" -alias node -noprompt

    # adapt config
    # (here we rely on security.ssl.enabled enabling SSL for all components and internal as well as
    # external communication channels)
    set_conf security.ssl.enabled true
    set_conf security.ssl.keystore ${TEST_DATA_DIR}/ssl/node.keystore
    set_conf security.ssl.keystore-password password
    set_conf security.ssl.key-password password
    set_conf security.ssl.truststore ${TEST_DATA_DIR}/ssl/ca.truststore
    set_conf security.ssl.truststore-password password
    USE_SSL=ON
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

function start_cluster {
  "$FLINK_DIR"/bin/start-cluster.sh

  # wait at most 10 seconds until the dispatcher is up
  local QUERY_URL
  if [ "x$USE_SSL" = "xON" ]; then
    QUERY_URL="http://localhost:8081/taskmanagers"
  else
    QUERY_URL="https://localhost:8081/taskmanagers"
  fi
  for i in {1..10}; do
    # without the || true this would exit our script if the JobManager is not yet up
    QUERY_RESULT=$(curl "$QUERY_URL" 2> /dev/null || true)

    # ensure the taskmanagers field is there at all and is not empty
    if [[ ${QUERY_RESULT} =~ \{\"taskmanagers\":\[.+\]\} ]]; then

      echo "Dispatcher REST endpoint is up."
      break
    fi

    echo "Waiting for dispatcher REST endpoint to come up..."
    sleep 1
  done
}

function start_taskmanagers {
    tmnum=$1
    echo "Start ${tmnum} more task managers"
    for (( c=0; c<tmnum; c++ ))
    do
        $FLINK_DIR/bin/taskmanager.sh start
    done
}

function start_and_wait_for_tm {

  tm_query_result=$(curl -s "http://localhost:8081/taskmanagers")

  # we assume that the cluster is running
  if ! [[ ${tm_query_result} =~ \{\"taskmanagers\":\[.*\]\} ]]; then
    echo "Your cluster seems to be unresponsive at the moment: ${tm_query_result}" 1>&2
    exit 1
  fi

  running_tms=`curl -s "http://localhost:8081/taskmanagers" | grep -o "id" | wc -l`

  ${FLINK_DIR}/bin/taskmanager.sh start

  for i in {1..10}; do
    local new_running_tms=`curl -s "http://localhost:8081/taskmanagers" | grep -o "id" | wc -l`
    if [ $((new_running_tms-running_tms)) -eq 0 ]; then
      echo "TaskManager is not yet up."
    else
      echo "TaskManager is up."
      break
    fi
    sleep 4
  done
}

function check_logs_for_errors {
  if grep -rv "GroupCoordinatorNotAvailableException" $FLINK_DIR/log \
      | grep -v "RetriableCommitFailedException" \
      | grep -v "NoAvailableBrokersException" \
      | grep -v "Async Kafka commit failed" \
      | grep -v "DisconnectException" \
      | grep -v "AskTimeoutException" \
      | grep -v "WARN  akka.remote.transport.netty.NettyTransport" \
      | grep -v  "WARN  org.apache.flink.shaded.akka.org.jboss.netty.channel.DefaultChannelPipeline" \
      | grep -v "jvm-exit-on-fatal-error" \
      | grep -v '^INFO:.*AWSErrorCode=\[400 Bad Request\].*ServiceEndpoint=\[https://.*\.s3\.amazonaws\.com\].*RequestType=\[HeadBucketRequest\]' \
      | grep -v "RejectedExecutionException" \
      | grep -v "An exception was thrown by an exception handler" \
      | grep -v "java.lang.NoClassDefFoundError: org/apache/hadoop/yarn/exceptions/YarnException" \
      | grep -v "java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration" \
      | grep -iq "error"; then
    echo "Found error in log files:"
    cat $FLINK_DIR/log/*
    EXIT_CODE=1
  fi
}

function check_logs_for_exceptions {
  if grep -rv "GroupCoordinatorNotAvailableException" $FLINK_DIR/log \
      | grep -v "RetriableCommitFailedException" \
      | grep -v "NoAvailableBrokersException" \
      | grep -v "Async Kafka commit failed" \
      | grep -v "DisconnectException" \
      | grep -v "AskTimeoutException" \
      | grep -v "WARN  akka.remote.transport.netty.NettyTransport" \
      | grep -v  "WARN  org.apache.flink.shaded.akka.org.jboss.netty.channel.DefaultChannelPipeline" \
      | grep -v '^INFO:.*AWSErrorCode=\[400 Bad Request\].*ServiceEndpoint=\[https://.*\.s3\.amazonaws\.com\].*RequestType=\[HeadBucketRequest\]' \
      | grep -v "RejectedExecutionException" \
      | grep -v "An exception was thrown by an exception handler" \
      | grep -v "Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.yarn.exceptions.YarnException" \
      | grep -v "Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.conf.Configuration" \
      | grep -v "java.lang.NoClassDefFoundError: org/apache/hadoop/yarn/exceptions/YarnException" \
      | grep -v "java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration" \
      | grep -v "java.lang.Exception: Execution was suspended" \
      | grep -v "Caused by: java.lang.Exception: JobManager is shutting down" \
      | grep -iq "exception"; then
    echo "Found exception in log files:"
    cat $FLINK_DIR/log/*
    EXIT_CODE=1
  fi
}

function check_logs_for_non_empty_out_files {
  if grep -ri "." $FLINK_DIR/log/*.out > /dev/null; then
    echo "Found non-empty .out files:"
    cat $FLINK_DIR/log/*.out
    EXIT_CODE=1
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
  if ! [ "`jps | grep 'FlinkZooKeeperQuorumPeer' | wc -l`" = "0" ]; then
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
  for i in {1..10}; do
    JOB_LIST_RESULT=$("$FLINK_DIR"/bin/flink list -r | grep "$1")

    if [[ "$JOB_LIST_RESULT" == "" ]]; then
      echo "Job ($1) is not yet running."
    else
      echo "Job ($1) is running."
      break
    fi
    sleep 1
  done
}

function wait_job_terminal_state {
  local job=$1
  local terminal_state=$2

  echo "Waiting for job ($job) to reach terminal state $terminal_state ..."

  while : ; do
    N=$(grep -o "Job $job reached globally terminal state $terminal_state" $FLINK_DIR/log/*standalonesession*.log | tail -1)

    if [[ -z $N ]]; then
      sleep 1
    else
      break
    fi
  done
}

function take_savepoint {
  "$FLINK_DIR"/bin/flink savepoint $1 $2
}

function cancel_job {
  "$FLINK_DIR"/bin/flink cancel $1
}

function check_result_hash {
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
    exit 2
  fi
  if [[ "$actual" != "$expected" ]]
  then
    echo "FAIL $name: Output hash mismatch.  Got $actual, expected $expected."
    echo "head hexdump of actual:"
    head $outfile_prefix* | hexdump -c
    exit 1
  else
    echo "pass $name"
    # Output files are left behind in /tmp
  fi
}

function s3_put {
  local_file=$1
  bucket=$2
  s3_file=$3
  resource="/${bucket}/${s3_file}"
  contentType="application/octet-stream"
  dateValue=`date -R`
  stringToSign="PUT\n\n${contentType}\n${dateValue}\n${resource}"
  s3Key=$ARTIFACTS_AWS_ACCESS_KEY
  s3Secret=$ARTIFACTS_AWS_SECRET_KEY
  signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`
  curl -X PUT -T "${local_file}" \
    -H "Host: ${bucket}.s3.amazonaws.com" \
    -H "Date: ${dateValue}" \
    -H "Content-Type: ${contentType}" \
    -H "Authorization: AWS ${s3Key}:${signature}" \
    https://${bucket}.s3.amazonaws.com/${s3_file}
}

function s3_delete {
  bucket=$1
  s3_file=$2
  resource="/${bucket}/${s3_file}"
  contentType="application/octet-stream"
  dateValue=`date -R`
  stringToSign="DELETE\n\n${contentType}\n${dateValue}\n${resource}"
  s3Key=$ARTIFACTS_AWS_ACCESS_KEY
  s3Secret=$ARTIFACTS_AWS_SECRET_KEY
  signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`
  curl -X DELETE \
    -H "Host: ${bucket}.s3.amazonaws.com" \
    -H "Date: ${dateValue}" \
    -H "Content-Type: ${contentType}" \
    -H "Authorization: AWS ${s3Key}:${signature}" \
    https://${bucket}.s3.amazonaws.com/${s3_file}
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
  kill_all 'StandaloneSessionClusterEntrypoint'
}

# Kills all task manager.
function tm_kill_all {
  kill_all 'TaskManagerRunner|TaskManager'
}

# Kills all processes that match the given name.
function kill_all {
  local pid=`jps | grep -E "${1}" | cut -d " " -f 1`
  kill ${pid} 2> /dev/null
  wait ${pid} 2> /dev/null
}

function kill_random_taskmanager {
  KILL_TM=$(jps | grep "TaskManager" | sort -R | head -n 1 | awk '{print $1}')
  kill -9 "$KILL_TM"
  echo "TaskManager $KILL_TM killed."
}

function setup_flink_slf4j_metric_reporter() {
  INTERVAL="${1:-1 SECONDS}"
  cp $FLINK_DIR/opt/flink-metrics-slf4j-*.jar $FLINK_DIR/lib/
  set_conf "metrics.reporter.slf4j.class" "org.apache.flink.metrics.slf4j.Slf4jReporter"
  set_conf "metrics.reporter.slf4j.interval" "${INTERVAL}"
}

function rollback_flink_slf4j_metric_reporter() {
  rm $FLINK_DIR/lib/flink-metrics-slf4j-*.jar
}

function get_job_metric {
  local job_id=$1
  local metric_name=$2

  local json=$(curl -s http://localhost:8081/jobs/${job_id}/metrics?get=${metric_name})
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

function clean_log_files {
    rm ${FLINK_DIR}/log/*
    echo "Deleted all files under ${FLINK_DIR}/log/"
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
