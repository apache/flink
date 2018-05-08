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

export PASS=1

echo "Flink dist directory: $FLINK_DIR"

TEST_ROOT=`pwd`
TEST_INFRA_DIR="$0"
TEST_INFRA_DIR=`dirname "$TEST_INFRA_DIR"`
cd $TEST_INFRA_DIR
TEST_INFRA_DIR=`pwd`
cd $TEST_ROOT

# used to randomize created directories
export TEST_DATA_DIR=$TEST_INFRA_DIR/temp-test-directory-$(date +%S%N)
echo "TEST_DATA_DIR: $TEST_DATA_DIR"

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
    parallelism.default: 1

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

    web.port: 8081
EOL
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
                echo "[ERROR] Parse error. Only available for localhost."
                PASS=""
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
  for i in {1..10}; do
    # without the || true this would exit our script if the JobManager is not yet up
    QUERY_RESULT=$(curl "http://localhost:8081/taskmanagers" 2> /dev/null || true)

    if [[ "$QUERY_RESULT" == "" ]]; then
      echo "Dispatcher/TaskManagers are not yet up"
    elif [[ "$QUERY_RESULT" != "{\"taskmanagers\":[]}" ]]; then
      echo "Dispatcher REST endpoint is up."
      break
    fi

    echo "Waiting for dispatcher REST endpoint to come up..."
    sleep 1
  done
}

function stop_cluster {
  "$FLINK_DIR"/bin/stop-cluster.sh

  # stop zookeeper only if there are processes running
  if ! [ "`jps | grep 'FlinkZooKeeperQuorumPeer' | wc -l`" = "0" ]; then
    "$FLINK_DIR"/bin/zookeeper.sh stop
  fi

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
    PASS=""
  fi
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
    PASS=""
  fi

  if grep -ri "." $FLINK_DIR/log/*.out > /dev/null; then
    echo "Found non-empty .out files:"
    cat $FLINK_DIR/log/*.out
    PASS=""
  fi
}

function wait_job_running {
  for i in {1..10}; do
    JOB_LIST_RESULT=$("$FLINK_DIR"/bin/flink list | grep "$1")

    if [[ "$JOB_LIST_RESULT" == "" ]]; then
      echo "Job ($1) is not yet running."
    else
      echo "Job ($1) is running."
      break
    fi
    sleep 1
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
    PASS=""
    echo "head hexdump of actual:"
    head $outfile_prefix* | hexdump -c
  else
    echo "pass $name"
    # Output files are left behind in /tmp
  fi
}

function check_all_pass {
  if [[ ! "$PASS" ]]
  then
    echo "One or more tests FAILED."
    exit 1
  fi
  echo "All tests PASS"
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

function get_metric_processed_records {
  OPERATOR=$1
  N=$(grep ".General purpose test job.$OPERATOR.numRecordsIn:" $FLINK_DIR/log/*taskexecutor*.log | sed 's/.* //g' | tail -1)
  if [ -z $N ]; then
    N=0
  fi
  echo $N
}

function get_num_metric_samples {
  OPERATOR=$1
  N=$(grep ".General purpose test job.$OPERATOR.numRecordsIn:" $FLINK_DIR/log/*taskexecutor*.log | wc -l)
  if [ -z $N ]; then
    N=0
  fi
  echo $N
}

function wait_oper_metric_num_in_records {
    OPERATOR=$1
    MAX_NUM_METRICS="${2:-200}"
    NUM_METRICS=$(get_num_metric_samples ${OPERATOR})
    OLD_NUM_METRICS=${3:-${NUM_METRICS}}
    # monitor the numRecordsIn metric of the state machine operator in the second execution
    # we let the test finish once the second restore execution has processed 200 records
    while : ; do
      NUM_METRICS=$(get_num_metric_samples ${OPERATOR})
      NUM_RECORDS=$(get_metric_processed_records ${OPERATOR})

      # only account for metrics that appeared in the second execution
      if (( $OLD_NUM_METRICS >= $NUM_METRICS )) ; then
        NUM_RECORDS=0
      fi

      if (( $NUM_RECORDS < $MAX_NUM_METRICS )); then
        echo "Waiting for job to process up to 200 records, current progress: $NUM_RECORDS records ..."
        sleep 1
      else
        break
      fi
    done
}

# make sure to clean up even in case of failures
function cleanup {
  stop_cluster
  check_all_pass
  rm -rf $TEST_DATA_DIR
  rm -f $FLINK_DIR/log/*
  revert_default_config
}
trap cleanup EXIT
