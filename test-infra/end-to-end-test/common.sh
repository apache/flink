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
if [[ -z $CLUSTER_MODE ]]; then
    echo "CLUSTER_MODE needs to be one of local or cluster."
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

function start_cluster {
  if [[ "$CLUSTER_MODE" == "local" ]]; then
    $FLINK_DIR/bin/start-local.sh
  elif [[ "$CLUSTER_MODE" == "cluster" ]]; then
    $FLINK_DIR/bin/start-cluster.sh
  else
    echo "Unrecognized cluster mode: $CLUSTER_MODE"
    exit
  fi

  # wait for TaskManager to come up
  # wait roughly 10 seconds
  for i in {1..10}; do
    # without the || true this would exit our script if the JobManager is not yet up
    QUERY_RESULT=$(curl "http://localhost:8081/taskmanagers" || true)

    if [[ "$QUERY_RESULT" == "" ]]; then
     echo "JobManager is not yet up"
    elif [[ "$QUERY_RESULT" != "{\"taskmanagers\":[]}" ]]; then
      break
    fi

    echo "Waiting for cluster to come up..."
    sleep 1
  done
}

function stop_cluster {
  if [[ "$CLUSTER_MODE" == "local" ]]; then
    $FLINK_DIR/bin/stop-local.sh
  elif [[ "$CLUSTER_MODE" == "cluster" ]]; then
    $FLINK_DIR/bin/stop-cluster.sh
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

  rm $FLINK_DIR/log/*
}

function check_result_hash {
  local name=$1
  local outfile_prefix=$2
  local expected=$3

  local actual=$(LC_ALL=C sort $outfile_prefix* | md5sum | awk '{print $1}' \
    || LC_ALL=C sort $outfile_prefix* | md5 -q) || exit 2  # OSX
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

# make sure to clean up even in case of failures
function cleanup {
  stop_cluster
  rm -r $TEST_DATA_DIR
  check_all_pass
}
trap cleanup EXIT
