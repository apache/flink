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

set -e
set -o pipefail

export FLINK_DIR="$1"
export CLUSTER_MODE="$2"

export PASS=1

echo "Flink dist directory: $FLINK_DIR"

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
      | grep -iq "exception"; then
    echo "Found exception in log files:"
    cat $FLINK_DIR/log/*
    PASS=""
  fi

  for f in `ls $FLINK_DIR/log/*.out`
  do
    if [[ -s $f ]]; then
      echo "Found non-empty file $f"
      cat $f
      PASS=""
    fi
  done

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

function clean_data_dir {
  rm -r $TEST_DATA_DIR
}
