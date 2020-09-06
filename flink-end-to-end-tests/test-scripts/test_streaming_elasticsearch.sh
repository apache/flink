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

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/elasticsearch-common.sh

ELASTICSEARCH_VERSION=$1
DOWNLOAD_URL=$2

mkdir -p $TEST_DATA_DIR

setup_elasticsearch $DOWNLOAD_URL $ELASTICSEARCH_VERSION
wait_elasticsearch_working

start_cluster

function test_cleanup {
  shutdown_elasticsearch_cluster index
}

on_exit test_cleanup

TEST_ES_JAR=${END_TO_END_DIR}/flink-elasticsearch${ELASTICSEARCH_VERSION}-test/target/Elasticsearch${ELASTICSEARCH_VERSION}SinkExample.jar

# run the Flink job
$FLINK_DIR/bin/flink run -p 1 $TEST_ES_JAR \
  --numRecords 20 \
  --index index \
  --type type

# 40 index requests and 20 final update requests
verify_result_line_number 60 index
