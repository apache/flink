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

mkdir -p $TEST_DATA_DIR

ELASTICSEARCH1_URL="https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.1.tar.gz"
ELASTICSEARCH2_URL="https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.3.5/elasticsearch-2.3.5.tar.gz"
ELASTICSEARCH5_URL="https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.1.2.tar.gz"

# start downloading elasticsearch1
echo "Downloading Elasticsearch1 from $ELASTICSEARCH1_URL"
curl "$ELASTICSEARCH1_URL" > $TEST_DATA_DIR/elasticsearch1.tar.gz

tar xzf $TEST_DATA_DIR/elasticsearch1.tar.gz -C $TEST_DATA_DIR/
ELASTICSEARCH1_DIR=$TEST_DATA_DIR/elasticsearch-1.7.1

# start elasticsearch1 cluster
$ELASTICSEARCH1_DIR/bin/elasticsearch -daemon

verify_elasticsearch_process_exist

start_cluster

TEST_ES1_JAR=$TEST_DATA_DIR/../../flink-elasticsearch1-test/target/Elasticsearch1SinkExample.jar

# run the Flink job
$FLINK_DIR/bin/flink run -p 1 $TEST_ES1_JAR \
  --index index \
  --type type

verify_result

shutdown_elasticsearch_cluster

mkdir -p $TEST_DATA_DIR

# start downloading elasticsearch2
echo "Downloading Elasticsearch2 from $ELASTICSEARCH2_URL"
curl "$ELASTICSEARCH2_URL" > $TEST_DATA_DIR/elasticsearch2.tar.gz

tar xzf $TEST_DATA_DIR/elasticsearch2.tar.gz -C $TEST_DATA_DIR/
ELASTICSEARCH2_DIR=$TEST_DATA_DIR/elasticsearch-2.3.5

# start elasticsearch cluster, different from elasticsearch1 since using -daemon here will hang the shell.
nohup $ELASTICSEARCH2_DIR/bin/elasticsearch &

verify_elasticsearch_process_exist

start_cluster

TEST_ES2_JAR=$TEST_DATA_DIR/../../flink-elasticsearch2-test/target/Elasticsearch2SinkExample.jar

# run the Flink job
$FLINK_DIR/bin/flink run -p 1 $TEST_ES2_JAR \
  --index index \
  --type type

verify_result

shutdown_elasticsearch_cluster

mkdir -p $TEST_DATA_DIR

# start downloading elasticsearch5
echo "Downloading Elasticsearch5 from $ELASTICSEARCH5_URL"
curl "$ELASTICSEARCH5_URL" > $TEST_DATA_DIR/elasticsearch5.tar.gz

tar xzf $TEST_DATA_DIR/elasticsearch5.tar.gz -C $TEST_DATA_DIR/
ELASTICSEARCH5_DIR=$TEST_DATA_DIR/elasticsearch-5.1.2

# start elasticsearch cluster, different from elasticsearch1 since using -daemon here will hang the shell.
nohup $ELASTICSEARCH5_DIR/bin/elasticsearch &

verify_elasticsearch_process_exist

start_cluster

TEST_ES5_JAR=$TEST_DATA_DIR/../../flink-elasticsearch5-test/target/Elasticsearch5SinkExample.jar

# run the Flink job
$FLINK_DIR/bin/flink run -p 1 $TEST_ES5_JAR \
  --index index \
  --type type

verify_result

rm -rf $FLINK_DIR/log/* 2> /dev/null

trap shutdown_elasticsearch_cluster INT
trap shutdown_elasticsearch_cluster EXIT
