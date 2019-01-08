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

if [[ -z $TEST_DATA_DIR ]]; then
  echo "Must run common.sh before elasticsearch-common.sh."
  exit 1
fi

function setup_elasticsearch {
    mkdir -p $TEST_DATA_DIR

    local downloadUrl=$1

    # start downloading Elasticsearch
    echo "Downloading Elasticsearch from $downloadUrl ..."
    curl "$downloadUrl" > $TEST_DATA_DIR/elasticsearch.tar.gz

    local elasticsearchDir=$TEST_DATA_DIR/elasticsearch
    mkdir -p $elasticsearchDir
    tar xzf $TEST_DATA_DIR/elasticsearch.tar.gz -C $elasticsearchDir --strip-components=1

    # start Elasticsearch cluster
    $elasticsearchDir/bin/elasticsearch &
}

function verify_elasticsearch_process_exist {
    for ((i=1;i<=10;i++)); do
        local elasticsearchProcess=$(jps | grep Elasticsearch | awk '{print $2}')

        echo "Waiting for Elasticsearch node to start ..."

        # make sure the elasticsearch node is actually running
        if [ "$elasticsearchProcess" != "Elasticsearch" ]; then
            sleep 1
        else
            echo "Elasticsearch node is running."
            return
        fi
    done

    echo "Elasticsearch node did not start properly"
    exit 1
}

function verify_result {
    local numRecords=$1
    local index=$2

    if [ -f "$TEST_DATA_DIR/output" ]; then
        rm $TEST_DATA_DIR/output
    fi

    while : ; do
      curl "localhost:9200/${index}/_search?q=*&pretty&size=21" > $TEST_DATA_DIR/output

      if [ -n "$(grep "\"total\" : $numRecords" $TEST_DATA_DIR/output)" ]; then
          echo "Elasticsearch end to end test pass."
          break
      else
          echo "Waiting for Elasticsearch records ..."
          sleep 1
      fi
    done
}

function shutdown_elasticsearch_cluster {
   local index=$1
   curl -X DELETE "http://localhost:9200/${index}"
   pid=$(jps | grep Elasticsearch | awk '{print $1}')
   kill -9 $pid
}
