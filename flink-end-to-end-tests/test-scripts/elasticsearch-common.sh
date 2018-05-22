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
  echo "Must run common.sh before kafka-common.sh."
  exit 1
fi

function verify_elasticsearch_process_exist {
    ELASTICSEARCH_PROCESS=$(jps | grep Elasticsearch | awk '{print $2}')

    # make sure the elasticsearch node is actually running
    if [ "$ELASTICSEARCH_PROCESS" != "Elasticsearch" ]; then
      echo "Elasticsearch node is not running."
      PASS=""
      exit 1
    else
      echo "Elasticsearch node is running."
    fi
}

function verify_result {
    if [ -f "$TEST_DATA_DIR/output" ]; then
        rm $TEST_DATA_DIR/output
    fi

    curl 'localhost:9200/index/_search?q=*&pretty&size=21' > $TEST_DATA_DIR/output

    if [ -n "$(grep '\"total\" : 21' $TEST_DATA_DIR/output)" ]; then
        echo "Elasticsearch end to end test pass."
    else
        echo "Elasticsearch end to end test failed."
        PASS=""
        exit 1
    fi
}

function shutdown_elasticsearch_cluster {
   pid=$(jps | grep Elasticsearch | awk '{print $1}')
   kill -SIGTERM $pid

   # make sure to run regular cleanup as well
   cleanup
}
