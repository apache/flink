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

if [[ -z $TEST_DATA_DIR ]]; then
  echo "Must run common.sh before hbase_common.sh."
  exit 1
fi

HBASE_VERSION="$1"
HBASE_DIR=$TEST_DATA_DIR/hbase-$HBASE_VERSION

# deploy a standalone hbase
function setup_hbase_dist {
    local downloadUrl="http://archive.apache.org/dist/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz"

    # start downloading Hbase
    echo "Downloading Hbase from $downloadUrl ..."
    curl "$downloadUrl" > $TEST_DATA_DIR/hbase.tar.gz

    mkdir -p $HBASE_DIR
    tar xzf $TEST_DATA_DIR/hbase.tar.gz -C $HBASE_DIR --strip-components=1

    cat > $HBASE_DIR/conf/hbase-site.xml << EOF
<configuration>
  <property>
    <name>hbase.tmp.dir</name>
    <value>$TEST_DATA_DIR</value>
  </property>
</configuration>
EOF
}

# download flink-shaded-hadoop-2-uber-2.4.1-10.0.jar
function download_flink_shaded_hadoop {
    local hadoopJar="https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.4.1-10
    .0/flink-shaded-hadoop-2-uber-2.4.1-10.0.jar"

    # start downloading Hbase
    echo "Downloading flink-shaded-hadoop-2-uber-2.4.1-10.0.jar from $hadoopJar ..."
    curl "$hadoopJar" > $TEST_DATA_DIR/flink-shaded-hadoop-2-uber-2.4.1-10.0.jar
}

# start standalone hbase
function start_hbase_cluster {
  if [[ -z $HBASE_DIR ]]; then
    echo "Must run 'setup_hbase_dist' before attempting to start hbase cluster"
    exit 1
  fi

  $HBASE_DIR/bin/start-hbase.sh

  # Wait until the hbase:meta table to be online.
  block_until_meta_online
  if [[ $? -ne 0 ]]; then
    echo "Timeout(30 seconds) to wait the hbase:meta table to be online"
    exit 1
  fi
}

# stop hbase cluster
function stop_hbase_cluster () {
  $HBASE_DIR/bin/stop-hbase.sh

  # Terminate hbase process if it still exists
  PIDS=$(jps -vl | grep -i HMaster | grep -v grep | awk '{print $1}'|| echo "")
  if [ ! -z "$PIDS" ]; then
    kill -s TERM $PIDS || true
  fi
}

# Wait until the hbase:meta table to be online.
function block_until_meta_online {
  for i in {1..30}; do
    result=$(echo 'describe "hbase:meta"' | $HBASE_DIR/bin/hbase shell | grep 'Table hbase:meta is ENABLED')
    if [[ "$result" == "Table hbase:meta is ENABLED" ]]; then
      return 0;
    fi
    sleep 1;
  done
  return 1;
}

# initialize hbase source table & sink table
function init_hbase_table {
  echo "create 'source_table', {NAME=>'a'}, {NAME=>'b'}" | $HBASE_DIR/bin/hbase shell >> $TEST_DATA_DIR/hbase.log 2>&1
  echo "create 'sink_table', {NAME=>'a'}, {NAME=>'b'}" | $HBASE_DIR/bin/hbase shell >> $TEST_DATA_DIR/hbase.log 2>&1
  echo "put 'source_table', 'row1', 'a:c1', 'value1'" | $HBASE_DIR/bin/hbase shell
  echo "put 'source_table', 'row1', 'b:c1', 'value2'" | $HBASE_DIR/bin/hbase shell
  echo "put 'source_table', 'row2', 'a:c1', 'value3'" | $HBASE_DIR/bin/hbase shell
  echo "put 'source_table', 'row2', 'b:c1', 'value4'" | $HBASE_DIR/bin/hbase shell
}
