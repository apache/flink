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

HBASE_VERSION=1.4.10
HBASE_DIR=$TEST_DATA_DIR/hbase-$HBASE_VERSION
SQL_CONF=$TEST_DATA_DIR/sql-client-session.conf

SQL_JARS_DIR=$END_TO_END_DIR/flink-sql-client-test/target/sql-jars
HBASE_SQL_JAR=$(find "$SQL_JARS_DIR" | grep -i "hbase")

if [[ -z $TEST_DATA_DIR ]]; then
  echo "$TEST_DATA_DIR does not exist."
  echo "Usage: ./run-single-test.sh test-scripts/test_sql_client_hbase.sh"
  exit 1
fi

function setup_hbase_dist {
  HBASE_URL="https://mirrors.tuna.tsinghua.edu.cn/apache/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz"
  echo "Downloading hbase dist package from $HBASE_URL"
  curl "$HBASE_URL" --retry 10 --retry-max-time 120 > $TEST_DATA_DIR/hbase.tgz

  tar xzf $TEST_DATA_DIR/hbase.tgz -C $TEST_DATA_DIR/
}

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

HBASE_LOAD_DATA_SCRIPT=$(cat<<EOF
  put 'source_table', 'row1', 'a:c1', 'value1'
  put 'source_table', 'row1', 'b:c1', 'value2'
  put 'source_table', 'row2', 'a:c1', 'value3'
  put 'source_table', 'row2', 'b:c1', 'value4'
EOF
)
HBASE_CELLS_COUNT=4

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

  # Initialize the source & sink hbase table
  echo "Initialize the source and sink hbase table"
  TABLE_LIST=("source_table" "sink_table")
  for table in "${TABLE_LIST[@]}"; do
    rc=$(echo "create '$table', {NAME=>'a'}, {NAME=>'b'}" | $HBASE_DIR/bin/hbase shell >>$TEST_DATA_DIR/hbase.log 2>&1)
    if [[ $rc -ne 0 ]]; then
      echo "Failed to create the hbase table: $table"
      exit 1
    fi
  done

  # Load some rows into hbase source table
  echo "Start to load some rows into hbase source table"
  echo "$HBASE_LOAD_DATA_SCRIPT" | $HBASE_DIR/bin/hbase shell
}

function stop_hbase_cluster () {
  $HBASE_DIR/bin/stop-hbase.sh

  PIDS=$(jps -vl | grep java | grep -i HMaster | grep -v grep | awk '{print $1}'|| echo "")
  if [ ! -z "$PIDS" ]; then
    kill -s TERM $PIDS || true
  fi
}

################################################################################
# Prepare the HBase connector
################################################################################

function sql_cleanup() {
  stop_hbase_cluster
}
on_exit sql_cleanup

echo "Prepare the HBase-$HBASE_VERSION standalone cluster..."

setup_hbase_dist

cat > $HBASE_DIR/conf/hbase-site.xml << EOF
<configuration>
  <property>
    <name>hbase.tmp.dir</name>
    <value>$TEST_DATA_DIR</value>
  </property>
</configuration>
EOF

start_hbase_cluster

################################################################################
# Prepare the Flink cluster
################################################################################

echo "Preparing Flink cluster..."

start_cluster
start_taskmanagers 2

################################################################################
# Run the SQL statements
################################################################################

cat >> $SQL_CONF << EOF
tables:
  - name: MyHBaseSource
    type: source-table
    connector:
      type: "hbase"
      property-version: 1
      version: "1.4.3"
      table-name: "source_table"
      zookeeper:
        quorum: "localhost:2181"
        znode.parent: "/hbase"
    schema:
      - name: rowkey
        type: STRING
      - name: a
        type: ROW<c1 STRING>
      - name: b
        type: ROW<c1 STRING>

  - name: MyHBaseSink
    type: sink-table
    connector:
      type: "hbase"
      property-version: 1
      version: "1.4.3"
      table-name: "sink_table"
      zookeeper:
        quorum: "localhost:2181"
        znode.parent: "/hbase"
      write.buffer-flush:
        max-size: "2mb"
        max-rows: 1000
        interval: "2s"
    schema:
      - name: rowkey
        type: STRING
      - name: a
        type: ROW<c1 STRING>
      - name: b
        type: ROW<c1 STRING>
EOF

SQL_STATEMENT_1=$(cat <<EOF
  INSERT INTO MyHBaseSink
    SELECT rowkey, a, b from MyHBaseSource
EOF
)

echo "$SQL_STATEMENT_1"

$FLINK_DIR/bin/sql-client.sh embedded \
  --jar $HBASE_SQL_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_1"

for i in {0..10}; do
  COUNT=$(echo "scan 'sink_table'"  | $HBASE_DIR/bin/hbase shell | grep value= | wc -l | awk '{print $1}')
  if [[ "$COUNT" == "$HBASE_CELLS_COUNT" ]]; then
    echo "Got the expected $COUNT cells, finished the HBase end-to-end test successfully."
    exit 0
  fi
  sleep 5
done

echo "Timeout(50 seconds) to get the expected $HBASE_CELLS_COUNT cells, Please check the hbase end-to-end test log."
exit 1
