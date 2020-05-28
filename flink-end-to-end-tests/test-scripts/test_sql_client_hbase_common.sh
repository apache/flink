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

HBASE_VERSION=$1

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/hbase_common.sh $HBASE_VERSION

SQL_JARS_DIR=$END_TO_END_DIR/flink-sql-client-test/target/sql-jars
HBASE_SQL_JAR=$(find "$SQL_JARS_DIR" | grep -i "hbase")
FLINK_SHADED_HADOOP_JAR=$TEST_DATA_DIR/flink-shaded-hadoop-2-uber-2.4.1-10.0.jar
SQL_CONF=$TEST_DATA_DIR/sql-client-session.conf
HBASE_CELLS_COUNT=4
SQL_STATEMENT_1=$(cat <<EOF
INSERT INTO MyHBaseSink
  SELECT rowkey, a, b from MyHBaseSource
EOF
)

function sql_cleanup() {
  stop_hbase_cluster
}
on_exit sql_cleanup

################################################################################
# Prepare connectors
################################################################################

echo "Preparing Hbase $HBASE_VERSION..."
setup_hbase_dist
start_hbase_cluster
init_hbase_table
download_flink_shaded_hadoop

################################################################################
# Prepare Flink
################################################################################

echo "Preparing Flink..."
start_cluster
start_taskmanagers 2

################################################################################
# Run SQL statements
################################################################################

echo "Testing SQL statements..."
echo "$SQL_STATEMENT_1"

# set session environment
cat >> $SQL_CONF << EOF
tables:
  - name: MyHBaseSource
    type: source-table
    connector:
      type: "hbase"
      property-version: 1
      version: "$HBASE_VERSION"
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
      version: "$HBASE_VERSION"
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

$FLINK_DIR/bin/sql-client.sh embedded \
  --jar $HBASE_SQL_JAR \
  --jar $FLINK_SHADED_HADOOP_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_1"

echo "Waiting for results..."
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
