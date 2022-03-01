/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/** Test json serialization for over aggregate. */
public class OverAggregateJsonPlanTest extends TableTestBase {
    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();
        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a int,\n"
                        + "  b varchar,\n"
                        + "  c bigint,\n"
                        + "  rowtime timestamp(3),\n"
                        + "  proctime as PROCTIME(),\n"
                        + "  watermark for rowtime as rowtime"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);
    }

    @Test
    public void testProctimeBoundedDistinctWithNonDistinctPartitionedRowOver() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a varchar,\n"
                        + "  b bigint,\n"
                        + "  c bigint,\n"
                        + "  d bigint,\n"
                        + "  e bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        String sql =
                "insert into MySink SELECT b,\n"
                        + "    COUNT(a) OVER (PARTITION BY b ORDER BY proctime\n"
                        + "        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt1,\n"
                        + "    SUM(a) OVER (PARTITION BY b ORDER BY proctime\n"
                        + "        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum1,\n"
                        + "    COUNT(DISTINCT a) OVER (PARTITION BY b ORDER BY proctime\n"
                        + "        ROWS BETWEEN 2 preceding AND CURRENT ROW) AS cnt2,\n"
                        + "    sum(DISTINCT c) OVER (PARTITION BY b ORDER BY proctime\n"
                        + "        ROWS BETWEEN 2 preceding AND CURRENT ROW) AS sum2\n"
                        + "FROM MyTable";
        util.verifyJsonPlan(sql);
    }

    @Test
    public void testProctimeBoundedDistinctPartitionedRowOver() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b bigint,\n"
                        + "  c bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        String sql =
                "insert into MySink SELECT c,\n"
                        + "    COUNT(DISTINCT a) OVER (PARTITION BY c ORDER BY proctime\n"
                        + "        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt1,\n"
                        + "    SUM(DISTINCT a) OVER (PARTITION BY c ORDER BY proctime\n"
                        + "        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum1\n"
                        + "FROM MyTable";
        util.verifyJsonPlan(sql);
    }

    @Test
    public void testProcTimeBoundedPartitionedRangeOver() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b double\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        String sql =
                "insert into MySink SELECT a,\n"
                        + "    AVG(c) OVER (PARTITION BY a ORDER BY proctime\n"
                        + "        RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW) AS avgA\n"
                        + "FROM MyTable";
        util.verifyJsonPlan(sql);
    }

    @Test
    public void testProcTimeBoundedNonPartitionedRangeOver() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        String sql =
                "insert into MySink SELECT a,\n"
                        + "    COUNT(c) OVER (ORDER BY proctime\n"
                        + "        RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW)\n"
                        + " FROM MyTable";
        util.verifyJsonPlan(sql);
    }

    @Test
    public void testProcTimeUnboundedPartitionedRangeOver() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b bigint,\n"
                        + "  c bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        String sql =
                "insert into MySink SELECT c,\n"
                        + "    COUNT(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED PRECEDING) AS cnt1,\n"
                        + "    SUM(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED PRECEDING) AS cnt2\n"
                        + "FROM MyTable";
        util.verifyJsonPlan(sql);
    }

    @Test
    public void testRowTimeBoundedPartitionedRowsOver() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        String sql =
                "insert into MySink SELECT c,\n"
                        + "    COUNT(a) OVER (PARTITION BY c ORDER BY rowtime\n"
                        + "        ROWS BETWEEN 5 preceding AND CURRENT ROW)\n"
                        + "FROM MyTable";
        util.verifyJsonPlan(sql);
    }

    @Test
    public void testProcTimeBoundedPartitionedRowsOverWithBuiltinProctime() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b bigint,\n"
                        + "  c bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        String sql =
                "insert into MySink SELECT a, "
                        + "  SUM(c) OVER ("
                        + "    PARTITION BY a ORDER BY proctime() ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), "
                        + "  MIN(c) OVER ("
                        + "    PARTITION BY a ORDER BY proctime() ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) "
                        + "FROM MyTable";
        util.verifyJsonPlan(sql);
    }
}
