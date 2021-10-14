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
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.PandasAggregateFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/** Test json serialization for over aggregate. */
public class PythonOverAggregateJsonPlanTest extends TableTestBase {
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
                        + "  c int not null,\n"
                        + "  rowtime timestamp(3),\n"
                        + "  proctime as PROCTIME(),\n"
                        + "  watermark for rowtime as rowtime"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);
        tEnv.createTemporarySystemFunction("pyFunc", new PandasAggregateFunction());
    }

    @Test
    public void testProcTimeBoundedPartitionedRangeOver() {
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
                        + "    pyFunc(c, c) OVER (PARTITION BY a ORDER BY proctime\n"
                        + "        RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW)\n"
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
                        + "    pyFunc(c, c) OVER (ORDER BY proctime\n"
                        + "        RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW)\n"
                        + " FROM MyTable";
        util.verifyJsonPlan(sql);
    }

    @Test
    public void testProcTimeUnboundedPartitionedRangeOver() {
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
                        + "    pyFunc(c, c) OVER (PARTITION BY a ORDER BY proctime RANGE UNBOUNDED PRECEDING)\n"
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
                "insert into MySink SELECT a,\n"
                        + "    pyFunc(c, c) OVER (PARTITION BY a ORDER BY rowtime\n"
                        + "        ROWS BETWEEN 5 preceding AND CURRENT ROW)\n"
                        + "FROM MyTable";
        util.verifyJsonPlan(sql);
    }

    @Test
    public void testProcTimeBoundedPartitionedRowsOverWithBuiltinProctime() {
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
                "insert into MySink SELECT a, "
                        + "  pyFunc(c, c) OVER ("
                        + "    PARTITION BY a ORDER BY proctime() ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) "
                        + "FROM MyTable";
        util.verifyJsonPlan(sql);
    }
}
