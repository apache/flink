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
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.TestPythonAggregateFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/** Test json serialization/deserialization for group window aggregate. */
public class PythonGroupWindowAggregateJsonPlanTest extends TableTestBase {
    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + " a INT NOT NULL,\n"
                        + " b BIGINT,\n"
                        + " c VARCHAR,\n"
                        + " `rowtime` AS TO_TIMESTAMP(c),\n"
                        + " proctime as PROCTIME(),\n"
                        + " WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(srcTableDdl);
        tEnv.createTemporarySystemFunction("pyFunc", new TestPythonAggregateFunction());
    }

    @Test
    public void testEventTimeTumbleWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " window_start TIMESTAMP(3),\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " c BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  TUMBLE_START(rowtime, INTERVAL '5' SECOND) as window_start,\n"
                        + "  TUMBLE_END(rowtime, INTERVAL '5' SECOND) as window_end,\n"
                        + "  pyFunc(a, a + 1)\n"
                        + "FROM MyTable\n"
                        + "GROUP BY b, TUMBLE(rowtime, INTERVAL '5' SECOND)");
    }

    @Test
    public void testProcTimeTumbleWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " c BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  TUMBLE_END(proctime, INTERVAL '15' MINUTE) as window_end,\n"
                        + "  pyFunc(a, a + 1)\n"
                        + "FROM MyTable\n"
                        + "GROUP BY b, TUMBLE(proctime, INTERVAL '15' MINUTE)");
    }

    @Test
    public void testEventTimeHopWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " c BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  pyFunc(a, a + 1)\n"
                        + "FROM MyTable\n"
                        + "GROUP BY b, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND)");
    }

    @Test
    public void testProcTimeHopWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " c BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  pyFunc(a, a + 1)\n"
                        + "FROM MyTable\n"
                        + "GROUP BY b, HOP(proctime, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE)");
    }

    @Test
    public void testEventTimeSessionWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " c BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  pyFunc(a, a + 1)\n"
                        + "FROM MyTable\n"
                        + "GROUP BY b, Session(rowtime, INTERVAL '10' SECOND)");
    }

    @Test
    public void testProcTimeSessionWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " b BIGINT,\n"
                        + " c BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  b,\n"
                        + "  pyFunc(a, a + 1)\n"
                        + "FROM MyTable\n"
                        + "GROUP BY b, Session(proctime, INTERVAL '10' MINUTE)");
    }
}
