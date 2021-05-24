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

/** Test json serialization/deserialization for join. */
public class JoinJsonPlanTest extends TableTestBase {

    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();

        String srcTableA =
                "CREATE TABLE A (\n"
                        + "  a1 int,\n"
                        + "  a2 bigint,\n"
                        + "  a3 bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        String srcTableB =
                "CREATE TABLE B (\n"
                        + "  b1 int,\n"
                        + "  b2 bigint,\n"
                        + "  b3 bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        String srcTableT =
                "CREATE TABLE t (\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        String srcTableS =
                "CREATE TABLE s (\n"
                        + "  x bigint,\n"
                        + "  y varchar,\n"
                        + "  z int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableA);
        tEnv.executeSql(srcTableB);
        tEnv.executeSql(srcTableT);
        tEnv.executeSql(srcTableS);
    }

    @Test
    public void testInnerJoin() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a1 int,\n"
                        + "  b1 int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("INSERT INTO MySink SELECT a1, b1 FROM A JOIN B ON a1 = b1");
    }

    @Test
    public void testInnerJoinWithEqualPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format("SELECT a1, b1 FROM (%s) JOIN (%s) ON a1 = b1", query1, query2);
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a1 int,\n"
                        + "  b1 int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(String.format("INSERT INTO MySink %s", query));
    }

    @Test
    public void testInnerJoinWithPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format(
                        "SELECT a1, a2, b1, b2 FROM (%s) JOIN (%s) ON a2 = b2", query1, query2);
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a1 int,\n"
                        + "  a2 bigint,\n"
                        + "  b1 int,\n"
                        + "  b2 bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(String.format("INSERT INTO MySink %s", query));
    }

    @Test
    public void testLeftJoinNonEqui() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a1 int,\n"
                        + "  b1 int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "INSERT INTO MySink SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1 AND a2 > b2");
    }
}
