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

/** Test json serialization/deserialization for calc. */
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

    @Test
    public void testLeftJoinWithEqualPkNonEqui() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format(
                        "SELECT a1, b1 FROM (%s) LEFT JOIN (%s) ON a1 = b1 AND a2 > b2",
                        query1, query2);
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a1 int,\n"
                        + "  a2 bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(String.format("INSERT INTO MySink %s", query));
    }

    @Test
    public void testLeftJoinWithRightNotPkNonEqui() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query =
                String.format("SELECT a1, b1 FROM (%s) LEFT JOIN B ON a1 = b1 AND a2 > b2", query1);
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
    public void testLeftJoinWithPkNonEqui() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format(
                        "SELECT a1, a2, b1, b2 FROM (%s) LEFT JOIN (%s) ON a2 = b2 AND a1 > b1",
                        query1, query2);
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
    public void testLeftJoin() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a1 int,\n"
                        + "  b1 int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("INSERT INTO MySink SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1");
    }

    @Test
    public void testLeftJoinWithEqualPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format("SELECT a1, b1 FROM (%s) LEFT JOIN (%s) ON a1 = b1", query1, query2);
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
    public void testLeftJoinWithRightNotPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query = String.format("SELECT a1, b1 FROM (%s) LEFT JOIN B ON a1 = b1", query1);
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
    public void testLeftJoinWithPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format(
                        "SELECT a1, a2, b1, b2 FROM (%s) LEFT JOIN (%s) ON a2 = b2",
                        query1, query2);
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
    public void testRightJoinNonEqui() {
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
                "INSERT INTO MySink SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1 AND a2 > b2");
    }

    @Test
    public void testRightJoinWithEqualPkNonEqui() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format(
                        "SELECT a1, b1 FROM (%s) RIGHT JOIN (%s) ON a1 = b1 AND a2 > b2",
                        query1, query2);
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
    public void testRightJoinWithRightNotPkNonEqui() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query =
                String.format(
                        "SELECT a1, b1 FROM (%s) RIGHT JOIN B ON a1 = b1 AND a2 > b2", query1);
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
    public void testRightJoinWithPkNonEqui() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format(
                        "SELECT a1, a2, b1, b2 FROM (%s) RIGHT JOIN (%s) ON a2 = b2 AND a1 > b1",
                        query1, query2);
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
    public void testRightJoin() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a1 int,\n"
                        + "  b1 int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("INSERT INTO MySink SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1");
    }

    @Test
    public void testRightJoinWithEqualPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format("SELECT a1, b1 FROM (%s) RIGHT JOIN (%s) ON a1 = b1", query1, query2);
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
    public void testRightJoinWithRightNotPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1";
        String query = String.format("SELECT a1, b1 FROM (%s) RIGHT JOIN B ON a1 = b1", query1);
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
    public void testRightJoinWithPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1";
        String query =
                String.format(
                        "SELECT a1, a2, b1, b2 FROM (%s) RIGHT JOIN (%s) ON a2 = b2",
                        query1, query2);
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
    public void testFullJoinNonEqui() {
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
                "INSERT INTO MySink SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1 AND a2 > b2");
    }

    @Test
    public void testFullJoinWithEqualPkNonEqui() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format(
                        "SELECT a1, b1 FROM (%s) FULL JOIN (%s) ON a1 = b1 AND a2 > b2",
                        query1, query2);
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
    public void testFullJoinWithFullNotPkNonEqui() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query =
                String.format("SELECT a1, b1 FROM (%s) FULL JOIN B ON a1 = b1 AND a2 > b2", query1);
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
    public void testFullJoinWithPkNonEqui() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format(
                        "SELECT a1, a2, b1, b2 FROM (%s) FULL JOIN (%s) ON a2 = b2 AND a1 > b1",
                        query1, query2);
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
    public void testFullJoin() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a1 int,\n"
                        + "  b1 int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("INSERT INTO MySink SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1");
    }

    @Test
    public void testFullJoinWithEqualPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format("SELECT a1, b1 FROM (%s) FULL JOIN (%s) ON a1 = b1", query1, query2);
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
    public void testFullJoinWithFullNotPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query = String.format("SELECT a1, b1 FROM (%s) FULL JOIN B ON a1 = b1", query1);
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
    public void testFullJoinWithPk() {
        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1";
        String query =
                String.format(
                        "SELECT a1, a2, b1, b2 FROM (%s) FULL JOIN (%s) ON a2 = b2",
                        query1, query2);
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
    public void testLeftOuterJoinEquiPred() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  b bigint,\n"
                        + "  y varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                String.format(
                        "INSERT INTO MySink %s", "SELECT b, y FROM t LEFT OUTER JOIN s ON a = z"));
    }

    @Test
    public void testLeftOuterJoinEquiAndLocalPred() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  b bigint,\n"
                        + "  y varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                String.format(
                        "INSERT INTO MySink %s",
                        "SELECT b, y FROM t LEFT OUTER JOIN s ON a = z AND b < 2"));
    }

    @Test
    public void testLeftOuterJoinEquiAndNonEquiPred() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  b bigint,\n"
                        + "  y varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                String.format(
                        "INSERT INTO MySink %s",
                        "SELECT b, y FROM t LEFT OUTER JOIN s ON a = z AND b < x"));
    }

    @Test
    public void testRightOuterJoinEquiPred() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  b bigint,\n"
                        + "  y varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                String.format(
                        "INSERT INTO MySink %s", "SELECT b, y FROM t RIGHT OUTER JOIN s ON a = z"));
    }

    @Test
    public void testRightOuterJoinEquiAndLocalPred() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  b bigint,\n"
                        + "  x bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                String.format(
                        "INSERT INTO MySink %s",
                        "SELECT b, x FROM t RIGHT OUTER JOIN s ON a = z AND x < 2"));
    }

    @Test
    public void testRightOuterJoinEquiAndNonEquiPred() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  b bigint,\n"
                        + "  y varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                String.format(
                        "INSERT INTO MySink %s",
                        "SELECT b, y FROM t RIGHT OUTER JOIN s ON a = z AND b < x"));
    }
}
