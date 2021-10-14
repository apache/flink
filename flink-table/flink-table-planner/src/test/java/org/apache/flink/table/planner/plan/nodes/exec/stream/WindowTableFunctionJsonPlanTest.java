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

/** Test json serialization/deserialization for window table function. */
public class WindowTableFunctionJsonPlanTest extends TableTestBase {

    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();

        String srcTable1Ddl =
                "CREATE TABLE MyTable (\n"
                        + " a INT,\n"
                        + " b BIGINT,\n"
                        + " c VARCHAR,\n"
                        + " `rowtime` AS TO_TIMESTAMP(c),\n"
                        + " proctime as PROCTIME(),\n"
                        + " WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(srcTable1Ddl);

        String srcTable2Ddl =
                "CREATE TABLE MyTable2 (\n"
                        + " a INT,\n"
                        + " b BIGINT,\n"
                        + " c VARCHAR,\n"
                        + " `rowtime` AS TO_TIMESTAMP(c),\n"
                        + " proctime as PROCTIME(),\n"
                        + " WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(srcTable2Ddl);
    }

    @Test
    public void testFollowedByWindowJoin() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " window_start TIMESTAMP(3) NOT NULL,\n"
                        + " window_end TIMESTAMP(3) NOT NULL,\n"
                        + " l_a INT,\n"
                        + " l_b BIGINT,\n"
                        + " l_c VARCHAR,\n"
                        + " r_a INT,\n"
                        + " r_b BIGINT,\n"
                        + " r_c VARCHAR\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  L.window_start,\n"
                        + "  L.window_end,\n"
                        + "  L.a,\n"
                        + "  L.b,\n"
                        + "  L.c,\n"
                        + "  R.a,\n"
                        + "  R.b,\n"
                        + "  R.c\n"
                        + "FROM (\n"
                        + "  SELECT\n"
                        + "    window_start,\n"
                        + "    window_end,\n"
                        + "    a,\n"
                        + "    b,\n"
                        + "    c\n"
                        + "  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))\n"
                        + "  WHERE b > 10\n"
                        + ") L\n"
                        + "JOIN (\n"
                        + "  SELECT\n"
                        + "    window_start,\n"
                        + "    window_end,\n"
                        + "    a,\n"
                        + "    b,\n"
                        + "    c\n"
                        + "  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))\n"
                        + "  WHERE b > 10\n"
                        + ") R\n"
                        + "ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a");
    }

    @Test
    public void testFollowedByWindowRank() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " window_start TIMESTAMP(3),\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " a INT,\n"
                        + " b BIGINT,\n"
                        + " c VARCHAR\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  a,\n"
                        + "  b,\n"
                        + "  c\n"
                        + "FROM (\n"
                        + "  SELECT\n"
                        + "    *,\n"
                        + "   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end ORDER BY b DESC) as rownum\n"
                        + "  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE)))\n"
                        + "WHERE rownum <= 3");
    }
}
