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

/** Test json serialization/deserialization for window join. */
public class WindowJoinJsonPlanTest extends TableTestBase {

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
    public void testEventTimeTumbleWindow() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + " l_a INT,\n"
                        + " window_start TIMESTAMP(3),\n"
                        + " window_end TIMESTAMP(3),\n"
                        + " l_cnt BIGINT,\n"
                        + " l_uv BIGINT,\n"
                        + " r_a INT,\n"
                        + " r_cnt BIGINT,\n"
                        + " r_uv BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values')\n";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select\n"
                        + "  L.a,\n"
                        + "  L.window_start,\n"
                        + "  L.window_end,\n"
                        + "  L.cnt,\n"
                        + "  L.uv,\n"
                        + "  R.a,\n"
                        + "  R.cnt,\n"
                        + "  R.uv\n"
                        + "FROM (\n"
                        + "  SELECT\n"
                        + "    a,\n"
                        + "    window_start,\n"
                        + "    window_end,\n"
                        + "    count(*) as cnt,\n"
                        + "    count(distinct c) AS uv\n"
                        + "  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))\n"
                        + "  GROUP BY a, window_start, window_end, window_time\n"
                        + ") L\n"
                        + "JOIN (\n"
                        + "  SELECT\n"
                        + "    a,\n"
                        + "    window_start,\n"
                        + "    window_end,\n"
                        + "    count(*) as cnt,\n"
                        + "    count(distinct c) AS uv\n"
                        + "  FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))\n"
                        + "  GROUP BY a, window_start, window_end, window_time\n"
                        + ") R\n"
                        + "ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.a");
    }
}
