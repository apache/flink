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

/** Test json serialization for match recognize. */
public class MatchRecognizeJsonPlanTest extends TableTestBase {
    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();
    }

    @Test
    public void testMatch() {
        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  id bigint,\n"
                        + "  name varchar,\n"
                        + "  proctime as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);
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
                "insert into MySink"
                        + " SELECT T.aid, T.bid, T.cid\n"
                        + "     FROM MyTable MATCH_RECOGNIZE (\n"
                        + "             ORDER BY proctime\n"
                        + "             MEASURES\n"
                        + "             `A\"`.id AS aid,\n"
                        + "             \u006C.id AS bid,\n"
                        + "             C.id AS cid\n"
                        + "             PATTERN (`A\"` \u006C C)\n"
                        + "             DEFINE\n"
                        + "                 `A\"` AS name = 'a',\n"
                        + "                 \u006C AS name = 'b',\n"
                        + "                 C AS name = 'c'\n"
                        + "     ) AS T";
        util.verifyJsonPlan(sql);
    }

    @Test
    public void testSkipToLast() {
        doTestAfterMatch("AFTER MATCH SKIP TO LAST B");
    }

    @Test
    public void testSkipToFirst() {
        doTestAfterMatch("AFTER MATCH SKIP TO FIRST B");
    }

    @Test
    public void testSkipPastLastRow() {
        doTestAfterMatch("AFTER MATCH SKIP PAST LAST ROW");
    }

    @Test
    public void testSkipToNextRow() {
        doTestAfterMatch("AFTER MATCH SKIP TO NEXT ROW");
    }

    private void doTestAfterMatch(Object afterClause) {
        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  vehicle_id bigint,\n"
                        + "  engine_temperature int,\n"
                        + "  rowtime timestamp_ltz(3),"
                        + "  WATERMARK FOR rowtime AS SOURCE_WATERMARK()\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  vehicle_id bigint,\n"
                        + "  startTime timestamp_ltz(3),\n"
                        + "  endTime timestamp_ltz(3),\n"
                        + "  Initial_Temp int,\n"
                        + "  Final_Temp int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        String sql =
                "insert into MySink"
                        + " SELECT * FROM\n"
                        + " MyTable\n"
                        + "   MATCH_RECOGNIZE(\n"
                        + "   PARTITION BY vehicle_id\n"
                        + "   ORDER BY `rowtime`\n"
                        + "   MEASURES \n"
                        + "       FIRST(A.`rowtime`) as startTime,\n"
                        + "       LAST(A.`rowtime`) as endTime,\n"
                        + "       FIRST(A.engine_temperature) as Initial_Temp,\n"
                        + "       LAST(A.engine_temperature) as Final_Temp\n"
                        + "   ONE ROW PER MATCH\n"
                        + "   %s\n"
                        + "   PATTERN (A+ B)\n"
                        + "   DEFINE\n"
                        + "       A as LAST(A.engine_temperature,1) is NULL OR A.engine_temperature > LAST(A.engine_temperature,1),\n"
                        + "       B as B.engine_temperature < LAST(A.engine_temperature)\n"
                        + "   )MR;";
        util.verifyJsonPlan(String.format(sql, afterClause));
    }
}
