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

/** Test json serialization/deserialization for IntervalJoin. */
public class IntervalJoinJsonPlanTest extends TableTestBase {

    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();

        String srcTableA =
                "CREATE TABLE A (\n"
                        + "  a int,\n"
                        + "  b varchar,\n"
                        + "  c bigint,\n"
                        + "  proctime as PROCTIME(),\n"
                        + "  rowtime as TO_TIMESTAMP(FROM_UNIXTIME(c)),\n"
                        + "  watermark for rowtime as rowtime - INTERVAL '1' second \n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        String srcTableB =
                "CREATE TABLE B (\n"
                        + "  a int,\n"
                        + "  b varchar,\n"
                        + "  c bigint,\n"
                        + "  proctime as PROCTIME(),\n"
                        + "  rowtime as TO_TIMESTAMP(FROM_UNIXTIME(c)),\n"
                        + "  watermark for rowtime as rowtime - INTERVAL '1' second \n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableA);
        tEnv.executeSql(srcTableB);
    }

    @Test
    public void testProcessingTimeInnerJoinWithOnClause() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a int,\n"
                        + "  b varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "INSERT INTO MySink "
                        + " SELECT t1.a, t2.b FROM A t1 JOIN B t2 ON\n"
                        + "    t1.a = t2.a AND \n"
                        + "    t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR");
    }

    @Test
    public void testRowTimeInnerJoinWithOnClause() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a int,\n"
                        + "  b varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "INSERT INTO MySink "
                        + "SELECT t1.a, t2.b FROM A t1 JOIN B t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR");
    }
}
