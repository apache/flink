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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test json serialization/deserialization for table source. */
public class TableSourceJsonPlanTest extends TableTestBase {

    @Rule public ExpectedException exception = ExpectedException.none();

    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);

        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
    }

    @Test
    public void testProjectPushDown() {
        String sinkTableDdl =
                "CREATE TABLE sink (\n"
                        + "  a bigint,\n"
                        + "  b int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("insert into sink select a, b from MyTable");
    }

    @Test
    public void testReadingMetadata() {
        String srcTableDdl =
                "CREATE TABLE MyTable2 (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar,\n"
                        + "  m varchar metadata\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'readable-metadata' = 'm:STRING',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);
        String sinkTableDdl =
                "CREATE TABLE sink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  m varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("insert into sink select a, b, m from MyTable2");
    }

    @Test
    public void testFilterPushDown() {
        String srcTableDdl =
                "CREATE TABLE src (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false',"
                        + "  'filterable-fields' = 'a')";
        tEnv.executeSql(srcTableDdl);
        util.verifyJsonPlan("insert into MySink select * from src where a > 0");
    }

    @Test
    public void testLimitPushDown() {
        util.verifyJsonPlan("insert into MySink select * from MyTable limit 3");
    }

    @Test
    public void testPartitionPushDown() {
        String srcTableDdl =
                "CREATE TABLE PartitionTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  p varchar)\n"
                        + "partitioned by (p)\n"
                        + "with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false',"
                        + "  'partition-list' = 'p:A')";
        tEnv.executeSql(srcTableDdl);
        util.verifyJsonPlan("insert into MySink select * from PartitionTable where p = 'A'");
    }

    @Test
    public void testWatermarkPushDown() {
        String srcTableDdl =
                "CREATE TABLE WatermarkTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c timestamp(3),\n"
                        + "  watermark for c as c - interval '5' second\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false',"
                        + "  'enable-watermark-push-down' = 'true',"
                        + "  'disable-lookup' = 'true')";
        tEnv.executeSql(srcTableDdl);
        String sinkTableDdl =
                "CREATE TABLE sink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c timestamp(3)\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("insert into sink select * from WatermarkTable");
    }
}
