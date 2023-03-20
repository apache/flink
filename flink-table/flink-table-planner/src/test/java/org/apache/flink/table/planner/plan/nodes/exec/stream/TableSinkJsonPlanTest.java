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
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/** Test json serialization/deserialization for table sink. */
public class TableSinkJsonPlanTest extends TableTestBase {

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
    }

    @Test
    public void testOverwrite() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'filesystem',\n"
                        + "  'format' = 'testcsv',\n"
                        + "  'path' = '/tmp')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("insert overwrite MySink select * from MyTable");
    }

    @Test
    public void testPartitioning() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") partitioned by (c) with (\n"
                        + "  'connector' = 'filesystem',\n"
                        + "  'format' = 'testcsv',\n"
                        + "  'path' = '/tmp')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("insert into MySink partition (c='A') select a, b from MyTable");
    }

    @Test
    public void testWritingMetadata() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  m varchar METADATA\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'writable-metadata' = 'm:STRING')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("insert into MySink select * from MyTable");
    }

    @Test
    public void testCdcWithNonDeterministicFuncSinkWithDifferentPk() {
        tEnv.createTemporaryFunction(
                "ndFunc", new JavaUserDefinedScalarFunctions.NonDeterministicUdf());

        String cdcDdl =
                "CREATE TABLE users (\n"
                        + "  user_id STRING,\n"
                        + "  user_name STRING,\n"
                        + "  email STRING,\n"
                        + "  balance DECIMAL(18,2),\n"
                        + "  primary key (user_id) not enforced\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'changelog-mode' = 'I,UA,UB,D'\n"
                        + ")";

        String sinkTableDdl =
                "CREATE TABLE sink (\n"
                        + "  user_id STRING,\n"
                        + "  user_name STRING,\n"
                        + "  email STRING,\n"
                        + "  balance DECIMAL(18,2),\n"
                        + "  PRIMARY KEY(email) NOT ENFORCED\n"
                        + ") WITH(\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false'\n"
                        + ")";
        tEnv.executeSql(cdcDdl);
        tEnv.executeSql(sinkTableDdl);

        util.verifyJsonPlan(
                "insert into sink select user_id, ndFunc(user_name), email, balance from users");
    }

    @Test
    public void testPartialInsert() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar,\n"
                        + "  d int,\n"
                        + "  e double,\n"
                        + "  f varchar\n"
                        + ") partitioned by (c) with (\n"
                        + "  'connector' = 'filesystem',\n"
                        + "  'format' = 'testcsv',\n"
                        + "  'path' = '/tmp')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink partition (c='A') (f,a,b) select c, a, b from MyTable");
    }
}
