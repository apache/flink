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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableFunc1;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Test json serialization/deserialization for correlate. */
public class CorrelateJsonPlanTest extends TableTestBase {
    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int not null,\n"
                        + "  c varchar,\n"
                        + "  d timestamp(3)\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);
    }

    @Test
    public void testCrossJoin() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a varchar,\n"
                        + "  b varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        TableFunc1 func1 = new TableFunc1();
        util.addFunction("func1", func1, Types.STRING);
        String sqlQuery =
                "insert into MySink SELECT c, s FROM MyTable, LATERAL TABLE(func1(c)) AS T(s)";
        util.verifyJsonPlan(sqlQuery);
    }

    @Test
    @Ignore("the case is ignored because of FLINK-21870")
    public void testRegisterByClass() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a varchar,\n"
                        + "  b varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        tEnv.createTemporaryFunction("func1", TableFunc1.class);
        String sqlQuery =
                "insert into MySink SELECT c, s FROM MyTable, LATERAL TABLE(func1(c)) AS T(s)";
        util.verifyJsonPlan(sqlQuery);
    }

    @Test
    public void testCrossJoinOverrideParameters() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a varchar,\n"
                        + "  b varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        TableFunc1 func1 = new TableFunc1();
        util.addFunction("func1", func1, Types.STRING);
        String sqlQuery =
                "insert into MySink SELECT c, s FROM MyTable, LATERAL TABLE(func1(c, '$')) AS T(s)";
        util.verifyJsonPlan(sqlQuery);
    }

    @Test
    public void testLeftOuterJoinWithLiteralTrue() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a varchar,\n"
                        + "  b varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        TableFunc1 func1 = new TableFunc1();
        util.addFunction("func1", func1, Types.STRING);
        String sqlQuery =
                "insert into MySink SELECT c, s FROM MyTable LEFT JOIN LATERAL TABLE(func1(c)) AS T(s) ON TRUE";
        util.verifyJsonPlan(sqlQuery);
    }

    @Test
    public void testJoinWithFilter() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a varchar,\n"
                        + "  b varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        TableFunc1 func1 = new TableFunc1();
        util.addFunction("func1", func1, Types.STRING);
        String sqlQuery =
                "insert into MySink "
                        + "select * from (SELECT c, s FROM MyTable, LATERAL TABLE(func1(c)) AS T(s)) as T2 where c = s";
        util.verifyJsonPlan(sqlQuery);
    }
}
