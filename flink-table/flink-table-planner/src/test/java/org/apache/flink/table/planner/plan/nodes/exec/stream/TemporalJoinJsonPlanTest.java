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
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/** Test json serialization/deserialization for TemporalJoin. */
public class TemporalJoinJsonPlanTest extends TableTestBase {

    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();

        tEnv.executeSql(
                "CREATE TABLE Orders (\n"
                        + " amount INT,\n"
                        + " currency STRING,\n"
                        + " rowtime TIMESTAMP(3),\n"
                        + " proctime AS PROCTIME(),\n"
                        + " WATERMARK FOR rowtime AS rowtime\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE RatesHistory (\n"
                        + " currency STRING,\n"
                        + " rate INT,\n"
                        + " rowtime TIMESTAMP(3),\n"
                        + " WATERMARK FOR rowtime AS rowtime,\n"
                        + " PRIMARY KEY(currency) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values'\n"
                        + ")");
        TemporalTableFunction ratesHistory =
                tEnv.from("RatesHistory").createTemporalTableFunction("rowtime", "currency");
        tEnv.createTemporarySystemFunction("Rates", ratesHistory);
    }

    @Test
    public void testJoinTemporalFunction() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "INSERT INTO MySink "
                        + "SELECT amount * r.rate "
                        + "FROM Orders AS o,  "
                        + "LATERAL TABLE (Rates(o.rowtime)) AS r "
                        + "WHERE o.currency = r.currency ");
    }

    @Test
    public void testTemporalTableJoin() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "INSERT INTO MySink "
                        + "SELECT amount * r.rate "
                        + "FROM Orders AS o  "
                        + "JOIN RatesHistory  FOR SYSTEM_TIME AS OF o.rowtime AS r "
                        + "ON o.currency = r.currency ");
    }
}
