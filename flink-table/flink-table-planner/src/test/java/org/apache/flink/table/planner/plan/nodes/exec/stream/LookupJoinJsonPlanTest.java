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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.planner.runtime.utils.InMemoryLookupableTableSource;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import scala.collection.JavaConverters;

/** Test json serialization/deserialization for LookupJoin. */
public class LookupJoinJsonPlanTest extends TableTestBase {

    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();

        String srcTableA =
                "CREATE TABLE MyTable (\n"
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
                "CREATE TABLE LookupTable (\n"
                        + "  id int,\n"
                        + "  name varchar,\n"
                        + "  age int \n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableA);
        tEnv.executeSql(srcTableB);
    }

    @Test
    public void testJoinTemporalTable() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a int,\n"
                        + "  b varchar,"
                        + "  c bigint,"
                        + "  proctime timestamp(3),"
                        + "  rowtime timestamp(3),"
                        + "  id int,"
                        + "  name varchar,"
                        + "  age int"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "INSERT INTO MySink SELECT * FROM MyTable AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id");
    }

    @Test
    public void testJoinTemporalTableWithProjectionPushDown() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a int,\n"
                        + "  b varchar,"
                        + "  c bigint,"
                        + "  proctime timestamp(3),"
                        + "  rowtime timestamp(3),"
                        + "  id int"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "INSERT INTO MySink \n"
                        + "SELECT T.*, D.id \n"
                        + "FROM MyTable AS T \n"
                        + "JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D \n"
                        + "ON T.a = D.id\n");
    }

    @Test
    public void testLegacyTableSourceException() {
        expectedException().expectMessage("TemporalTableSourceSpec can not be serialized.");
        TableSchema tableSchema =
                TableSchema.builder()
                        .field("id", Types.INT)
                        .field("name", Types.STRING)
                        .field("age", Types.INT)
                        .build();
        InMemoryLookupableTableSource.createTemporaryTable(
                tEnv,
                false,
                JavaConverters.asScalaIteratorConverter(new ArrayList<Row>().iterator())
                        .asScala()
                        .toList(),
                tableSchema,
                "LookupTable",
                true);
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a int,\n"
                        + "  b varchar,"
                        + "  c bigint,"
                        + "  proctime timestamp(3),"
                        + "  rowtime timestamp(3),"
                        + "  id int,"
                        + "  name varchar,"
                        + "  age int"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "INSERT INTO MySink SELECT * FROM MyTable AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id");
    }
}
