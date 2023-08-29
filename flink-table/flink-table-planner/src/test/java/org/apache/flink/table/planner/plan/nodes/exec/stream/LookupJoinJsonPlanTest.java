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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.runtime.utils.InMemoryLookupableTableSource;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import scala.collection.JavaConverters;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        String sinkTable1 =
                "CREATE TABLE Sink1 (\n"
                        + "  a int,\n"
                        + "  name varchar,"
                        + "  age int"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false')";
        String sinkTable2 =
                "CREATE TABLE MySink1 (\n"
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
        tEnv.executeSql(srcTableA);
        tEnv.executeSql(srcTableB);
        tEnv.executeSql(sinkTable1);
        tEnv.executeSql(sinkTable2);
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
        assertThatThrownBy(
                        () ->
                                util.verifyJsonPlan(
                                        "INSERT INTO MySink SELECT * FROM MyTable AS T JOIN LookupTable "
                                                + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "TemporalTableSourceSpec can not be serialized."));
    }

    @Test
    public void testAggAndLeftJoinWithTryResolveMode() {
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
                        OptimizerConfigOptions.NonDeterministicUpdateStrategy.TRY_RESOLVE);

        util.verifyJsonPlan(
                "INSERT INTO Sink1 "
                        + "SELECT T.a, D.name, D.age "
                        + "FROM (SELECT max(a) a, count(c) c, PROCTIME() proctime FROM MyTable GROUP BY b) T "
                        + "LEFT JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id");
    }

    @Test
    public void testJoinTemporalTableWithAsyncHint() {
        // LookupTable has sync func only, just verify the hint has take effect
        util.verifyJsonPlan(
                "INSERT INTO MySink1 SELECT "
                        + "/*+ LOOKUP('table'='D', 'async'='true', 'output-mode'='allow_unordered') */ * "
                        + "FROM MyTable AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id");
    }

    @Test
    public void testJoinTemporalTableWithAsyncHint2() {
        // LookupTable has sync func only, just verify the hint has take effect
        util.verifyJsonPlan(
                "INSERT INTO MySink1 SELECT "
                        + "/*+ LOOKUP('table'='D', 'async'='true', 'timeout'='600s', 'capacity'='1000') */ * "
                        + "FROM MyTable AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id");
    }

    @Test
    public void testJoinTemporalTableWithRetryHint() {
        util.verifyJsonPlan(
                "INSERT INTO MySink1 SELECT "
                        + "/*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */ * "
                        + "FROM MyTable AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id");
    }

    @Test
    public void testJoinTemporalTableWithAsyncRetryHint() {
        // LookupTable has sync func only, just verify the hint has take effect
        util.verifyJsonPlan(
                "INSERT INTO MySink1 SELECT "
                        + "/*+ LOOKUP('table'='D', 'async'='true', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */ * "
                        + "FROM MyTable AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id");
    }

    @Test
    public void testJoinTemporalTableWithAsyncRetryHint2() {
        // LookupTable has sync func only, just verify the hint has take effect
        util.verifyJsonPlan(
                "INSERT INTO MySink1 SELECT "
                        + "/*+ LOOKUP('table'='D', 'async'='true', 'timeout'='600s', 'capacity'='1000', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s', 'max-attempts'='3') */ * "
                        + "FROM MyTable AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id");
    }

    @Test
    public void testLeftJoinTemporalTableWithPreFilter() {
        util.verifyJsonPlan(
                "INSERT INTO MySink1 SELECT * "
                        + "FROM MyTable AS T LEFT JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b > 'abc'");
    }

    @Test
    public void testLeftJoinTemporalTableWithPostFilter() {
        util.verifyJsonPlan(
                "INSERT INTO MySink1 SELECT * "
                        + "FROM MyTable AS T LEFT JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id "
                        + "AND CHAR_LENGTH(D.name) > CHAR_LENGTH(T.b)");
    }

    @Test
    public void testLeftJoinTemporalTableWithMultiJoinConditions() {
        util.verifyJsonPlan(
                "INSERT INTO MySink1 SELECT * "
                        + "FROM MyTable AS T LEFT JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D "
                        + "ON T.a = D.id AND T.b > 'abc' AND T.b <> D.name AND T.c = 100");
    }
}
