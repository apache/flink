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

package org.apache.flink.table.planner.runtime.batch.sql.agg;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/** Test for local aggregate push down. */
public class LocalAggregatePushDownITCase extends BatchTestBase {

    @Before
    public void before() {
        super.before();
        env().setParallelism(1); // set sink parallelism to 1

        String testDataId = TestValuesTableFactory.registerData(TestData.personData());
        String ddl =
                "CREATE TABLE AggregatableTable (\n"
                        + "  id int,\n"
                        + "  age int,\n"
                        + "  name string,\n"
                        + "  height int,\n"
                        + "  gender string,\n"
                        + "  deposit bigint,\n"
                        + "  points bigint,\n"
                        + "  metadata_1 BIGINT METADATA,\n"
                        + "  metadata_2 STRING METADATA,\n"
                        + "  PRIMARY KEY (`id`) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'data-id' = '"
                        + testDataId
                        + "',\n"
                        + "  'filterable-fields' = 'id;age',\n"
                        + "  'readable-metadata' = 'metadata_1:BIGINT, metadata_2:STRING',\n"
                        + "  'bounded' = 'true'\n"
                        + ")";
        tEnv().executeSql(ddl);

        // partitioned table
        String ddl2 =
                "CREATE TABLE AggregatableTable_Part (\n"
                        + "  id int,\n"
                        + "  age int,\n"
                        + "  name string,\n"
                        + "  height int,\n"
                        + "  gender string,\n"
                        + "  deposit bigint,\n"
                        + "  points bigint,\n"
                        + "  distance BIGINT,\n"
                        + "  type STRING\n"
                        + ") PARTITIONED BY (type)\n"
                        + "WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'data-id' = '"
                        + testDataId
                        + "',\n"
                        + "  'filterable-fields' = 'id;age',\n"
                        + "  'partition-list' = 'type:A;type:B;type:C;type:D',\n"
                        + "  'bounded' = 'true'\n"
                        + ")";
        tEnv().executeSql(ddl2);

        // partitioned table
        String ddl3 =
                "CREATE TABLE AggregatableTable_No_Proj (\n"
                        + "  id int,\n"
                        + "  age int,\n"
                        + "  name string,\n"
                        + "  height int,\n"
                        + "  gender string,\n"
                        + "  deposit bigint,\n"
                        + "  points bigint,\n"
                        + "  distance BIGINT,\n"
                        + "  type STRING\n"
                        + ")\n"
                        + "WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'data-id' = '"
                        + testDataId
                        + "',\n"
                        + "  'filterable-fields' = 'id;age',\n"
                        + "  'enable-projection-push-down' = 'false',\n"
                        + "  'bounded' = 'true'\n"
                        + ")";
        tEnv().executeSql(ddl3);
    }

    @Test
    public void testPushDownLocalHashAggWithGroup() {
        checkResult(
                "SELECT\n"
                        + "  avg(deposit) as avg_dep,\n"
                        + "  sum(deposit),\n"
                        + "  count(1),\n"
                        + "  gender\n"
                        + "FROM\n"
                        + "  AggregatableTable\n"
                        + "GROUP BY gender\n"
                        + "ORDER BY avg_dep",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(Row.of(126, 630, 5, "f"), Row.of(220, 1320, 6, "m"))),
                false);
    }

    @Test
    public void testDisablePushDownLocalAgg() {
        // disable push down local agg
        tEnv().getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED,
                        false);

        checkResult(
                "SELECT\n"
                        + "  avg(deposit) as avg_dep,\n"
                        + "  sum(deposit),\n"
                        + "  count(1),\n"
                        + "  gender\n"
                        + "FROM\n"
                        + "  AggregatableTable\n"
                        + "GROUP BY gender\n"
                        + "ORDER BY avg_dep",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(Row.of(126, 630, 5, "f"), Row.of(220, 1320, 6, "m"))),
                false);
    }

    @Test
    public void testPushDownLocalHashAggWithoutGroup() {
        checkResult(
                "SELECT\n"
                        + "  avg(deposit),\n"
                        + "  sum(deposit),\n"
                        + "  count(*)\n"
                        + "FROM\n"
                        + "  AggregatableTable",
                JavaScalaConversionUtil.toScala(Collections.singletonList(Row.of(177, 1950, 11))),
                false);
    }

    @Test
    public void testPushDownLocalSortAggWithoutSort() {
        // enable sort agg
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg");

        checkResult(
                "SELECT\n"
                        + "  avg(deposit),\n"
                        + "  sum(deposit),\n"
                        + "  count(*)\n"
                        + "FROM\n"
                        + "  AggregatableTable",
                JavaScalaConversionUtil.toScala(Collections.singletonList(Row.of(177, 1950, 11))),
                false);
    }

    @Test
    public void testPushDownLocalSortAggWithSort() {
        // enable sort agg
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg");

        checkResult(
                "SELECT\n"
                        + "  avg(deposit),\n"
                        + "  sum(deposit),\n"
                        + "  count(1),\n"
                        + "  gender,\n"
                        + "  age\n"
                        + "FROM\n"
                        + "  AggregatableTable\n"
                        + "GROUP BY gender, age",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(50, 50, 1, "f", 19),
                                Row.of(200, 200, 1, "f", 20),
                                Row.of(250, 750, 3, "m", 23),
                                Row.of(126, 380, 3, "f", 25),
                                Row.of(300, 300, 1, "m", 27),
                                Row.of(170, 170, 1, "m", 28),
                                Row.of(100, 100, 1, "m", 34))),
                false);
    }

    @Test
    public void testPushDownLocalAggAfterFilterPushDown() {
        checkResult(
                "SELECT\n"
                        + "  avg(deposit),\n"
                        + "  sum(deposit),\n"
                        + "  count(1),\n"
                        + "  gender,\n"
                        + "  age\n"
                        + "FROM\n"
                        + "  AggregatableTable\n"
                        + "WHERE age <= 20\n"
                        + "GROUP BY gender, age",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(Row.of(50, 50, 1, "f", 19), Row.of(200, 200, 1, "f", 20))),
                false);
    }

    @Test
    public void testPushDownLocalAggWithMetadata() {
        checkResult(
                "SELECT\n"
                        + "  sum(metadata_1),\n"
                        + "  metadata_2\n"
                        + "FROM\n"
                        + "  AggregatableTable\n"
                        + "GROUP BY metadata_2",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(156, 'C'),
                                Row.of(183, 'A'),
                                Row.of(51, 'D'),
                                Row.of(70, 'B'))),
                false);
    }

    @Test
    public void testPushDownLocalAggWithPartition() {
        checkResult(
                "SELECT\n"
                        + "  sum(deposit),\n"
                        + "  count(1),\n"
                        + "  type,\n"
                        + "  name\n"
                        + "FROM\n"
                        + "  AggregatableTable_Part\n"
                        + "WHERE type in ('A', 'C')"
                        + "GROUP BY type, name",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(150, 1, "C", "jack"),
                                Row.of(180, 1, "A", "emma"),
                                Row.of(200, 1, "A", "tom"),
                                Row.of(200, 1, "C", "eva"),
                                Row.of(300, 1, "C", "danny"),
                                Row.of(400, 1, "A", "tommas"),
                                Row.of(50, 1, "C", "olivia"))),
                false);
    }

    @Test
    public void testPushDownLocalAggWithoutProjectionPushDown() {
        checkResult(
                "SELECT\n"
                        + "  avg(deposit),\n"
                        + "  sum(deposit),\n"
                        + "  count(1),\n"
                        + "  gender,\n"
                        + "  age\n"
                        + "FROM\n"
                        + "  AggregatableTable_No_Proj\n"
                        + "WHERE age <= 20\n"
                        + "GROUP BY gender, age",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(Row.of(50, 50, 1, "f", 19), Row.of(200, 200, 1, "f", 20))),
                false);
    }

    @Test
    public void testPushDownLocalAggWithoutAuxGrouping() {
        // enable two-phase aggregate, otherwise there is no local aggregate
        tEnv().getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE");

        checkResult(
                "SELECT\n"
                        + "  id,\n"
                        + "  name,\n"
                        + "  count(*)\n"
                        + "FROM\n"
                        + "  AggregatableTable\n"
                        + "WHERE id > 8\n"
                        + "GROUP BY id, name",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of(9, "emma", 1),
                                Row.of(10, "benji", 1),
                                Row.of(11, "eva", 1))),
                false);
    }
}
