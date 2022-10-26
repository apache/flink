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

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/** Plan test for dynamic filtering. */
@RunWith(Parameterized.class)
public class DynamicFilteringTest extends TableTestBase {

    // Notes that the here name is used to load the correct plan.
    @Parameterized.Parameters(name = "mode = {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {BatchShuffleMode.ALL_EXCHANGES_BLOCKING},
                    {BatchShuffleMode.ALL_EXCHANGES_PIPELINED},
                });
    }

    private final BatchShuffleMode batchShuffleMode;

    public DynamicFilteringTest(BatchShuffleMode batchShuffleMode) {
        this.batchShuffleMode = batchShuffleMode;
    }

    private BatchTableTestUtil util;

    @Before
    public void before() {
        util = batchTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 10);

        util.tableEnv()
                .getConfig()
                .getConfiguration()
                .set(ExecutionOptions.BATCH_SHUFFLE_MODE, batchShuffleMode);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE fact1 (\n"
                                + "  a1 bigint,\n"
                                + "  b1 int,\n"
                                + "  c1 varchar,\n"
                                + "  p1 varchar\n"
                                + ") PARTITIONED BY(p1) WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'runtime-source' = 'NewSource',\n"
                                + " 'partition-list' = 'p1:1;p1:2;p1:3',\n"
                                + " 'dynamic-filtering-fields' = 'p1;b1',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE fact2 (\n"
                                + "  a2 bigint,\n"
                                + "  b2 int,\n"
                                + "  c2 varchar,\n"
                                + "  p2 varchar\n"
                                + ") PARTITIONED BY(p2) WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'runtime-source' = 'NewSource',\n"
                                + " 'partition-list' = 'p1:1;p1:2;p1:3',\n"
                                + " 'dynamic-filtering-fields' = 'p2',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE dim (\n"
                                + "  x BIGINT,\n"
                                + "  y BIGINT,\n"
                                + "  z VARCHAR,\n"
                                + "  p VARCHAR\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
    }

    @Test
    public void testLegacySource() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE legacy_source (\n"
                                + "  a1 BIGINT,\n"
                                + "  b1 BIGINT,\n"
                                + "  c1 VARCHAR,\n"
                                + "  d1 BIGINT,\n"
                                + "  p1 VARCHAR\n"
                                + ") PARTITIONED BY (p1)\n"
                                + "  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'runtime-source' = 'SourceFunction',\n"
                                + " 'partition-list' = 'p1:1;p1:2;p1:3',\n"
                                + " 'dynamic-filtering-fields' = 'p1',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.verifyExplain(
                "SELECT * FROM legacy_source, dim WHERE p1 = p AND x > 10",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.JSON_EXECUTION_PLAN)));
    }

    @Test
    public void testSimpleDynamicFiltering() {
        // the execution plan contains 'Placeholder-Filter' operator
        util.verifyExplain(
                "SELECT * FROM fact1, dim WHERE p1 = p AND x > 10",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.JSON_EXECUTION_PLAN)));
    }

    @Test
    public void testDynamicFilteringWithMultipleInput() {
        // the execution plan does not contain 'Placeholder-Filter' operator
        util.verifyExplain(
                "SELECT * FROM fact1, dim, fact2 WHERE p1 = p and p1 = p2 AND x > 10",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.JSON_EXECUTION_PLAN)));
    }

    @Test
    public void testDuplicateFactTables() {
        // the fact tables can not be reused
        util.verifyExplain(
                "SELECT * FROM (SELECT * FROM fact1, dim WHERE p1 = p AND x > 10) t1 JOIN fact1 t2 ON t1.y = t2.b1",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.JSON_EXECUTION_PLAN)));
    }

    @Test
    public void testReuseDimSide() {
        // dynamic filtering collector will be reused for both fact tables
        util.verifyExplain(
                "SELECT * FROM fact1, dim WHERE p1 = p AND x > 10 "
                        + "UNION ALL "
                        + "SELECT * FROM fact2, dim WHERE p2 = p AND x > 10",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.JSON_EXECUTION_PLAN)));
    }

    @Test
    public void testDynamicFilteringWithStaticPartitionPruning() {
        util.verifyExplain(
                "SELECT * FROM fact1, dim WHERE p1 = p AND x > 10 and p1 > 1",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(ExplainDetail.JSON_EXECUTION_PLAN)));
    }
}
