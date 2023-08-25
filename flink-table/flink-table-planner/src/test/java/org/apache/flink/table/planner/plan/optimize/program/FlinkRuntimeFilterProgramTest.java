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
 * withOUT WARRANTIES OR ConDITIonS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.optimize.program;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

/** Test for {@link FlinkRuntimeFilterProgram}. */
public class FlinkRuntimeFilterProgramTest extends TableTestBase {
    // 128L * 1024L * 48 = 6MB
    public static final long SUITABLE_DIM_ROW_COUNT = 128L * 1024L;
    // 1024L * 1024L * 1024L * 60 = 60GB
    public static final long SUITABLE_FACT_ROW_COUNT = 1024L * 1024L * 1024L;

    private final BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());
    private final TestValuesCatalog catalog =
            new TestValuesCatalog("testCatalog", "test_database", true);

    @Before
    public void setup() {
        catalog.open();
        util.tableEnv().registerCatalog("testCatalog", catalog);
        util.tableEnv().useCatalog("testCatalog");
        TableConfig tableConfig = util.tableEnv().getConfig();
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_RUNTIME_FILTER_ENABLED, true);
        tableConfig.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_RUNTIME_FILTER_MAX_BUILD_DATA_SIZE,
                MemorySize.parse("10m"));

        // row avg size is 48
        String dimDdl =
                "create table dim (\n"
                        + "  id BIGINT,\n"
                        + "  male BOOLEAN,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT,\n"
                        + "  dim_date_sk BIGINT\n"
                        + ")  with (\n"
                        + " 'connector' = 'values',\n"
                        + " 'runtime-source' = 'NewSource',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(dimDdl);

        // row avg size is 60
        String factDdl =
                "create table fact (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT,\n"
                        + "  fact_date_sk BIGINT\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'runtime-source' = 'NewSource',\n"
                        + "  'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(factDdl);
    }

    @Test
    public void testSimpleInnerJoin() throws Exception {
        // runtime filter will succeed
        setupSuitableTableStatistics();
        String query = "select * from fact, dim where fact.amount = dim.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testSemiJoin() throws Exception {
        // runtime filter will succeed
        setupSuitableTableStatistics();
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1L);
        String query =
                "select * from fact where fact.fact_date_sk in (select dim_date_sk from dim where dim.price < 500)";
        util.verifyPlan(query);
    }

    @Test
    public void testLeftOuterJoinWithLeftBuild() throws Exception {
        // runtime filter will succeed
        setupSuitableTableStatistics();
        String query =
                "select * from dim left outer join fact on fact.amount = dim.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testLeftOuterJoinWithRightBuild() throws Exception {
        // runtime filter will not succeed
        setupSuitableTableStatistics();
        String query =
                "select * from fact left outer join dim on fact.amount = dim.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testFullOuterJoin() throws Exception {
        // runtime filter will not succeed
        setupSuitableTableStatistics();
        String query =
                "select * from fact full outer join (select * from dim where dim.price < 500) on fact_date_sk = dim_date_sk";
        util.verifyPlan(query);
    }

    @Test
    public void testAntiJoin() throws Exception {
        // runtime filter will not succeed
        setupSuitableTableStatistics();
        String query =
                "select * from fact where fact.fact_date_sk not in (select dim_date_sk from dim where dim.price < 500)";
        util.verifyPlan(query);
    }

    @Test
    public void testNestedLoopJoin() throws Exception {
        // runtime filter will not succeed
        setupTableRowCount("dim", 1L);
        setupTableRowCount("fact", SUITABLE_FACT_ROW_COUNT);
        String query = "select * from fact, dim where fact.amount = dim.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testProbeSideIsTooSmall() throws Exception {
        // runtime filter will not succeed
        setupTableRowCount("dim", SUITABLE_DIM_ROW_COUNT);
        // fact is 7.5 GB < 10 GB
        setupTableRowCount("fact", 128L * 1024L * 1024L);
        String query = "select * from fact, dim where fact.amount = dim.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testBuildSideIsTooLarge() throws Exception {
        // runtime filter will not succeed
        // dim is 48 MB > 6MB
        setupTableRowCount("dim", 1024L * 1024L);
        setupTableRowCount("fact", SUITABLE_FACT_ROW_COUNT);
        String query = "select * from fact, dim where fact.amount = dim.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testFilterRatioIsTooSmall() throws Exception {
        // runtime filter will not succeed
        setupSuitableTableStatistics();
        setupTableColumnNdv("dim", "amount", 768L);
        setupTableColumnNdv("fact", "amount", 1024L);
        String query = "select * from fact, dim where fact.amount = dim.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testBuildSideIsJoinWithoutExchange() throws Exception {
        // runtime filter will succeed
        setupSuitableTableStatistics();
        util.tableEnv()
                .executeSql(
                        "create table fact2 (\n"
                                + "  id BIGINT,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        setupTableRowCount("fact2", SUITABLE_FACT_ROW_COUNT);

        String query =
                "select * from dim, fact, fact2 where fact.amount = fact2.amount and"
                        + " fact.amount = dim.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testBuildSideIsJoinWithTwoAggInputs() throws Exception {
        // runtime filter will succeed
        setupSuitableTableStatistics();
        util.tableEnv()
                .executeSql(
                        "create table fact2 (\n"
                                + "  id BIGINT,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        setupTableRowCount("fact2", SUITABLE_FACT_ROW_COUNT);
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1L);
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "ONE_PHASE");

        String query =
                "select * from fact join (select * from "
                        + "(select dim_date_sk, sum(dim.price) from dim group by dim_date_sk) agg1 join "
                        + "(select dim_date_sk, sum(dim.amount) from dim group by dim_date_sk) agg2 "
                        + "on agg1.dim_date_sk = agg2.dim_date_sk) as dimSide "
                        + "on fact.fact_date_sk = dimSide.dim_date_sk";
        util.verifyPlan(query);
    }

    @Test
    public void testBuildSideIsLeftJoinWithoutExchange() throws Exception {
        // runtime filter will not succeed, because the original build side is left join(without
        // exchange), so we can only push builder to it's left input, but the left input is too
        // large to as builder.
        setupSuitableTableStatistics();
        util.tableEnv()
                .executeSql(
                        "create table fact2 (\n"
                                + "  id BIGINT,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  fact_date_sk BIGINT\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        setupTableRowCount("fact2", SUITABLE_FACT_ROW_COUNT);

        String query =
                "select * from fact2 join (select * from fact left join dim on dim.amount = fact.amount"
                        + " and dim.price < 500) as dimSide on fact2.amount = dimSide.amount";
        util.verifyPlan(query);
    }

    @Test
    public void testBuildSideIsAggWithoutExchange() throws Exception {
        // runtime filter will succeed
        // The following two config are used to let the build side is a direct Agg (without
        // Exchange)
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1L);
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "ONE_PHASE");

        setupSuitableTableStatistics();
        String query =
                "select * from fact join"
                        + " (select dim_date_sk, sum(dim.price) from dim where"
                        + "  dim.price < 500 group by dim_date_sk) dimSide"
                        + " on fact.fact_date_sk = dimSide.dim_date_sk";
        util.verifyPlan(query);
    }

    @Test
    public void testBuildSideIsCalcWithoutExchange() throws Exception {
        // runtime filter will succeed
        // The following two config are used to let the build side is a direct Calc (without
        // Exchange)
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1L);
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "ONE_PHASE");
        setupSuitableTableStatistics();
        String query =
                "select * from fact join"
                        + " (select dim_date_sk, sum(dim.price) + 1 as sum_price from dim where"
                        + "  dim.price < 500 group by dim_date_sk) dimSide"
                        + " on fact.fact_date_sk = dimSide.dim_date_sk";
        util.verifyPlan(query);
    }

    @Test
    public void testCannotInjectMoreThanOneRuntimeFilterInSamePlace() throws Exception {
        setupSuitableTableStatistics();
        util.tableEnv()
                .executeSql(
                        "create table dim2 (\n"
                                + "  id BIGINT,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        setupTableRowCount("dim2", SUITABLE_DIM_ROW_COUNT);

        String query =
                "select * from fact, dim, dim2 where fact.amount = dim.amount and"
                        + " fact.amount = dim2.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testPushDownProbeSideWithCalc() throws Exception {
        setupSuitableTableStatistics();
        String query =
                "select * from dim, fact where dim.amount = fact.amount and dim.price < 500 and fact.price > 600";
        util.verifyPlan(query);
    }

    @Test
    public void testCannotPushDownProbeSideWithCalc() throws Exception {
        setupSuitableTableStatistics();
        String query =
                "select * from dim inner join (select fact_date_sk, RAND(10) as random from fact) "
                        + "as factSide on dim.amount = factSide.random and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testPushDownProbeSideToAllInputsOfJoin() throws Exception {
        setupSuitableTableStatistics();
        util.tableEnv()
                .executeSql(
                        "create table fact2 (\n"
                                + "  id BIGINT,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        setupTableRowCount("fact2", SUITABLE_FACT_ROW_COUNT);

        String query =
                "select * from fact, fact2, dim where fact.amount = fact2.amount and"
                        + " fact.amount = dim.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testPushDownProbeSideToOneInputOfJoin() throws Exception {
        setupSuitableTableStatistics();
        util.tableEnv()
                .executeSql(
                        "create table fact2 (\n"
                                + "  id BIGINT,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        setupTableRowCount("fact2", SUITABLE_FACT_ROW_COUNT);

        String query =
                "select * from fact, fact2, dim where fact.price = fact2.price and"
                        + " fact.amount = dim.amount and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testCannotPushDownProbeSideWithJoin() throws Exception {
        setupSuitableTableStatistics();
        util.tableEnv()
                .executeSql(
                        "create table fact2 (\n"
                                + "  id2 BIGINT,\n"
                                + "  amount2 BIGINT,\n"
                                + "  price2 BIGINT\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + "  'runtime-source' = 'NewSource',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        setupTableRowCount("fact2", SUITABLE_FACT_ROW_COUNT);

        String query =
                "select * from (select * from fact inner join fact2 on fact.id = fact2.id2) as factSide "
                        + "inner join dim on factSide.amount = dim.amount and factSide.price2 = dim.price and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testPushDownProbeSideWithAgg() throws Exception {
        setupTableRowCount("dim", SUITABLE_DIM_ROW_COUNT);
        setupTableRowCount("fact", 1024L * SUITABLE_FACT_ROW_COUNT);
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "ONE_PHASE");
        String query =
                "select * from dim join"
                        + " (select id, fact_date_sk, sum(fact.price) from fact group by (id, fact_date_sk)) factSide"
                        + " on dim.dim_date_sk = factSide.fact_date_sk and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testCannotPushDownProbeSideWithAgg() throws Exception {
        setupTableRowCount("dim", SUITABLE_DIM_ROW_COUNT);
        setupTableRowCount("fact", 1024L * SUITABLE_FACT_ROW_COUNT);
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "ONE_PHASE");
        String query =
                "select * from dim join"
                        + " (select id, fact_date_sk, sum(fact.price) as sum_price from fact group by (id, fact_date_sk)) factSide"
                        + " on dim.price = factSide.sum_price and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testPushDownProbeSideWithUnion() throws Exception {
        // probe side will be pushed down to union.
        setupSuitableTableStatistics();
        String query =
                "Select * from (select id, fact_date_sk, amount as amount1 from fact where price < 500 union all "
                        + "select id, fact_date_sk, amount from fact where price > 600) fact2, dim"
                        + " where fact2.fact_date_sk = dim.dim_date_sk"
                        + " and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testDoesNotApplyRuntimeFilterAndDPPOnSameKey() throws Exception {
        // runtime filter will not success, because already applied DPP on the key
        setupTableRowCount("dim", SUITABLE_DIM_ROW_COUNT);
        createPartitionedFactTable(SUITABLE_FACT_ROW_COUNT);
        String query =
                "select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500";
        util.verifyPlan(query);
    }

    @Test
    public void testProbeSideIsTableSourceWithoutExchange() throws Exception {
        // runtime filter will not succeed, because probe side is a direct table source
        setupSuitableTableStatistics();
        String query = "select * from fact, dim where fact.amount = dim.amount and dim.price = 500";
        util.verifyPlan(query);
    }

    private void createPartitionedFactTable(long rowCount) throws Exception {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE fact_part (\n"
                                + "  id BIGINT,\n"
                                + "  name STRING,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  fact_date_sk BIGINT\n"
                                + ") PARTITIONED BY (fact_date_sk)\n"
                                + "WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'runtime-source' = 'NewSource',\n"
                                + " 'partition-list' = 'fact_date_sk:1990;fact_date_sk:1991;fact_date_sk:1992',\n"
                                + " 'dynamic-filtering-fields' = 'fact_date_sk;amount',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        final CatalogPartitionSpec partSpec =
                new CatalogPartitionSpec(Collections.singletonMap("fact_date_sk", "666"));
        catalog.createPartition(
                new ObjectPath(util.getTableEnv().getCurrentDatabase(), "fact_part"),
                partSpec,
                new CatalogPartitionImpl(new HashMap<>(), ""),
                true);
        catalog.alterPartitionStatistics(
                new ObjectPath(util.getTableEnv().getCurrentDatabase(), "fact_part"),
                partSpec,
                new CatalogTableStatistics(rowCount, 10, 1000L, 2000L),
                true);
    }

    private void setupSuitableTableStatistics() throws Exception {
        setupTableRowCount("dim", SUITABLE_DIM_ROW_COUNT);
        setupTableRowCount("fact", SUITABLE_FACT_ROW_COUNT);
    }

    private void setupTableRowCount(String tableName, long rowCount) throws Exception {
        catalog.alterTableStatistics(
                new ObjectPath(util.getTableEnv().getCurrentDatabase(), tableName),
                new CatalogTableStatistics(rowCount, 1, 1, 1),
                false);
    }

    private void setupTableColumnNdv(String tableName, String columnName, long ndv)
            throws Exception {
        CatalogColumnStatisticsDataLong longColStats =
                new CatalogColumnStatisticsDataLong(-123L, 763322L, ndv, 79L);
        catalog.alterTableColumnStatistics(
                new ObjectPath(util.getTableEnv().getCurrentDatabase(), tableName),
                new CatalogColumnStatistics(Collections.singletonMap(columnName, longColStats)),
                false);
    }
}
