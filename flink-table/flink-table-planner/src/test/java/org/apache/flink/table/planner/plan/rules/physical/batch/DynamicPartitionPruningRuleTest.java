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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/**
 * Test for rules that extend {@link DynamicPartitionPruningRule} to create {@link
 * org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringTableSourceScan}.
 */
public class DynamicPartitionPruningRuleTest extends TableTestBase {
    protected BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());
    TestValuesCatalog catalog = new TestValuesCatalog("testCatalog", "test_database", true);

    @Before
    public void setup() {
        catalog.open();
        util.tableEnv().registerCatalog("testCatalog", catalog);
        util.tableEnv().useCatalog("testCatalog");
        TableConfig tableConfig = util.tableEnv().getConfig();
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED, true);

        // partition fact table.
        String ddl1 =
                "CREATE TABLE test_database.fact_part (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT,\n"
                        + "  fact_date_sk BIGINT\n"
                        + ") PARTITIONED BY (fact_date_sk)\n"
                        + "WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'partition-list' = 'fact_date_sk:1990;fact_date_sk:1991;fact_date_sk:1992',\n"
                        + " 'dynamic-filtering-fields' = 'fact_date_sk;amount',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl1);

        // dim table.
        String ddl2 =
                "CREATE TABLE test_database.dim (\n"
                        + "  id BIGINT,\n"
                        + "  male BOOLEAN,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT,\n"
                        + "  dim_date_sk BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl2);
    }

    @Test
    public void testDimTableFilteringFieldsNotInJoinKey() {
        // fact_part.id not in dynamic-filtering-fields, so dynamic partition pruning will not
        // succeed.
        String query =
                "Select * from dim, fact_part where fact_part.id = dim.id and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDimTableWithoutFilter() {
        // If dim side without filters, dynamic partition pruning will not succeed.
        String query =
                "Select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.price > 100";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDimTableWithUnsuitableFilter() {
        // For filters in dim table side, they need to filter enough partitions. Like NOT NULL will
        // not succeed for dynamic partition pruning.
        String query =
                "Select * from dim join fact_part on fact_part.fact_date_sk = dim.dim_date_sk where dim.id is not null";
        util.verifyRelPlan(query);
    }

    @Test
    public void testFactTableIsNotPartitionTable() {
        // non-partition fact table. Dynamic partition pruning will not succeed if fact side is not
        // partition table.
        String ddl1 =
                "CREATE TABLE test_database.none_part_fact (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT,\n"
                        + "  fact_date_sk BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'dynamic-filtering-fields' = 'fact_date_sk;amount',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl1);

        String query =
                "Select * from dim, none_part_fact where none_part_fact.fact_date_sk = dim.dim_date_sk"
                        + " and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDimTableWithFilterPushDown() {
        // Even though have filter push down, dynamic partition pruning will succeed.
        String query =
                "Select * from fact_part join (Select * from dim) t1"
                        + " on fact_part.fact_date_sk = dim_date_sk where t1.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testJoinKeyIsDynamicFilterFieldNotPartitionKey() {
        // Not only partition key, but also dynamic filtering field in join key will succeed in
        // dynamic partition pruning.
        String query =
                "Select * from dim, fact_part where fact_part.amount = dim.amount and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDynamicFilteringFactInRightRule() throws TableNotExistException {
        // Base rule.
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(1, 1, 1, 1);
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "dim"), tableStatistics, false);
        String query =
                "Select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDynamicFilteringFactInLeftRule() throws TableNotExistException {
        // Base rule.
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(1, 1, 1, 1);
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "dim"), tableStatistics, false);
        String query =
                "Select * from fact_part, dim where fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDynamicFilteringFactInRightWithExchangeRule() {
        // Base rule.
        String query =
                "Select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDynamicFilteringFactInLeftWithExchangeRule() {
        // Base rule.
        String query =
                "Select * from fact_part, dim where fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDynamicFilteringFactInRightWithCalcRule() throws TableNotExistException {
        // Base rule.
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(1, 1, 1, 1);
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "dim"), tableStatistics, false);
        String query =
                "Select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.price > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDynamicFilteringFactInLeftWithCalcRule() throws TableNotExistException {
        // Base rule.
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(1, 1, 1, 1);
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "dim"), tableStatistics, false);
        String query =
                "Select * from fact_part, dim where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.price > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDynamicFilteringFactInRightWithExchangeAndCalcRule() {
        // Base rule.
        String query =
                "Select * from dim, fact_part where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.price > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDynamicFilteringFactInLeftWithExchangeAndCalcRule() {
        // Base rule.
        String query =
                "Select * from fact_part, dim where fact_part.fact_date_sk = dim.dim_date_sk"
                        + " and fact_part.price > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testComplexCalcInFactSide() {
        // Although the partition key is converted, Dynamic Partition pruning can be successfully
        // applied.
        String query =
                "Select * from dim join (select fact_date_sk as fact_date_sk1, price + 1 as price1 from fact_part) t1"
                        + " on t1.fact_date_sk1 = dim_date_sk and t1.price1 > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testPartitionKeysIsComputeColumnsInFactSide() {
        // Dynamic filtering will not succeed for this query.
        String query =
                "Select * from dim join (select fact_date_sk + 1 as fact_date_sk1, price + 1 as price1 from fact_part) t1"
                        + " on t1.fact_date_sk1 = dim_date_sk and t1.price1 > 200 and dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testDynamicFilteringFieldIsComputeColumnsInFactSide()
            throws TableNotExistException {
        CatalogTableStatistics tableStatistics = new CatalogTableStatistics(1, 1, 1, 1);
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "dim"), tableStatistics, false);
        // in this case. amount + 1 as amount is not a partition key, will succeed.
        String query =
                "Select * from dim join (select fact_date_sk, amount + 1 as amount from fact_part) t1 on"
                        + " fact_date_sk = dim_date_sk and t1.amount = dim.amount where dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testLeftOuterJoinWithFactInLeft() {
        // left outer join with fact in left will not succeed. Because if fact in left, filtering
        // condition is useless.
        String query =
                "Select * from fact_part left outer join dim on fact_part.fact_date_sk = dim.dim_date_sk"
                        + " where dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testLeftOutJoinWithFactInRight() {
        // left outer join with fact in right will succeed.
        String query =
                "Select * from dim left outer join fact_part on fact_part.fact_date_sk = dim.dim_date_sk"
                        + " where dim.price < 500";
        util.verifyRelPlan(query);
    }

    @Test
    public void testSemiJoin() {
        // Now dynamic partition pruning support semi join, this query will succeed.
        String query =
                "Select * from fact_part where fact_part.fact_date_sk in"
                        + " (select dim_date_sk from dim where dim.price < 500)";
        util.verifyRelPlan(query);
    }

    @Test
    public void testFullOuterJoin() {
        // Now dynamic partition pruning don't support full outer join.
        String query =
                "Select * from fact_part full outer join"
                        + " (select *  from dim where dim.price < 500) on fact_date_sk = dim_date_sk";
        util.verifyRelPlan(query);
    }

    @Test
    public void testAntiJoin() {
        // Now dynamic partition prune don't support anti join.
        String query =
                "Select * from fact_part where not exists"
                        + " (select dim_date_sk from dim where dim.price < 500)";
        util.verifyRelPlan(query);
    }

    @Test
    public void testMultiJoin() {
        // Another table.
        String ddl =
                "CREATE TABLE test_database.sales (\n"
                        + "  id BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String query =
                "Select * from fact_part, dim, sales where fact_part.id = sales.id and"
                        + " fact_part.fact_date_sk = dim.dim_date_sk and dim.price < 500 and dim.amount > 100";
        util.verifyRelPlan(query);
    }

    @Test
    public void testComplexDimSideWithJoinInDimSide() {
        // Dim side contains join will not succeed in this version, it will improve later.
        String ddl1 =
                "CREATE TABLE test_database.sales (\n"
                        + "  id BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl1);

        String ddl2 =
                "CREATE TABLE test_database.item (\n"
                        + "  id BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl2);

        String query =
                "Select * from fact_part join"
                        + " (select * from dim, sales, item where"
                        + " dim.id = sales.id and sales.id = item.id and dim.price < 500 and sales.price > 300) dimSide"
                        + " on fact_part.fact_date_sk = dimSide.dim_date_sk";
        util.verifyRelPlan(query);
    }

    @Test
    public void testComplexDimSideWithAggInDimSide() {
        // Dim side contains agg will not succeed in this version, it will improve later.
        String ddl =
                "CREATE TABLE test_database.sales (\n"
                        + "  id BIGINT,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String query =
                "Select * from fact_part join"
                        + " (select dim_date_sk, sum(dim.price) from dim where"
                        + "  dim.price < 500 group by dim_date_sk) dimSide"
                        + " on fact_part.fact_date_sk = dimSide.dim_date_sk";
        util.verifyRelPlan(query);
    }
}
