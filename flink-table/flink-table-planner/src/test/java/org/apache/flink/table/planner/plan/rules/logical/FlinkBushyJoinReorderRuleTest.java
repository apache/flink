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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/** Plan test for {@link FlinkBushyJoinReorderRule}. */
public class FlinkBushyJoinReorderRuleTest extends TableTestBase {
    private final BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());
    private final TestValuesCatalog catalog =
            new TestValuesCatalog("test_catalog", "test_database", true);

    @Before
    public void setup() throws TableNotExistException {
        catalog.open();
        util.tableEnv().registerCatalog("test_catalog", catalog);
        util.tableEnv().useCatalog("test_catalog");
        TableConfig tableConfig = util.tableEnv().getConfig();
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD, 5);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T1 (a1 int, b1 bigint, c1 int, d1 string, e1 bigint) "
                                + "WITH ('connector' = 'values', 'bounded' = 'true')");
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "T1"),
                new CatalogTableStatistics(100000, 1, 1, 1),
                false);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T2 (a2 int, b2 bigint, c2 int, d2 string, e2 bigint) "
                                + "WITH ('connector' = 'values', 'bounded' = 'true')");
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "T2"),
                new CatalogTableStatistics(10000, 1, 1, 1),
                false);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T3 (a3 int, b3 bigint, c3 string) "
                                + "WITH ('connector' = 'values', 'bounded' = 'true')");
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "T3"),
                new CatalogTableStatistics(1000, 1, 1, 1),
                false);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T4 (a4 int, b4 bigint, c4 int, d4 string, e4 bigint) "
                                + "WITH ('connector' = 'values', 'bounded' = 'true')");
        catalog.alterTableStatistics(
                new ObjectPath("test_database", "T4"),
                new CatalogTableStatistics(100, 1, 1, 1),
                false);
    }

    @Test
    public void testBushyJoinReorderWithFullOuterJoin() {
        // full join can not reorder, all join keys will generate null.
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "FULL OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "FULL OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "FULL OUTER JOIN T1 ON T4.b4 = T1.b1";
        util.verifyRelPlan(query);
    }

    @Test
    public void testBushyJoinReorderWithInnerAndFullOuterJoin() {
        // This case can not do bushy join reorder.
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "FULL OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1";
        util.verifyRelPlan(query);
    }

    @Test
    public void testBushyJoinReorderWithInnerJoin() {
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1 "
                        + "WHERE T1.a1 > 0 AND T3.a3 > 0";
        util.verifyRelPlan(query);
    }

    @Test
    public void testBushyJoinReorderWithLeftOuterJoin() {
        // can reorder, all join keys will not generate null.
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "LEFT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "LEFT OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "LEFT OUTER JOIN T1 ON T4.b4 = T1.b1 WHERE T4.a4 < 3";
        util.verifyRelPlan(query);
    }

    @Test
    public void testBushyJoinReorderWithInnerAndLeftOuterJoin() {
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "LEFT OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1 "
                        + "WHERE T4.a4 < 3";
        util.verifyRelPlan(query);
    }

    @Test
    public void testBushyJoinReorderWithRightOuterJoin() {
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "RIGHT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T3.b3 = T2.b2 "
                        + "JOIN T1 ON T2.b2 = T1.b1 WHERE T2.a2 <= 2";
        util.verifyRelPlan(query);
    }

    @Test
    public void testBushyJoinReorderWithTrueCondition() {
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4, T3, T2, T1 "
                        + "WHERE T4.a4 <= 1 AND T3.a3 <= 1 AND T2.a2 <= 1 AND T1.a1 <= 1";
        util.verifyRelPlan(query);
    }

    @Test
    public void testBushyJoinReorderWithInnerJoinAndTrueCondition() {
        String query =
                "SELECT tab1.d4, tab1.c3, T2.d2, T1.d1 FROM T1, "
                        + "(SELECT * FROM T3 JOIN T4 ON T4.b4 = T3.b3) tab1, T2 "
                        + "WHERE tab1.a4 <= 1 AND tab1.a3 <= 1 AND T2.a2 <= 1 AND T1.a1 <= 1";
        util.verifyRelPlan(query);
    }

    @Test
    public void testBushyJoinReorderWithMixedJoinTypeAndCondition() {
        // This case include inner join, left outer join and true condition.
        String query =
                "SELECT tab2.d4, tab2.c3, tab2.d2, T1.d1 FROM T1, (SELECT * FROM T4 "
                        + "LEFT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T4.b4 = T2.b2) tab2 "
                        + "WHERE tab2.a2 <= 2";
        util.verifyRelPlan(query);
    }

    @Test
    public void testBushyJoinReorderWithBushyTree() throws TableNotExistException {
        CatalogColumnStatisticsDataLong longColStats =
                new CatalogColumnStatisticsDataLong(100L, 100L, 50L, 1000L);
        Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>(1);
        colStatsMap.put("b1", longColStats);
        catalog.alterTableColumnStatistics(
                new ObjectPath("test_database", "T1"),
                new CatalogColumnStatistics(colStatsMap),
                false);

        longColStats = new CatalogColumnStatisticsDataLong(100L, 100L, 500000L, 1000L);
        colStatsMap = new HashMap<>(1);
        colStatsMap.put("b2", longColStats);
        catalog.alterTableColumnStatistics(
                new ObjectPath("test_database", "T2"),
                new CatalogColumnStatistics(colStatsMap),
                false);

        longColStats = new CatalogColumnStatisticsDataLong(100L, 100L, 50L, 1000L);
        colStatsMap = new HashMap<>(1);
        colStatsMap.put("b3", longColStats);
        catalog.alterTableColumnStatistics(
                new ObjectPath("test_database", "T3"),
                new CatalogColumnStatistics(colStatsMap),
                false);

        longColStats = new CatalogColumnStatisticsDataLong(100L, 100L, 500000L, 1000L);
        colStatsMap = new HashMap<>(1);
        colStatsMap.put("b4", longColStats);
        catalog.alterTableColumnStatistics(
                new ObjectPath("test_database", "T4"),
                new CatalogColumnStatistics(colStatsMap),
                false);

        String query =
                "SELECT tab2.d4, tab2.c3, tab1.d2, tab1.d1 FROM "
                        + "(SELECT * FROM T1 JOIN T2 ON T1.b1 = T2.b2) tab1 "
                        + "JOIN (SELECT * FROM T3 JOIN T4 ON T3.b3 = T4.b4) tab2 "
                        + "ON tab1.b2 = tab2.b4";
        util.verifyRelPlan(query);
    }
}
