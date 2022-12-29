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

package org.apache.flink.table.planner.runtime.batch.sql.join;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import scala.Enumeration;

/**
 * ITCase for JoinReorder in batch mode. This class will test {@link
 * org.apache.flink.table.planner.plan.rules.logical.FlinkBushyJoinReorderRule} and {@link
 * org.apache.calcite.rel.rules.LoptOptimizeJoinRule} together by changing the factor
 * isBushyJoinReorder.
 */
@RunWith(Parameterized.class)
public class JoinReorderITCase extends BatchTestBase {

    private TableEnvironment tEnv;
    private Catalog catalog;

    @Parameterized.Parameter(value = 0)
    public Enumeration.Value expectedJoinType;

    @Parameterized.Parameter(value = 1)
    public boolean isBushyJoinReorder;

    @Parameterized.Parameters(name = "expectedJoinType={0}, isBushyJoinReorder={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {JoinType.BroadcastHashJoin(), false},
                    {JoinType.BroadcastHashJoin(), true},
                    {JoinType.HashJoin(), false},
                    {JoinType.HashJoin(), true},
                    {JoinType.SortMergeJoin(), false},
                    {JoinType.SortMergeJoin(), true},
                    {JoinType.NestedLoopJoin(), false},
                    {JoinType.NestedLoopJoin(), true}
                });
    }

    @Before
    public void before() throws Exception {
        super.before();
        tEnv = tEnv();
        catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();

        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);

        if (!isBushyJoinReorder) {
            tEnv.getConfig()
                    .getConfiguration()
                    .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD, 3);
        }

        JoinITCaseHelper.disableOtherJoinOpForJoin(tEnv(), expectedJoinType);

        // Test data
        String dataId2 = TestValuesTableFactory.registerData(TestData.data2());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T1 (\n"
                                + "  a1 INT,\n"
                                + "  b1 BIGINT,\n"
                                + "  c1 INT,\n"
                                + "  d1 STRING,\n"
                                + "  e1 BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId2));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T1"),
                new CatalogTableStatistics(100000, 1, 1, 1),
                false);

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T2 (\n"
                                + "  a2 INT,\n"
                                + "  b2 BIGINT,\n"
                                + "  c2 INT,\n"
                                + "  d2 STRING,\n"
                                + "  e2 BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId2));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T2"),
                new CatalogTableStatistics(10000, 1, 1, 1),
                false);

        String dataId3 = TestValuesTableFactory.registerData(TestData.smallData3());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T3 (\n"
                                + "  a3 INT,\n"
                                + "  b3 BIGINT,\n"
                                + "  c3 STRING\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId3));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T3"),
                new CatalogTableStatistics(1000, 1, 1, 1),
                false);

        String dataId5 = TestValuesTableFactory.registerData(TestData.data5());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T4 (\n"
                                + "  a4 INT,\n"
                                + "  b4 BIGINT,\n"
                                + "  c4 INT,\n"
                                + "  d4 STRING,\n"
                                + "  e4 BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId5));
        catalog.alterTableStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T4"),
                new CatalogTableStatistics(100, 1, 1, 1),
                false);
    }

    @Test
    public void testJoinReorderWithFullOuterJoin() {
        if (expectedJoinType == JoinType.BroadcastHashJoin()) {
            return;
        }
        checkResult(
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "FULL OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "FULL OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "FULL OUTER JOIN T1 ON T2.b2 = T1.b1",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("ABC", null, "ABC", "ABC"),
                                Row.of("BCD", null, "BCD", "BCD"),
                                Row.of("CDE", null, "CDE", "CDE"),
                                Row.of("DEF", null, "DEF", "DEF"),
                                Row.of("EFG", null, "EFG", "EFG"),
                                Row.of("FGH", null, "FGH", "FGH"),
                                Row.of("GHI", null, "GHI", "GHI"),
                                Row.of("HIJ", null, "HIJ", "HIJ"),
                                Row.of(
                                        "Hallo Welt wie gehts?",
                                        null,
                                        "Hallo Welt wie gehts?",
                                        "Hallo Welt wie gehts?"),
                                Row.of("Hallo Welt wie", null, "Hallo Welt wie", "Hallo Welt wie"),
                                Row.of("Hallo Welt", "Hello world", "Hallo Welt", "Hallo Welt"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt", "Hallo Welt"),
                                Row.of("Hallo", "Hi", "Hallo", "Hallo"),
                                Row.of("IJK", null, "IJK", "IJK"),
                                Row.of("JKL", null, "JKL", "JKL"),
                                Row.of("KLM", null, "KLM", "KLM"))),
                false);
    }

    @Test
    public void testJoinReorderWithInnerAndFullOuterJoin() {
        if (expectedJoinType == JoinType.BroadcastHashJoin()) {
            return;
        }
        checkResult(
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "FULL OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("Hallo Welt", "Hello world", "Hallo Welt", "Hallo Welt"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt", "Hallo Welt"),
                                Row.of("Hallo", "Hi", "Hallo", "Hallo"))),
                false);
    }

    @Test
    public void testJoinReorderWithInnerJoin() {
        checkResult(
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1 "
                        + "WHERE T1.a1 > 0 AND T3.a3 > 0",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("Hallo", "Hi", "Hallo", "Hallo"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt", "Hallo Welt"),
                                Row.of("Hallo Welt", "Hello world", "Hallo Welt", "Hallo Welt"))),
                false);
    }

    @Test
    public void testJoinReorderWithLeftOuterJoin() {
        // can reorder, all join keys will not generate null.
        checkResult(
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "LEFT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "LEFT OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "LEFT OUTER JOIN T1 ON T4.b4 = T1.b1 "
                        + "WHERE T4.a4 < 3",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("Hallo", "Hi", "Hallo", "Hallo"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt", "Hallo Welt"),
                                Row.of("Hallo Welt", "Hello world", "Hallo Welt", "Hallo Welt"),
                                Row.of(
                                        "Hallo Welt wie",
                                        null,
                                        "Hallo Welt wie",
                                        "Hallo Welt wie"))),
                false);
    }

    @Test
    public void testJoinReorderWithInnerAndLeftOuterJoin() {
        checkResult(
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "LEFT OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1 "
                        + "WHERE T4.a4 < 3",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("Hallo", "Hi", "Hallo", "Hallo"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt", "Hallo Welt"),
                                Row.of("Hallo Welt", "Hello world", "Hallo Welt", "Hallo Welt"))),
                false);
    }

    @Test
    public void testJoinReorderWithRightOuterJoin() {
        checkResult(
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "RIGHT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T3.b3 = T2.b2 "
                        + "JOIN T1 ON T2.b2 = T1.b1 "
                        + "WHERE T2.a2 <= 2",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("Hallo", "Hi", "Hallo", "Hallo"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt", "Hallo Welt"),
                                Row.of("Hallo Welt", "Hello world", "Hallo Welt", "Hallo Welt"))),
                false);
    }

    @Test
    public void testJoinReorderWithTrueCondition() {
        if (expectedJoinType != JoinType.NestedLoopJoin()) {
            return;
        }
        checkResult(
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4, T3, T2, T1 "
                        + "WHERE T4.a4 <= 1 AND T3.a3 <= 1 AND T2.a2 <= 1 AND T1.a1 <= 1",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(Row.of("Hallo", "Hi", "Hallo", "Hallo"))),
                false);
    }

    @Test
    public void testJoinReorderWithInnerJoinAndTrueCondition() {
        if (expectedJoinType != JoinType.NestedLoopJoin()) {
            return;
        }
        checkResult(
                "SELECT tab1.d4, tab1.c3, T2.d2, T1.d1 FROM T1, "
                        + "(SELECT * FROM T3 JOIN T4 ON T4.b4 = T3.b3) tab1, T2 "
                        + "WHERE tab1.a4 <= 1 AND tab1.a3 <= 1 AND T2.a2 <= 1 AND T1.a1 <= 1",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(Row.of("Hallo", "Hi", "Hallo", "Hallo"))),
                false);
    }

    @Test
    public void testJoinReorderWithMixedJoinTypeAndCondition() {
        if (expectedJoinType != JoinType.NestedLoopJoin()) {
            return;
        }
        checkResult(
                "SELECT tab2.d4, tab2.c3, tab2.d2, T1.d1 FROM T1, (SELECT * FROM T4 "
                        + "LEFT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T4.b4 = T2.b2) tab2 "
                        + "WHERE tab2.a4 <= 1 AND tab2.a3 <= 1 AND tab2.a2 <= 1 AND T1.a1 <= 1",
                JavaScalaConversionUtil.toScala(
                        Collections.singletonList(Row.of("Hallo", "Hi", "Hallo", "Hallo"))),
                false);
    }

    @Test
    public void testBusyTreeJoinReorder() throws TableNotExistException, TablePartitionedException {
        CatalogColumnStatisticsDataLong longColStats =
                new CatalogColumnStatisticsDataLong(100L, 100L, 50L, 1000L);
        Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>(1);
        colStatsMap.put("b1", longColStats);
        catalog.alterTableColumnStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T1"),
                new CatalogColumnStatistics(colStatsMap),
                false);

        longColStats = new CatalogColumnStatisticsDataLong(100L, 100L, 500000L, 1000L);
        colStatsMap = new HashMap<>(1);
        colStatsMap.put("b2", longColStats);
        catalog.alterTableColumnStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T2"),
                new CatalogColumnStatistics(colStatsMap),
                false);

        longColStats = new CatalogColumnStatisticsDataLong(100L, 100L, 50L, 1000L);
        colStatsMap = new HashMap<>(1);
        colStatsMap.put("b3", longColStats);
        catalog.alterTableColumnStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T3"),
                new CatalogColumnStatistics(colStatsMap),
                false);

        longColStats = new CatalogColumnStatisticsDataLong(100L, 100L, 500000L, 1000L);
        colStatsMap = new HashMap<>(1);
        colStatsMap.put("b4", longColStats);
        catalog.alterTableColumnStatistics(
                new ObjectPath(tEnv.getCurrentDatabase(), "T4"),
                new CatalogColumnStatistics(colStatsMap),
                false);

        checkResult(
                "SELECT tab2.d4, tab2.c3, tab1.d2, tab1.d1 FROM "
                        + "(SELECT * FROM T1 JOIN T2 ON T1.b1 = T2.b2) tab1 "
                        + "JOIN (SELECT * FROM T3 JOIN T4 ON T3.b3 = T4.b4) tab2 "
                        + "ON tab1.b2 = tab2.b4",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("Hallo", "Hi", "Hallo", "Hallo"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt", "Hallo Welt"),
                                Row.of("Hallo Welt", "Hello world", "Hallo Welt", "Hallo Welt"))),
                false);
    }

    @Test
    public void testBushyJoinReorderThreshold() {
        // Set bushy join reorder dp threshold to 2, so that the bushy join reorder strategy will
        // not be selected if the number of tables need to be reordered more than 2.
        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD, 2);
        checkResult(
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1 "
                        + "WHERE T1.a1 > 0 AND T3.a3 > 0",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("Hallo", "Hi", "Hallo", "Hallo"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt", "Hallo Welt"),
                                Row.of("Hallo Welt", "Hello world", "Hallo Welt", "Hallo Welt"))),
                false);
    }
}
