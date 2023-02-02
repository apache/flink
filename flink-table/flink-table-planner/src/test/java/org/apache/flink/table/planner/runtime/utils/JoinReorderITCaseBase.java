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

package org.apache.flink.table.planner.runtime.utils;

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
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The base case of join reorder ITCase. This class will test {@link
 * org.apache.flink.table.planner.plan.rules.logical.FlinkBushyJoinReorderRule} and {@link
 * org.apache.calcite.rel.rules.LoptOptimizeJoinRule} together by changing the factor
 * isBushyJoinReorder.
 */
public abstract class JoinReorderITCaseBase extends TestLogger {

    private static final int DEFAULT_PARALLELISM = 4;

    protected TableEnvironment tEnv;
    private Catalog catalog;

    protected abstract TableEnvironment getTableEnvironment();

    protected abstract void assertEquals(String query, List<String> expectedList);

    @BeforeEach
    public void before() throws Exception {
        tEnv = getTableEnvironment();
        catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();

        tEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
        tEnv.getConfig()
                .getConfiguration()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                        DEFAULT_PARALLELISM);

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

    @AfterEach
    public void after() {
        TestValuesTableFactory.clearAllData();
    }

    @ParameterizedTest(name = "Is bushy join reorder: {0}")
    @ValueSource(booleans = {true, false})
    public void testJoinReorderWithFullOuterJoin(boolean isBushyJoinReorder) {
        setIsBushyJoinReorder(isBushyJoinReorder);
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "FULL OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "FULL OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "FULL OUTER JOIN T1 ON T2.b2 = T1.b1";

        List<String> expectedList =
                Arrays.asList(
                        "ABC,null,ABC,ABC",
                        "BCD,null,BCD,BCD",
                        "CDE,null,CDE,CDE",
                        "DEF,null,DEF,DEF",
                        "EFG,null,EFG,EFG",
                        "FGH,null,FGH,FGH",
                        "GHI,null,GHI,GHI",
                        "HIJ,null,HIJ,HIJ",
                        "Hallo Welt wie gehts?,null,Hallo Welt wie gehts?,Hallo Welt wie gehts?",
                        "Hallo Welt wie,null,Hallo Welt wie,Hallo Welt wie",
                        "Hallo Welt,Hello world,Hallo Welt,Hallo Welt",
                        "Hallo Welt,Hello,Hallo Welt,Hallo Welt",
                        "Hallo,Hi,Hallo,Hallo",
                        "IJK,null,IJK,IJK",
                        "JKL,null,JKL,JKL",
                        "KLM,null,KLM,KLM");
        assertEquals(query, expectedList);
    }

    @ParameterizedTest(name = "Is bushy join reorder: {0}")
    @ValueSource(booleans = {true, false})
    public void testJoinReorderWithInnerAndFullOuterJoin(boolean isBushyJoinReorder) {
        setIsBushyJoinReorder(isBushyJoinReorder);
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "FULL OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1";

        List<String> expectedList =
                Arrays.asList(
                        "Hallo Welt,Hello world,Hallo Welt,Hallo Welt",
                        "Hallo Welt,Hello,Hallo Welt,Hallo Welt",
                        "Hallo,Hi,Hallo,Hallo");
        assertEquals(query, expectedList);
    }

    @ParameterizedTest(name = "Is bushy join reorder: {0}")
    @ValueSource(booleans = {true, false})
    public void testJoinReorderWithInnerJoin(boolean isBushyJoinReorder) {
        setIsBushyJoinReorder(isBushyJoinReorder);
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1 WHERE T1.a1 > 0 AND T3.a3 > 0";

        List<String> expectedList =
                Arrays.asList(
                        "Hallo Welt,Hello world,Hallo Welt,Hallo Welt",
                        "Hallo Welt,Hello,Hallo Welt,Hallo Welt",
                        "Hallo,Hi,Hallo,Hallo");
        assertEquals(query, expectedList);
    }

    @ParameterizedTest(name = "Is bushy join reorder: {0}")
    @ValueSource(booleans = {true, false})
    public void testJoinReorderWithLeftOuterJoin(boolean isBushyJoinReorder) {
        setIsBushyJoinReorder(isBushyJoinReorder);
        // can reorder, all join keys will not generate null.
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "LEFT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "LEFT OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "LEFT OUTER JOIN T1 ON T4.b4 = T1.b1 WHERE T4.a4 < 3";

        List<String> expectedList =
                Arrays.asList(
                        "Hallo,Hi,Hallo,Hallo",
                        "Hallo Welt,Hello,Hallo Welt,Hallo Welt",
                        "Hallo Welt,Hello world,Hallo Welt,Hallo Welt",
                        "Hallo Welt wie,null,Hallo Welt wie,Hallo Welt wie");
        assertEquals(query, expectedList);
    }

    @ParameterizedTest(name = "Is bushy join reorder: {0}")
    @ValueSource(booleans = {true, false})
    public void testJoinReorderWithInnerAndLeftOuterJoin(boolean isBushyJoinReorder) {
        setIsBushyJoinReorder(isBushyJoinReorder);
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "LEFT OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1 "
                        + "WHERE T4.a4 < 3";

        List<String> expectedList =
                Arrays.asList(
                        "Hallo,Hi,Hallo,Hallo",
                        "Hallo Welt,Hello,Hallo Welt,Hallo Welt",
                        "Hallo Welt,Hello world,Hallo Welt,Hallo Welt");
        assertEquals(query, expectedList);
    }

    @ParameterizedTest(name = "Is bushy join reorder: {0}")
    @ValueSource(booleans = {true, false})
    public void testJoinReorderWithRightOuterJoin(boolean isBushyJoinReorder) {
        setIsBushyJoinReorder(isBushyJoinReorder);
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "RIGHT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T3.b3 = T2.b2 "
                        + "JOIN T1 ON T2.b2 = T1.b1 WHERE T2.a2 <= 2";

        List<String> expectedList =
                Arrays.asList(
                        "Hallo,Hi,Hallo,Hallo",
                        "Hallo Welt,Hello,Hallo Welt,Hallo Welt",
                        "Hallo Welt,Hello world,Hallo Welt,Hallo Welt");
        assertEquals(query, expectedList);
    }

    @ParameterizedTest(name = "Is bushy join reorder: {0}")
    @ValueSource(booleans = {true, false})
    public void testJoinReorderWithTrueCondition(boolean isBushyJoinReorder) {
        setIsBushyJoinReorder(isBushyJoinReorder);
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4, T3, T2, T1 "
                        + "WHERE T4.a4 <= 1 AND T3.a3 <= 1 AND T2.a2 <= 1 AND T1.a1 <= 1";
        assertEquals(query, Collections.singletonList("Hallo,Hi,Hallo,Hallo"));
    }

    @ParameterizedTest(name = "Is bushy join reorder: {0}")
    @ValueSource(booleans = {true, false})
    public void testJoinReorderWithInnerJoinAndTrueCondition(boolean isBushyJoinReorder) {
        setIsBushyJoinReorder(isBushyJoinReorder);
        String query =
                "SELECT tab1.d4, tab1.c3, T2.d2, T1.d1 FROM T1, "
                        + "(SELECT * FROM T3 JOIN T4 ON T4.b4 = T3.b3) tab1, T2 "
                        + "WHERE tab1.a4 <= 1 AND tab1.a3 <= 1 AND T2.a2 <= 1 AND T1.a1 <= 1";
        assertEquals(query, Collections.singletonList("Hallo,Hi,Hallo,Hallo"));
    }

    @ParameterizedTest(name = "Is bushy join reorder: {0}")
    @ValueSource(booleans = {true, false})
    public void testJoinReorderWithMixedJoinTypeAndCondition(boolean isBushyJoinReorder) {
        setIsBushyJoinReorder(isBushyJoinReorder);
        String query =
                "SELECT tab2.d4, tab2.c3, tab2.d2, T1.d1 FROM T1, (SELECT * FROM T4 "
                        + "LEFT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T4.b4 = T2.b2) tab2 "
                        + "WHERE tab2.a4 <= 1 AND tab2.a3 <= 1 AND tab2.a2 <= 1 AND T1.a1 <= 1";
        assertEquals(query, Collections.singletonList("Hallo,Hi,Hallo,Hallo"));
    }

    @ParameterizedTest(name = "Is bushy join reorder: {0}")
    @ValueSource(booleans = {true, false})
    public void testBushyTreeJoinReorder(boolean isBushyJoinReorder)
            throws TableNotExistException, TablePartitionedException {
        setIsBushyJoinReorder(isBushyJoinReorder);
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

        String query =
                "SELECT tab2.d4, tab2.c3, tab1.d2, tab1.d1 FROM "
                        + "(SELECT * FROM T1 JOIN T2 ON T1.b1 = T2.b2) tab1 "
                        + "JOIN (SELECT * FROM T3 JOIN T4 ON T3.b3 = T4.b4) tab2 "
                        + "ON tab1.b2 = tab2.b4";

        List<String> expectedList =
                Arrays.asList(
                        "Hallo,Hi,Hallo,Hallo",
                        "Hallo Welt,Hello,Hallo Welt,Hallo Welt",
                        "Hallo Welt,Hello world,Hallo Welt,Hallo Welt");
        assertEquals(query, expectedList);
    }

    private void setIsBushyJoinReorder(boolean isBushyJoinReorder) {
        if (!isBushyJoinReorder) {
            tEnv.getConfig()
                    .getConfiguration()
                    .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD, 3);
        } else {
            tEnv.getConfig()
                    .getConfiguration()
                    .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD, 1000);
        }
    }
}
