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
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.rules.logical.FlinkJoinReorderRule;
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

import scala.Enumeration;

/** ITCase for JoinReorder in batch mode. */
@RunWith(Parameterized.class)
public class JoinReorderITCase extends BatchTestBase {

    private TableEnvironment tEnv;
    private Catalog catalog;

    @Parameterized.Parameter(value = 0)
    public Enumeration.Value expectedJoinType;

    @Parameterized.Parameter(value = 1)
    public boolean isBusyJoinReorder;

    @Parameterized.Parameters(name = "expectedJoinType={0}, isBusyJoinReorder={1}")
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

        if (isBusyJoinReorder) {
            tEnv.getConfig()
                    .getConfiguration()
                    .set(FlinkJoinReorderRule.TABLE_OPTIMIZER_BUSY_JOIN_REORDER_THRESHOLD, 5);
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
    public void testFullOuterJoinReorder() {
        // Only supports NestedLoopJoin.
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
    public void testInnerJoinReorder() {
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
    public void testLeftOuterJoinReorder() {
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
    public void testInnerAndLeftOuterJoinReorder() {
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
    public void testRightOuterJoinReorder() {
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
    public void testBusyJoinReorderDpThreshold() {
        // Set busy join reorder dp threshold to 2, so that the busy join reorder strategy will not
        // be selected if the number of tables need to be reordered more than 2.
        tEnv.getConfig()
                .getConfiguration()
                .set(FlinkJoinReorderRule.TABLE_OPTIMIZER_BUSY_JOIN_REORDER_THRESHOLD, 2);
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
