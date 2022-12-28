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
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/** Plan test for {@link FlinkBusyJoinReorderRule}. */
public class FlinkBusyJoinReorderRuleTest extends TableTestBase {
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
        tableConfig.set(FlinkJoinReorderRule.TABLE_OPTIMIZER_BUSY_JOIN_REORDER_THRESHOLD, 5);

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
    public void testFullOuterJoinReorder() {
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4, T3, T2, T1 WHERE T4.a4 > 1 AND T3.a3 > 1";
        util.verifyRelPlan(query);
    }

    @Test
    public void testInnerJoinReorder() {
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1 "
                        + "WHERE T1.a1 > 0 AND T3.a3 > 0";
        util.verifyRelPlan(query);
    }

    @Test
    public void testLeftOuterJoinReorder() {
        // can reorder, all join keys will not generate null.
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "LEFT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "LEFT OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "LEFT OUTER JOIN T1 ON T4.b4 = T1.b1 WHERE T4.a4 < 3";
        util.verifyRelPlan(query);
    }

    @Test
    public void testInnerAndLeftOuterJoinReorder() {
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "JOIN T3 ON T4.b4 = T3.b3 "
                        + "LEFT OUTER JOIN T2 ON T4.b4 = T2.b2 "
                        + "JOIN T1 ON T4.b4 = T1.b1 "
                        + "WHERE T4.a4 < 3";
        util.verifyRelPlan(query);
    }

    @Test
    public void testRightOuterJoinReorder() {
        String query =
                "SELECT T4.d4, T3.c3, T2.d2, T1.d1 FROM T4 "
                        + "RIGHT OUTER JOIN T3 ON T4.b4 = T3.b3 "
                        + "JOIN T2 ON T3.b3 = T2.b2 "
                        + "JOIN T1 ON T2.b2 = T1.b1 WHERE T2.a2 <= 2";
        util.verifyRelPlan(query);
    }
}
