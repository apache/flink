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

package org.apache.flink.connectors.hive;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED;
import static org.apache.flink.table.planner.utils.TableTestUtil.readFromResource;
import static org.assertj.core.api.Assertions.assertThat;

/** Test hive query plan. */
public class HiveDialectQueryPlanTest {

    private static HiveCatalog hiveCatalog;
    private static TableEnvironment tableEnv;

    @BeforeClass
    public static void setup() throws Exception {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        // required by query like "src.`[k].*` from src"
        hiveCatalog.getHiveConf().setVar(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT, "none");
        hiveCatalog.open();
        tableEnv = getTableEnvWithHiveCatalog();

        // create tables
        tableEnv.executeSql("create table foo (x int, y int)");

        HiveTestUtils.createTextTableInserter(hiveCatalog, "default", "foo")
                .addRow(new Object[] {1, 1})
                .addRow(new Object[] {2, 2})
                .addRow(new Object[] {3, 3})
                .addRow(new Object[] {4, 4})
                .addRow(new Object[] {5, 5})
                .commit();
    }

    @Before
    public void before() {
        // enable native hive agg function
        tableEnv.getConfig().set(TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED, true);
    }

    @Test
    public void testSumAggFunctionPlan() {
        // test explain
        String sql = "select x, sum(y) from foo group by x";
        String actualPlan = explainSql(sql);
        assertThat(actualPlan).isEqualTo(readFromResource("/explain/testSumAggFunctionPlan.out"));

        // test fallback to hive sum udaf
        tableEnv.getConfig().set(TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED, false);
        String actualSortAggPlan = explainSql(sql);
        assertThat(actualSortAggPlan)
                .isEqualTo(readFromResource("/explain/testSumAggFunctionFallbackPlan.out"));
    }

    @Test
    public void testCountAggFunctionPlan() {
        // test explain
        String sql = "select x, count(*), count(y), count(distinct y) from foo group by x";
        String actualPlan = explainSql(sql);
        assertThat(actualPlan).isEqualTo(readFromResource("/explain/testCountAggFunctionPlan.out"));

        // test fallback to hive count udaf
        tableEnv.getConfig().set(TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED, false);
        String actualSortAggPlan = explainSql(sql);
        assertThat(actualSortAggPlan)
                .isEqualTo(readFromResource("/explain/testCountAggFunctionFallbackPlan.out"));
    }

    @Test
    public void testAvgAggFunctionPlan() {
        // test explain
        String sql = "select x, avg(y) from foo group by x";
        String actualPlan = explainSql(sql);
        assertThat(actualPlan).isEqualTo(readFromResource("/explain/testAvgAggFunctionPlan.out"));

        // test fallback to hive avg udaf
        tableEnv.getConfig().set(TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED, false);
        String actualSortAggPlan = explainSql(sql);
        assertThat(actualSortAggPlan)
                .isEqualTo(readFromResource("/explain/testAvgAggFunctionFallbackPlan.out"));
    }

    @Test
    public void testMinAggFunctionPlan() {
        // test explain
        String sql = "select x, min(y) from foo group by x";
        String actualPlan = explainSql(sql);
        assertThat(actualPlan).isEqualTo(readFromResource("/explain/testMinAggFunctionPlan.out"));

        // test fallback to hive min udaf
        tableEnv.getConfig().set(TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED, false);
        String actualSortAggPlan = explainSql(sql);
        assertThat(actualSortAggPlan)
                .isEqualTo(readFromResource("/explain/testMinAggFunctionFallbackPlan.out"));
    }

    @Test
    public void testMaxAggFunctionPlan() {
        // test explain
        String sql = "select x, max(y) from foo group by x";
        String actualPlan = explainSql(sql);
        assertThat(actualPlan).isEqualTo(readFromResource("/explain/testMaxAggFunctionPlan.out"));

        // test fallback to hive max udaf
        tableEnv.getConfig().set(TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED, false);
        String actualSortAggPlan = explainSql(sql);
        assertThat(actualSortAggPlan)
                .isEqualTo(readFromResource("/explain/testMaxAggFunctionFallbackPlan.out"));
    }

    private String explainSql(String sql) {
        return (String)
                CollectionUtil.iteratorToList(tableEnv.executeSql("explain " + sql).collect())
                        .get(0)
                        .getField(0);
    }

    private static TableEnvironment getTableEnvWithHiveCatalog() {
        TableEnvironment tableEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        // automatically load hive module in hive-compatible mode
        HiveModule hiveModule =
                new HiveModule(
                        hiveCatalog.getHiveVersion(),
                        tableEnv.getConfig(),
                        Thread.currentThread().getContextClassLoader());
        CoreModule coreModule = CoreModule.INSTANCE;
        for (String loaded : tableEnv.listModules()) {
            tableEnv.unloadModule(loaded);
        }
        tableEnv.loadModule("hive", hiveModule);
        tableEnv.loadModule("core", coreModule);
        return tableEnv;
    }
}
